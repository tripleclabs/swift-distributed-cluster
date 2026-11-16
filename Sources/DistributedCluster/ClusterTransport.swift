import Foundation
import Logging
import NIOCore
import NIOPosix
import Synchronization

internal enum ControlFrame: Sendable {
    case watchActor(watcherID: ClusterActorID, subjectID: ClusterActorID)
    case unwatchActor(watcherID: ClusterActorID, subjectID: ClusterActorID)
    case actorTerminated(subjectID: ClusterActorID)
    case receptionistSync(keyID: String, actorID: ClusterActorID, removed: Bool)
    case heartbeat
}

internal struct WatchEnvelope: Codable, Sendable {
    var watcherID: ClusterActorID
    var subjectID: ClusterActorID
}

internal struct TerminatedEnvelope: Codable, Sendable {
    var subjectID: ClusterActorID
}

internal struct ReceptionistSyncEnvelope: Codable, Sendable {
    var keyID: String
    var actorID: ClusterActorID
    var removed: Bool
}

internal final class ClusterTransport: Sendable {
    typealias Writer = NIOAsyncChannelOutboundWriter<WireFrame>
    typealias PeerChannel = NIOAsyncChannel<WireFrame, WireFrame>
    typealias ServerChannel = NIOAsyncChannel<PeerChannel, Never>

    struct PendingCall {
        let node: Cluster.Node
        let target: ClusterActorID
        let continuation: CheckedContinuation<Data, any Error>
    }

    struct State {
        var writers: [Cluster.Node: Writer] = [:]
        var channels: [Cluster.Node: PeerChannel] = [:]
        var pending: [UInt64: PendingCall] = [:]
        var connectWaiters: [Cluster.Node: [CheckedContinuation<Writer, any Error>]] = [:]
        var serverChannel: ServerChannel?
        var onFrame: (@Sendable (WireFrame, Cluster.Node) async -> Void)?
        var onNodeDown: (@Sendable (Cluster.Node) async -> Void)?
        var isShutdown: Bool = false
    }

    let localNode: Cluster.Node
    let settings: ClusterSystem.Settings
    let logger: Logger
    let group: MultiThreadedEventLoopGroup
    let state = Mutex(State())
    let correlationCounter = Atomic<UInt64>(1)

    init(localNode: Cluster.Node, settings: ClusterSystem.Settings) {
        self.localNode = localNode
        self.settings = settings
        self.logger = settings.logger
        self.group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
    }

    func nextCorrelationID() -> UInt64 {
        correlationCounter.wrappingAdd(1, ordering: .sequentiallyConsistent).newValue
    }

    func start(
        onFrame: @escaping @Sendable (WireFrame, Cluster.Node) async -> Void,
        onNodeDown: @escaping @Sendable (Cluster.Node) async -> Void
    ) async throws(ClusterError) {
        state.withLock { s in
            s.onFrame = onFrame
            s.onNodeDown = onNodeDown
        }
        let serverChannel: ServerChannel
        do {
            serverChannel = try await ServerBootstrap(group: group)
                .serverChannelOption(.socketOption(.so_reuseaddr), value: 1)
                .bind(host: localNode.endpoint.host, port: localNode.endpoint.port) { channel in
                    channel.eventLoop.makeCompletedFuture {
                        try channel.pipeline.syncOperations.addHandler(ByteToMessageHandler(WireFrameDecoder()))
                        try channel.pipeline.syncOperations.addHandler(MessageToByteHandler(WireFrameEncoder()))
                        return try NIOAsyncChannel<WireFrame, WireFrame>(wrappingChannelSynchronously: channel)
                    }
                }
        } catch {
            throw .bindFailed(host: localNode.endpoint.host, port: localNode.endpoint.port, reason: "\(error)")
        }
        state.withLock { s in
            s.serverChannel = serverChannel
        }
        Swift::Task {
            await self.runAcceptLoop(serverChannel: serverChannel)
        }
    }

    private func runAcceptLoop(serverChannel: ServerChannel) async {
        do {
            try await withThrowingDiscardingTaskGroup { group in
                try await serverChannel.executeThenClose { inbound in
                    for try await childChannel in inbound {
                        group.addTask {
                            await self.supervisePeerChannel(childChannel, initialPeer: nil)
                        }
                    }
                }
            }
        } catch {
            logger.debug("Accept loop closed: \(error)")
        }
    }

    func supervisePeerChannel(_ channel: PeerChannel, initialPeer: Cluster.Node?) async {
        let peerBox = Mutex<Cluster.Node?>(initialPeer)
        defer {
            await withTaskCancellationShield {
                try? await channel.channel.close()
                if let peer = peerBox.withLock({ $0 }) {
                    await self.handlePeerDown(peer)
                }
            }
        }
        do {
            try await channel.executeThenClose { inbound, outbound in
                if let peer = initialPeer {
                    self.registerWriter(outbound, channel: channel, for: peer)
                }
                try await withThrowingDiscardingTaskGroup { group in
                    group.addTask {
                        while !Swift::Task.isCancelled {
                            do {
                                try await Swift::Task.sleep(for: self.settings.heartbeatInterval)
                                let ping = WireFrame(
                                    type: .heartbeat,
                                    correlationID: 0,
                                    sender: self.localNode,
                                    payload: Data("ping".utf8)
                                )
                                try await outbound.write(ping)
                            } catch {
                                break
                            }
                        }
                    }
                    for try await frame in inbound {
                        if frame.type == .heartbeat {
                            if frame.payload == Data("ping".utf8) {
                                let pong = WireFrame(
                                    type: .heartbeat,
                                    correlationID: 0,
                                    sender: self.localNode,
                                    payload: Data("pong".utf8)
                                )
                                try? await outbound.write(pong)
                            }
                            continue
                        }
                        let isNewPeer = peerBox.withLock { current -> Bool in
                            if current == nil {
                                current = frame.sender
                                return true
                            }
                            return false
                        }
                        if isNewPeer {
                            self.registerWriter(outbound, channel: channel, for: frame.sender)
                        }
                        group.addTask {
                            await self.handleInboundFrame(frame)
                        }
                    }
                }
            }
        } catch {
            logger.debug("Peer channel closed: \(error)")
        }
    }

    private func registerWriter(_ writer: Writer, channel: PeerChannel, for peer: Cluster.Node) {
        let waiters = state.withLock { s -> [CheckedContinuation<Writer, any Error>] in
            s.writers[peer] = writer
            s.channels[peer] = channel
            return s.connectWaiters.removeValue(forKey: peer) ?? []
        }
        for waiter in waiters {
            waiter.resume(returning: writer)
        }
    }

    private func handleInboundFrame(_ frame: WireFrame) async {
        switch frame.type {
        case .reply:
            resumePending(correlationID: frame.correlationID, result: .success(frame.payload))
        case .error:
            let failure: ClusterError
            if let envelope = try? decodeCodableValue(ErrorEnvelope.self, from: frame.payload) {
                failure = envelope.clusterError ?? .decodingFailed("Remote error: \(envelope.message)")
            } else {
                failure = .decodingFailed("Remote error frame could not be decoded")
            }
            resumePending(correlationID: frame.correlationID, result: .failure(failure))
        case .heartbeat:
            break
        case .remoteCall, .remoteCallVoid, .watchActor, .unwatchActor, .actorTerminated, .receptionistSync:
            let callback = state.withLock { $0.onFrame }
            await callback?(frame, frame.sender)
        }
    }

    func handlePeerDown(_ peer: Cluster.Node) async {
        let (pendingCalls, waiters, callback) = state.withLock { s -> ([PendingCall], [CheckedContinuation<Writer, any Error>], (@Sendable (Cluster.Node) async -> Void)?) in
            s.writers.removeValue(forKey: peer)
            s.channels.removeValue(forKey: peer)
            var removed: [PendingCall] = []
            s.pending = s.pending.filter { entry in
                if entry.value.node == peer {
                    removed.append(entry.value)
                    return false
                }
                return true
            }
            let pendingWaiters = s.connectWaiters.removeValue(forKey: peer) ?? []
            return (removed, pendingWaiters, s.onNodeDown)
        }
        for call in pendingCalls {
            call.continuation.resume(throwing: ClusterError.nodeUnreachable(peer))
        }
        for waiter in waiters {
            waiter.resume(throwing: ClusterError.nodeUnreachable(peer))
        }
        await callback?(peer)
    }

    func writer(for node: Cluster.Node) async throws(ClusterError) -> Writer {
        let needsConnect: Bool = state.withLock { s in
            if s.writers[node] != nil {
                return false
            }
            return true
        }
        if let existing = state.withLock({ $0.writers[node] }) {
            return existing
        }
        _ = needsConnect
        do {
            return try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Writer, any Error>) in
                var shouldConnect = false
                state.withLock { s in
                    if s.isShutdown {
                        continuation.resume(throwing: ClusterError.systemShuttingDown)
                        return
                    }
                    if let writer = s.writers[node] {
                        continuation.resume(returning: writer)
                        return
                    }
                    if s.connectWaiters[node] != nil {
                        s.connectWaiters[node, default: []].append(continuation)
                        return
                    }
                    s.connectWaiters[node] = [continuation]
                    shouldConnect = true
                }
                if shouldConnect {
                    Swift::Task {
                        await self.connect(to: node)
                    }
                }
            }
        } catch let error as ClusterError {
            throw error
        } catch {
            throw .nodeUnreachable(node)
        }
    }

    private func connect(to node: Cluster.Node) async {
        do {
            let channel = try await ClientBootstrap(group: group)
                .connect(host: node.endpoint.host, port: node.endpoint.port) { channel in
                    channel.eventLoop.makeCompletedFuture {
                        try channel.pipeline.syncOperations.addHandler(ByteToMessageHandler(WireFrameDecoder()))
                        try channel.pipeline.syncOperations.addHandler(MessageToByteHandler(WireFrameEncoder()))
                        return try NIOAsyncChannel<WireFrame, WireFrame>(wrappingChannelSynchronously: channel)
                    }
                }
            Swift::Task {
                await self.supervisePeerChannel(channel, initialPeer: node)
            }
        } catch {
            let waiters = state.withLock { s in
                s.connectWaiters.removeValue(forKey: node) ?? []
            }
            for waiter in waiters {
                waiter.resume(throwing: ClusterError.nodeUnreachable(node))
            }
        }
    }

    func sendFrame(_ frame: WireFrame, to node: Cluster.Node) async throws(ClusterError) {
        let writer = try await writer(for: node)
        do {
            try await writer.write(frame)
        } catch let error as ClusterError {
            throw error
        } catch {
            throw .nodeUnreachable(node)
        }
    }

    func sendControlFrame(_ control: ControlFrame, to node: Cluster.Node) async {
        guard let frame = makeControlFrame(control) else { return }
        do {
            try await sendFrame(frame, to: node)
        } catch {
            logger.debug("Control frame to \(node) failed: \(error)")
        }
    }

    func sendControlFrameOrThrow(_ control: ControlFrame, to node: Cluster.Node) async throws(ClusterError) {
        guard let frame = makeControlFrame(control) else { return }
        try await sendFrame(frame, to: node)
    }

    private func makeControlFrame(_ control: ControlFrame) -> WireFrame? {
        switch control {
        case .watchActor(let watcherID, let subjectID):
            guard let payload = try? encodeCodableValue(WatchEnvelope(watcherID: watcherID, subjectID: subjectID)) else { return nil }
            return WireFrame(type: .watchActor, correlationID: 0, sender: localNode, payload: payload)
        case .unwatchActor(let watcherID, let subjectID):
            guard let payload = try? encodeCodableValue(WatchEnvelope(watcherID: watcherID, subjectID: subjectID)) else { return nil }
            return WireFrame(type: .unwatchActor, correlationID: 0, sender: localNode, payload: payload)
        case .actorTerminated(let subjectID):
            guard let payload = try? encodeCodableValue(TerminatedEnvelope(subjectID: subjectID)) else { return nil }
            return WireFrame(type: .actorTerminated, correlationID: 0, sender: localNode, payload: payload)
        case .receptionistSync(let keyID, let actorID, let removed):
            guard let payload = try? encodeCodableValue(ReceptionistSyncEnvelope(keyID: keyID, actorID: actorID, removed: removed)) else { return nil }
            return WireFrame(type: .receptionistSync, correlationID: 0, sender: localNode, payload: payload)
        case .heartbeat:
            return WireFrame(type: .heartbeat, correlationID: 0, sender: localNode, payload: Data("ping".utf8))
        }
    }

    func performRemoteCall(
        frame: WireFrame,
        to node: Cluster.Node,
        target: ClusterActorID,
        timeout: Duration
    ) async throws(ClusterError) -> Data {
        let correlationID = frame.correlationID
        do {
            return try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Data, any Error>) in
                state.withLock { s in
                    if s.isShutdown {
                        continuation.resume(throwing: ClusterError.systemShuttingDown)
                        return
                    }
                    s.pending[correlationID] = PendingCall(node: node, target: target, continuation: continuation)
                }
                Swift::Task {
                    do {
                        try await self.sendFrame(frame, to: node)
                    } catch let error as ClusterError {
                        self.failPending(correlationID: correlationID, error: error)
                    } catch {
                        self.failPending(correlationID: correlationID, error: .nodeUnreachable(node))
                    }
                }
                Swift::Task {
                    try? await Swift::Task.sleep(for: timeout)
                    self.failPending(
                        correlationID: correlationID,
                        error: .invocationTimeout(correlationID: correlationID, target: target)
                    )
                }
            }
        } catch let error as ClusterError {
            throw error
        } catch {
            throw .nodeUnreachable(node)
        }
    }

    func failPending(correlationID: UInt64, error: ClusterError) {
        let call = state.withLock { s in
            s.pending.removeValue(forKey: correlationID)
        }
        call?.continuation.resume(throwing: error)
    }

    func resumePending(correlationID: UInt64, result: Result<Data, ClusterError>) {
        let call = state.withLock { s in
            s.pending.removeValue(forKey: correlationID)
        }
        guard let call else { return }
        switch result {
        case .success(let data):
            call.continuation.resume(returning: data)
        case .failure(let error):
            call.continuation.resume(throwing: error)
        }
    }

    func knownPeers() -> [Cluster.Node] {
        state.withLock { s in
            Array(s.writers.keys)
        }
    }

    func shutdown() async {
        let (serverChannel, channels, pendingCalls, waiters) = state.withLock { s -> (ServerChannel?, [PeerChannel], [PendingCall], [CheckedContinuation<Writer, any Error>]) in
            if s.isShutdown {
                return (nil, [], [], [])
            }
            s.isShutdown = true
            let pending = Array(s.pending.values)
            s.pending.removeAll()
            var allWaiters: [CheckedContinuation<Writer, any Error>] = []
            for (_, list) in s.connectWaiters {
                allWaiters.append(contentsOf: list)
            }
            s.connectWaiters.removeAll()
            let chans = Array(s.channels.values)
            s.channels.removeAll()
            s.writers.removeAll()
            let server = s.serverChannel
            s.serverChannel = nil
            s.onFrame = nil
            s.onNodeDown = nil
            return (server, chans, pending, allWaiters)
        }
        for call in pendingCalls {
            call.continuation.resume(throwing: ClusterError.systemShuttingDown)
        }
        for waiter in waiters {
            waiter.resume(throwing: ClusterError.systemShuttingDown)
        }
        await withTaskCancellationShield {
            if let serverChannel {
                try? await serverChannel.channel.close()
            }
            for channel in channels {
                try? await channel.channel.close()
            }
            try? await group.shutdownGracefully()
        }
    }
}
