import Foundation
import Logging
import NIOCore
import NIOPosix
#if MutualTLS
import NIOSSL
#endif
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

    struct PeerConnection {
        let id: UUID
        let writer: Writer
        let channel: PeerChannel
    }

    struct State {
        var peers: [Cluster.Node: PeerConnection] = [:]
        var extraChannels: [UUID: PeerChannel] = [:]
        var pending: [UInt64: PendingCall] = [:]
        var connectWaiters: [Cluster.Node: [CheckedContinuation<Writer, any Error>]] = [:]
        var serverChannel: ServerChannel?
        var acceptTask: Swift::Task<Void, Never>?
        var peerTasks: [UUID: Swift::Task<Void, Never>] = [:]
        var onFrame: (@Sendable (WireFrame, Cluster.Node) async -> Void)?
        var onPeerConnected: (@Sendable (Cluster.Node) async -> Void)?
        var onNodeDown: (@Sendable (Cluster.Node) async -> Void)?
        var isShutdown: Bool = false
    }

    let localNode: Cluster.Node
    let settings: ClusterSystem.Settings
    let logger: Logger
    let group: MultiThreadedEventLoopGroup
    let state = Mutex(State())
    let correlationCounter = Atomic<UInt64>(1)
    #if MutualTLS
    private let sslContexts = Mutex<(server: NIOSSLContext?, client: NIOSSLContext?)>((nil, nil))
    #endif

    init(localNode: Cluster.Node, settings: ClusterSystem.Settings) {
        self.localNode = localNode
        self.settings = settings
        self.logger = settings.logger
        self.group = MultiThreadedEventLoopGroup(numberOfThreads: max(1, min(System.coreCount, 4)))
    }

    private func wrapPeerChannel(
        _ channel: any Channel,
        isServer: Bool,
        remoteHost: String?
    ) -> EventLoopFuture<PeerChannel> {
        channel.eventLoop.makeCompletedFuture {
            #if MutualTLS
            let contexts = self.sslContexts.withLock { $0 }
            if isServer, let serverContext = contexts.server {
                try channel.pipeline.syncOperations.addHandler(NIOSSLServerHandler(context: serverContext))
            } else if !isServer, let clientContext = contexts.client {
                let sniHost = self.settings.tls?.serverHostname ?? Self.nonIPLiteralHost(remoteHost)
                try channel.pipeline.syncOperations.addHandler(
                    NIOSSLClientHandler(context: clientContext, serverHostname: sniHost)
                )
            }
            #endif
            try channel.pipeline.syncOperations.addHandler(ByteToMessageHandler(WireFrameDecoder()))
            try channel.pipeline.syncOperations.addHandler(MessageToByteHandler(WireFrameEncoder()))
            return try NIOAsyncChannel<WireFrame, WireFrame>(wrappingChannelSynchronously: channel)
        }
    }

    #if MutualTLS
    private static func nonIPLiteralHost(_ host: String?) -> String? {
        guard let host, !host.isEmpty else { return nil }
        var ipv4 = in_addr()
        var ipv6 = in6_addr()
        if inet_pton(AF_INET, host, &ipv4) == 1 || inet_pton(AF_INET6, host, &ipv6) == 1 {
            return nil
        }
        return host
    }
    #endif

    func nextCorrelationID() -> UInt64 {
        correlationCounter.wrappingAdd(1, ordering: .sequentiallyConsistent).newValue
    }

    func start(
        onFrame: @escaping @Sendable (WireFrame, Cluster.Node) async -> Void,
        onPeerConnected: @escaping @Sendable (Cluster.Node) async -> Void,
        onNodeDown: @escaping @Sendable (Cluster.Node) async -> Void
    ) async throws(ClusterError) {
        #if MutualTLS
        if let tls = settings.tls {
            do {
                let serverCtx = try NIOSSLContext(configuration: tls.server)
                let clientCtx = try NIOSSLContext(configuration: tls.client)
                sslContexts.withLock { $0 = (serverCtx, clientCtx) }
            } catch {
                throw .bindFailed(
                    host: localNode.endpoint.host,
                    port: localNode.endpoint.port,
                    reason: "TLS initialization failed: \(error)"
                )
            }
        }
        #endif
        state.withLock { s in
            s.onFrame = onFrame
            s.onPeerConnected = onPeerConnected
            s.onNodeDown = onNodeDown
        }
        let serverChannel: ServerChannel
        do {
            serverChannel = try await ServerBootstrap(group: group)
                .serverChannelOption(.socketOption(.so_reuseaddr), value: 1)
                .bind(host: localNode.endpoint.host, port: localNode.endpoint.port) { channel in
                    self.wrapPeerChannel(channel, isServer: true, remoteHost: nil)
                }
        } catch {
            throw .bindFailed(host: localNode.endpoint.host, port: localNode.endpoint.port, reason: "\(error)")
        }

        let acceptTask = Swift::Task {
            await self.runAcceptLoop(serverChannel: serverChannel)
        }
        let shouldClose = state.withLock { s -> Bool in
            if s.isShutdown {
                return true
            }
            s.serverChannel = serverChannel
            s.acceptTask = acceptTask
            return false
        }
        if shouldClose {
            acceptTask.cancel()
            try? await serverChannel.channel.close()
            await acceptTask.value
        }
    }

    func connectToSeedNodes(_ endpoints: [Cluster.Endpoint]) {
        for endpoint in endpoints {
            if endpoint.host == localNode.endpoint.host && endpoint.port == localNode.endpoint.port {
                continue
            }
            let taskID = UUID()
            let task = Swift::Task {
                defer {
                    _ = self.state.withLock { $0.peerTasks.removeValue(forKey: taskID) }
                }
                do {
                    let channel = try await ClientBootstrap(group: self.group)
                        .connect(host: endpoint.host, port: endpoint.port) { channel in
                            self.wrapPeerChannel(channel, isServer: false, remoteHost: endpoint.host)
                        }
                    await self.supervisePeerChannel(
                        channel,
                        initialPeer: nil,
                        channelID: taskID,
                        sendInitialPing: true
                    )
                } catch {
                    self.logger.debug("Seed node connect to \(endpoint.host):\(endpoint.port) failed: \(error)")
                }
            }
            let inserted = state.withLock { s -> Bool in
                if s.isShutdown { return false }
                s.peerTasks[taskID] = task
                return true
            }
            if !inserted {
                task.cancel()
            }
        }
    }

    private func runAcceptLoop(serverChannel: ServerChannel) async {
        do {
            try await withThrowingDiscardingTaskGroup { group in
                try await serverChannel.executeThenClose { inbound in
                    for try await childChannel in inbound {
                        let channelID = UUID()
                        group.addTask {
                            await self.supervisePeerChannel(
                                childChannel,
                                initialPeer: nil,
                                channelID: channelID,
                                sendInitialPing: false
                            )
                        }
                    }
                }
            }
        } catch {
            logger.debug("Accept loop closed: \(error)")
        }
    }

    func supervisePeerChannel(
        _ channel: PeerChannel,
        initialPeer: Cluster.Node?,
        channelID: UUID,
        sendInitialPing: Bool = false
    ) async {
        let peerBox = Mutex<Cluster.Node?>(initialPeer)
        let lastActivity = Mutex<ContinuousClock.Instant>(.now)
        defer {
            await withTaskCancellationShield {
                try? await channel.channel.close()
                if let peer = peerBox.withLock({ $0 }) {
                    await self.handlePeerDown(peer, channelID: channelID)
                }
            }
        }
        do {
            try await channel.executeThenClose { inbound, outbound in
                if let peer = initialPeer {
                    guard await self.registerWriter(outbound, channel: channel, channelID: channelID, for: peer) else {
                        return
                    }
                } else if sendInitialPing {
                    let ping = WireFrame(
                        type: .heartbeat,
                        correlationID: 0,
                        sender: self.localNode,
                        payload: Data("ping".utf8)
                    )
                    try? await outbound.write(ping)
                }
                try await withThrowingDiscardingTaskGroup { group in
                    group.addTask {
                        while !Swift::Task.isCancelled {
                            do {
                                try await Swift::Task.sleep(for: self.settings.heartbeatInterval)
                                let elapsed = lastActivity.withLock { ContinuousClock.now - $0 }
                                if elapsed > self.settings.heartbeatTimeout {
                                    self.logger.debug("Peer heartbeat timeout (\(elapsed)); closing channel")
                                    try? await channel.channel.close()
                                    break
                                }
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
                        lastActivity.withLock { $0 = .now }
                        let isNewPeer = peerBox.withLock { current -> Bool in
                            if current == nil {
                                current = frame.sender
                                return true
                            }
                            return false
                        }
                        if isNewPeer {
                            guard await self.registerWriter(
                                outbound,
                                channel: channel,
                                channelID: channelID,
                                for: frame.sender
                            ) else {
                                break
                            }
                        }
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
                        group.addTask {
                            await self.handleInboundFrame(frame)
                        }
                    }
                    group.cancelAll()
                }
            }
        } catch {
            logger.debug("Peer channel closed: \(error)")
        }
    }

    @discardableResult
    private func registerWriter(
        _ writer: Writer,
        channel: PeerChannel,
        channelID: UUID,
        for peer: Cluster.Node
    ) async -> Bool {
        let (accepted, waiters, isPrimary, onConnected) = state.withLock {
            s -> (Bool, [CheckedContinuation<Writer, any Error>], Bool, (@Sendable (Cluster.Node) async -> Void)?) in
            if s.isShutdown {
                return (false, [], false, nil)
            }
            if let existing = s.peers[peer], existing.id != channelID {
                s.extraChannels[channelID] = channel
                let pendingWaiters = s.connectWaiters.removeValue(forKey: peer) ?? []
                return (true, pendingWaiters, false, nil)
            }
            s.peers[peer] = PeerConnection(id: channelID, writer: writer, channel: channel)
            let pendingWaiters = s.connectWaiters.removeValue(forKey: peer) ?? []
            return (true, pendingWaiters, true, s.onPeerConnected)
        }
        guard accepted else { return false }
        let writerToResume = isPrimary ? writer : state.withLock { $0.peers[peer]?.writer ?? writer }
        for waiter in waiters {
            waiter.resume(returning: writerToResume)
        }
        if isPrimary {
            await onConnected?(peer)
        }
        return true
    }

    private func handleInboundFrame(_ frame: WireFrame) async {
        switch frame.type {
        case .reply:
            resumePending(correlationID: frame.correlationID, result: .success(frame.payload))
        case .error:
            let failure = decodeThrownError(from: frame.payload)
            resumePending(correlationID: frame.correlationID, result: .failure(failure))
        case .heartbeat:
            break
        case .remoteCall, .remoteCallVoid, .watchActor, .unwatchActor, .actorTerminated, .receptionistSync, .swimGossip:
            let callback = state.withLock { $0.onFrame }
            await callback?(frame, frame.sender)
        }
    }

    func handlePeerDown(_ peer: Cluster.Node, channelID: UUID) async {
        let (isCurrent, pendingCalls, waiters, callback) = state.withLock {
            s -> (Bool, [PendingCall], [CheckedContinuation<Writer, any Error>], (@Sendable (Cluster.Node) async -> Void)?) in
            s.extraChannels.removeValue(forKey: channelID)
            guard s.peers[peer]?.id == channelID else {
                return (false, [], [], nil)
            }
            s.peers.removeValue(forKey: peer)
            var removed: [PendingCall] = []
            s.pending = s.pending.filter { entry in
                if entry.value.node == peer {
                    removed.append(entry.value)
                    return false
                }
                return true
            }
            let pendingWaiters = s.connectWaiters.removeValue(forKey: peer) ?? []
            return (true, removed, pendingWaiters, s.onNodeDown)
        }
        guard isCurrent else { return }
        for call in pendingCalls {
            call.continuation.resume(throwing: ClusterError.nodeUnreachable(peer))
        }
        for waiter in waiters {
            waiter.resume(throwing: ClusterError.nodeUnreachable(peer))
        }
        await callback?(peer)
    }

    func writer(for node: Cluster.Node) async throws(ClusterError) -> Writer {
        if let existing = state.withLock({ $0.peers[node]?.writer }) {
            return existing
        }
        do {
            return try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Writer, any Error>) in
                var shouldConnect = false
                state.withLock { s in
                    if s.isShutdown {
                        continuation.resume(throwing: ClusterError.systemShuttingDown)
                        return
                    }
                    if let writer = s.peers[node]?.writer {
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
                    self.wrapPeerChannel(channel, isServer: false, remoteHost: node.endpoint.host)
                }
            let taskID = UUID()
            let task = Swift::Task {
                defer {
                    _ = self.state.withLock { $0.peerTasks.removeValue(forKey: taskID) }
                }
                await self.supervisePeerChannel(channel, initialPeer: node, channelID: taskID)
            }
            let inserted = state.withLock { s -> Bool in
                if s.isShutdown { return false }
                s.peerTasks[taskID] = task
                return true
            }
            if !inserted {
                task.cancel()
                try? await channel.channel.close()
                let waiters = state.withLock { s in
                    s.connectWaiters.removeValue(forKey: node) ?? []
                }
                for waiter in waiters {
                    waiter.resume(throwing: ClusterError.systemShuttingDown)
                }
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
    ) async throws -> Data {
        let correlationID = frame.correlationID
        let timeoutTask = Mutex<Swift::Task<Void, Never>?>(nil)
        let sendTask = Mutex<Swift::Task<Void, Never>?>(nil)
        defer {
            timeoutTask.withLock { $0 }?.cancel()
            sendTask.withLock { $0 }?.cancel()
        }

        return try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Data, any Error>) in
                let registered = state.withLock { s -> Bool in
                    if s.isShutdown {
                        return false
                    }
                    s.pending[correlationID] = PendingCall(node: node, target: target, continuation: continuation)
                    return true
                }
                guard registered else {
                    continuation.resume(throwing: ClusterError.systemShuttingDown)
                    return
                }
                let sTask = Swift::Task {
                    do {
                        try await self.sendFrame(frame, to: node)
                    } catch let error as ClusterError {
                        self.failPending(correlationID: correlationID, error: error)
                    } catch {
                        self.failPending(correlationID: correlationID, error: .nodeUnreachable(node))
                    }
                }
                sendTask.withLock { $0 = sTask }

                let tTask = Swift::Task {
                    do {
                        try await Swift::Task.sleep(for: timeout)
                        self.failPending(
                            correlationID: correlationID,
                            error: .invocationTimeout(correlationID: correlationID, target: target)
                        )
                    } catch {
                        // Cancelled when the RPC completed or caller cancelled.
                    }
                }
                timeoutTask.withLock { $0 = tTask }
            }
        } onCancel: {
            self.cancelPending(correlationID: correlationID)
        }
    }

    func cancelPending(correlationID: UInt64) {
        let call = state.withLock { s in
            s.pending.removeValue(forKey: correlationID)
        }
        call?.continuation.resume(throwing: CancellationError())
    }

    func failPending(correlationID: UInt64, error: ClusterError) {
        let call = state.withLock { s in
            s.pending.removeValue(forKey: correlationID)
        }
        call?.continuation.resume(throwing: error)
    }

    func resumePending(correlationID: UInt64, result: Result<Data, any Error>) {
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
            Array(s.peers.keys)
        }
    }

    func shutdown() async {
        let (serverChannel, channels, acceptTask, peerTasks, pendingCalls, waiters) = state.withLock {
            s -> (ServerChannel?, [PeerChannel], Swift::Task<Void, Never>?, [Swift::Task<Void, Never>], [PendingCall], [CheckedContinuation<Writer, any Error>]) in
            if s.isShutdown {
                return (nil, [], nil, [], [], [])
            }
            s.isShutdown = true
            let pending = Array(s.pending.values)
            s.pending.removeAll()
            var allWaiters: [CheckedContinuation<Writer, any Error>] = []
            for (_, list) in s.connectWaiters {
                allWaiters.append(contentsOf: list)
            }
            s.connectWaiters.removeAll()
            let chans = s.peers.values.map(\.channel) + Array(s.extraChannels.values)
            s.peers.removeAll()
            s.extraChannels.removeAll()
            let server = s.serverChannel
            s.serverChannel = nil
            let accept = s.acceptTask
            s.acceptTask = nil
            let pTasks = Array(s.peerTasks.values)
            s.peerTasks.removeAll()
            s.onFrame = nil
            s.onPeerConnected = nil
            s.onNodeDown = nil
            return (server, chans, accept, pTasks, pending, allWaiters)
        }
        for call in pendingCalls {
            call.continuation.resume(throwing: ClusterError.systemShuttingDown)
        }
        for waiter in waiters {
            waiter.resume(throwing: ClusterError.systemShuttingDown)
        }
        await withTaskCancellationShield {
            acceptTask?.cancel()
            for task in peerTasks {
                task.cancel()
            }
            if let serverChannel {
                try? await serverChannel.channel.close()
            }
            for channel in channels {
                try? await channel.channel.close()
            }
            await acceptTask?.value
            for task in peerTasks {
                await task.value
            }
            try? await group.shutdownGracefully()
        }
    }
}
