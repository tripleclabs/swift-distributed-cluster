import Distributed
import Foundation
import Logging
import ServiceLifecycle
import Synchronization

public final class ClusterSystem: Distributed::DistributedActorSystem, Service, Sendable {
    public typealias ActorID = ClusterActorID
    public typealias InvocationEncoder = ClusterInvocationEncoder
    public typealias InvocationDecoder = ClusterInvocationDecoder
    public typealias ResultHandler = ClusterResultHandler
    public typealias SerializationRequirement = Codable

    public struct Settings: Sendable {
        public var bindHost: String = "127.0.0.1"
        public var bindPort: Int = 2552
        public var seedNodes: [Cluster.Endpoint] = []
        public var requestTimeout: Duration = .seconds(10)
        public var heartbeatInterval: Duration = .seconds(2)
        public var heartbeatTimeout: Duration = .seconds(6)
        public var logger: Logger

        public init(nodeName: String) {
            self.logger = Logger(label: "cluster.\(nodeName)")
        }
    }

    internal struct WeakActorBox: Sendable {
        weak let actor: any DistributedActor?
    }

    internal struct WeakWatcherBox: Sendable {
        weak let watcher: any LifecycleWatch?
    }

    internal typealias ActorExecutor = @Sendable (ClusterInvocationDecoder, ClusterResultHandler) async throws -> Void

    internal struct ActorEntry: Sendable {
        let box: WeakActorBox
        let executor: ActorExecutor
    }

    internal struct State: Sendable {
        var localActors: [ActorID: ActorEntry] = [:]
        var watchersBySubject: [ActorID: [ActorID: WeakWatcherBox]] = [:]
        var subjectsByWatcher: [ActorID: Set<ActorID>] = [:]
        var remoteSubjectsByNode: [Cluster.Node: Set<ActorID>] = [:]
        var nextIncarnation: UInt64 = 1
        var isShuttingDown: Bool = false
    }

    public let name: String
    public let node: Cluster.Node
    public let settings: Settings
    public let receptionist: ClusterReceptionist

    internal let state = Mutex(State())
    internal let transport: ClusterTransport
    internal let terminatedContinuation: AsyncStream<Void>.Continuation
    public let terminated: AsyncStream<Void>

    public init(
        _ name: String,
        configuring configure: (inout Settings) -> Void = { _ in }
    ) async throws(ClusterError) {
        var resolved = Settings(nodeName: name)
        configure(&resolved)

        self.name = name
        self.settings = resolved
        self.node = Cluster.Node(
            name: name,
            endpoint: Cluster.Endpoint(host: resolved.bindHost, port: resolved.bindPort)
        )

        let (stream, continuation) = AsyncStream<Void>.makeStream()
        self.terminated = stream
        self.terminatedContinuation = continuation
        self.receptionist = ClusterReceptionist(localNode: self.node)
        self.transport = ClusterTransport(localNode: self.node, settings: resolved)

        self.receptionist.attach(system: self)

        try await self.transport.start(
            onFrame: { [weak self] frame, peer in
                await self?.handleInboundFrame(frame, from: peer)
            },
            onNodeDown: { [weak self] deadNode in
                await self?.handleNodeDown(deadNode)
            }
        )
    }

    public func run() async throws {
        for await _ in terminated {}
    }

    public func shutdown() async {
        let alreadyShuttingDown = state.withLock { s -> Bool in
            if s.isShuttingDown { return true }
            s.isShuttingDown = true
            return false
        }
        guard !alreadyShuttingDown else { return }

        await withTaskCancellationShield {
            await transport.shutdown()
            terminatedContinuation.finish()
        }
    }

    public func assignID<Act>(_ actorType: Act.Type) -> ActorID where Act: DistributedActor {
        state.withLock { s in
            let inc = s.nextIncarnation
            s.nextIncarnation &+= 1
            let path = "\(String(describing: actorType))-\(inc)"
            return ClusterActorID(node: node, path: path, incarnation: inc)
        }
    }

    public func actorReady<Act>(_ actor: Act) where Act: DistributedActor, Act.ID == ActorID {
        let actorID = actor.id
        let executor: ActorExecutor = { [weak actor, weak self] decoder, handler in
            guard let actor else {
                throw ClusterError.remoteActorNotFound(actorID)
            }
            guard let self else {
                throw ClusterError.systemShuttingDown
            }
            var mutableDecoder = decoder
            let target = Distributed::RemoteCallTarget(decoder.payload.targetIdentifier)
            try await self.executeDistributedTarget(
                on: actor,
                target: target,
                invocationDecoder: &mutableDecoder,
                handler: handler
            )
        }
        state.withLock { s in
            s.localActors[actor.id] = ActorEntry(box: WeakActorBox(actor: actor), executor: executor)
        }
    }

    public func resignID(_ id: ActorID) {
        let (localWatchers, remoteWatcherNodes, hadActor) = state.withLock { s -> ([any LifecycleWatch], Set<Cluster.Node>, Bool) in
            let had = s.localActors.removeValue(forKey: id) != nil

            if let watchedSubjects = s.subjectsByWatcher.removeValue(forKey: id) {
                for subject in watchedSubjects {
                    s.watchersBySubject[subject]?.removeValue(forKey: id)
                }
            }

            let entries = s.watchersBySubject.removeValue(forKey: id) ?? [:]
            var resolvedLocals: [any LifecycleWatch] = []
            var remoteNodes: Set<Cluster.Node> = []

            for (watcherID, box) in entries {
                if watcherID.node == self.node {
                    if let watcher = box.watcher {
                        resolvedLocals.append(watcher)
                    }
                    s.subjectsByWatcher[watcherID]?.remove(id)
                } else {
                    remoteNodes.insert(watcherID.node)
                }
            }
            return (resolvedLocals, remoteNodes, had)
        }

        if hadActor || !localWatchers.isEmpty || !remoteWatcherNodes.isEmpty {
            Swift::Task {
                await withTaskCancellationShield {
                    await self.receptionist.removeActor(id)
                    for watcher in localWatchers {
                        await self.deliverTermination(to: watcher, subjectID: id)
                    }
                    for remoteNode in remoteWatcherNodes {
                        await self.transport.sendControlFrame(
                            .actorTerminated(subjectID: id),
                            to: remoteNode
                        )
                    }
                }
            }
        }
    }

    public func resolve<Act>(id: ActorID, as actorType: Act.Type) throws -> Act?
    where Act: DistributedActor, Act.ID == ActorID {
        guard id.node == self.node else {
            return nil
        }
        return state.withLock { s in
            guard let anyActor = s.localActors[id]?.box.actor else {
                return nil
            }
            return anyActor as? Act
        }
    }

    public func makeInvocationEncoder() -> InvocationEncoder {
        ClusterInvocationEncoder()
    }

    public func remoteCall<Act, Err, Res>(
        on actor: Act,
        target: Distributed::RemoteCallTarget,
        invocation: inout InvocationEncoder,
        throwing: Err.Type,
        returning: Res.Type
    ) async throws -> Res where Act: DistributedActor, Err: Error, Res: Codable, Act.ID == ActorID {
        let isShuttingDown = state.withLock { $0.isShuttingDown }
        if isShuttingDown {
            throw ClusterError.systemShuttingDown
        }
        let invocationData = try invocation.makePayload(targetIdentifier: target.identifier)
        let envelope = RemoteCallEnvelope(targetID: actor.id, invocationData: invocationData)
        let envelopeData = try encodeCodableValue(envelope)
        let correlationID = transport.nextCorrelationID()
        let frame = WireFrame(type: .remoteCall, correlationID: correlationID, sender: node, payload: envelopeData)
        let replyData = try await transport.performRemoteCall(
            frame: frame,
            to: actor.id.node,
            target: actor.id,
            timeout: settings.requestTimeout
        )
        if Res.self == Void.self, let voidValue = () as? Res {
            return voidValue
        }
        return try decodeCodableValue(Res.self, from: replyData)
    }

    public func remoteCallVoid<Act, Err>(
        on actor: Act,
        target: Distributed::RemoteCallTarget,
        invocation: inout InvocationEncoder,
        throwing: Err.Type
    ) async throws where Act: DistributedActor, Err: Error, Act.ID == ActorID {
        let isShuttingDown = state.withLock { $0.isShuttingDown }
        if isShuttingDown {
            throw ClusterError.systemShuttingDown
        }
        let invocationData = try invocation.makePayload(targetIdentifier: target.identifier)
        let envelope = RemoteCallEnvelope(targetID: actor.id, invocationData: invocationData)
        let envelopeData = try encodeCodableValue(envelope)
        let correlationID = transport.nextCorrelationID()
        let frame = WireFrame(type: .remoteCallVoid, correlationID: correlationID, sender: node, payload: envelopeData)
        _ = try await transport.performRemoteCall(
            frame: frame,
            to: actor.id.node,
            target: actor.id,
            timeout: settings.requestTimeout
        )
    }

    internal func registerWatch(watcher: any LifecycleWatch, subjectID: ActorID) {
        let watcherID = watcher.id
        guard watcherID != subjectID else { return }

        enum WatchAction {
            case none
            case noproc
            case remote(Cluster.Node)
        }

        let action = state.withLock { s -> WatchAction in
            if subjectID.node == self.node {
                guard s.localActors[subjectID]?.box.actor != nil else {
                    return .noproc
                }
                s.watchersBySubject[subjectID, default: [:]][watcherID] = WeakWatcherBox(watcher: watcher)
                s.subjectsByWatcher[watcherID, default: []].insert(subjectID)
                return .none
            } else {
                s.watchersBySubject[subjectID, default: [:]][watcherID] = WeakWatcherBox(watcher: watcher)
                s.subjectsByWatcher[watcherID, default: []].insert(subjectID)
                s.remoteSubjectsByNode[subjectID.node, default: []].insert(subjectID)
                return .remote(subjectID.node)
            }
        }

        switch action {
        case .none:
            break
        case .noproc:
            Swift::Task {
                await withTaskCancellationShield {
                    await self.deliverTermination(to: watcher, subjectID: subjectID)
                }
            }
        case .remote(let peerNode):
            Swift::Task {
                do {
                    try await self.transport.sendControlFrameOrThrow(
                        .watchActor(watcherID: watcherID, subjectID: subjectID),
                        to: peerNode
                    )
                } catch {
                    await withTaskCancellationShield {
                        await self.deliverTermination(to: watcher, subjectID: subjectID)
                    }
                }
            }
        }
    }

    internal func unregisterWatch(watcherID: ActorID, subjectID: ActorID) {
        state.withLock { s in
            s.watchersBySubject[subjectID]?.removeValue(forKey: watcherID)
            s.subjectsByWatcher[watcherID]?.remove(subjectID)
            if subjectID.node != self.node {
                s.remoteSubjectsByNode[subjectID.node]?.remove(subjectID)
            }
        }
        if subjectID.node != node {
            Swift::Task {
                await self.transport.sendControlFrame(
                    .unwatchActor(watcherID: watcherID, subjectID: subjectID),
                    to: subjectID.node
                )
            }
        }
    }

    internal func broadcastReceptionistSync(keyID: String, actorID: ActorID, removed: Bool) async {
        for peer in transport.knownPeers() where peer != node {
            await transport.sendControlFrame(
                .receptionistSync(keyID: keyID, actorID: actorID, removed: removed),
                to: peer
            )
        }
    }

    internal func handleNodeDown(_ deadNode: Cluster.Node) async {
        let watchersToNotify = state.withLock { s -> [(ActorID, any LifecycleWatch)] in
            let subjects = s.remoteSubjectsByNode.removeValue(forKey: deadNode) ?? []
            var result: [(ActorID, any LifecycleWatch)] = []
            for subject in subjects {
                let entries = s.watchersBySubject.removeValue(forKey: subject) ?? [:]
                for (watcherID, box) in entries where watcherID.node == self.node {
                    if let watcher = box.watcher {
                        result.append((subject, watcher))
                    }
                    s.subjectsByWatcher[watcherID]?.remove(subject)
                }
            }
            return result
        }
        for (subject, watcher) in watchersToNotify {
            await self.deliverTermination(to: watcher, subjectID: subject)
        }
    }

    internal func handleInboundFrame(_ frame: WireFrame, from peer: Cluster.Node) async {
        switch frame.type {
        case .remoteCall, .remoteCallVoid:
            await handleRemoteCallFrame(frame, from: peer)
        case .watchActor:
            guard let envelope = try? decodeCodableValue(WatchEnvelope.self, from: frame.payload) else { return }
            let subjectAlive = state.withLock { s -> Bool in
                guard s.localActors[envelope.subjectID]?.box.actor != nil else {
                    return false
                }
                s.watchersBySubject[envelope.subjectID, default: [:]][envelope.watcherID] = WeakWatcherBox(watcher: nil)
                return true
            }
            if !subjectAlive {
                await transport.sendControlFrame(.actorTerminated(subjectID: envelope.subjectID), to: envelope.watcherID.node)
            }
        case .unwatchActor:
            guard let envelope = try? decodeCodableValue(WatchEnvelope.self, from: frame.payload) else { return }
            state.withLock { s in
                _ = s.watchersBySubject[envelope.subjectID]?.removeValue(forKey: envelope.watcherID)
            }
        case .actorTerminated:
            guard let envelope = try? decodeCodableValue(TerminatedEnvelope.self, from: frame.payload) else { return }
            await notifyLocalWatchers(subjectID: envelope.subjectID)
        case .receptionistSync:
            guard let envelope = try? decodeCodableValue(ReceptionistSyncEnvelope.self, from: frame.payload) else { return }
            receptionist.applyRemoteSync(keyID: envelope.keyID, actorID: envelope.actorID, removed: envelope.removed)
        case .reply, .error, .heartbeat:
            break
        }
    }

    internal func deliverTermination(to watcher: any LifecycleWatch, subjectID: ActorID) async {
        _ = await watcher.whenLocal { isolatedWatcher in
            await isolatedWatcher.terminated(actor: subjectID)
        }
    }

    internal func notifyLocalWatchers(subjectID: ActorID) async {
        let watchers = state.withLock { s -> [any LifecycleWatch] in
            let entries = s.watchersBySubject.removeValue(forKey: subjectID) ?? [:]
            var result: [any LifecycleWatch] = []
            for (watcherID, box) in entries where watcherID.node == self.node {
                if let watcher = box.watcher {
                    result.append(watcher)
                }
                s.subjectsByWatcher[watcherID]?.remove(subjectID)
            }
            s.remoteSubjectsByNode[subjectID.node]?.remove(subjectID)
            return result
        }
        for watcher in watchers {
            await self.deliverTermination(to: watcher, subjectID: subjectID)
        }
    }

    internal func handleRemoteCallFrame(_ frame: WireFrame, from peer: Cluster.Node) async {
        let envelope: RemoteCallEnvelope
        do {
            envelope = try decodeCodableValue(RemoteCallEnvelope.self, from: frame.payload)
        } catch {
            await sendErrorFrame(correlationID: frame.correlationID, error: .decodingFailed("Unable to decode remote call envelope"), to: peer)
            return
        }

        let entry = state.withLock { s in
            s.localActors[envelope.targetID]
        }
        guard let entry, entry.box.actor != nil else {
            await sendErrorFrame(correlationID: frame.correlationID, error: .remoteActorNotFound(envelope.targetID), to: peer)
            return
        }

        let decoder: ClusterInvocationDecoder
        do {
            decoder = try ClusterInvocationDecoder(data: envelope.invocationData)
        } catch {
            await sendErrorFrame(correlationID: frame.correlationID, error: error, to: peer)
            return
        }

        let handler = ClusterResultHandler(correlationID: frame.correlationID) { [weak self] type, correlationID, data in
            guard let self else { return }
            let replyFrame = WireFrame(type: type, correlationID: correlationID, sender: self.node, payload: data)
            do {
                try await self.transport.sendFrame(replyFrame, to: peer)
            } catch {
                // Caller already gone or node unreachable; nothing further to do.
            }
        }

        do {
            try await entry.executor(decoder, handler)
        } catch {
            let clusterError = (error as? ClusterError) ?? .decodingFailed("\(error)")
            await sendErrorFrame(correlationID: frame.correlationID, error: clusterError, to: peer)
        }
    }

    internal func sendErrorFrame(correlationID: UInt64, error: ClusterError, to peer: Cluster.Node) async {
        let envelope = ErrorEnvelope(clusterError: error, message: "\(error)")
        guard let payload = try? encodeCodableValue(envelope) else { return }
        let frame = WireFrame(type: .error, correlationID: correlationID, sender: node, payload: payload)
        do {
            try await transport.sendFrame(frame, to: peer)
        } catch {
            // Peer unreachable while reporting an error; drop the frame.
        }
    }
}
