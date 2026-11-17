import Distributed
import Foundation
import Logging
import ServiceLifecycle
import Synchronization
#if SWIMDiscovery
import SWIM
#endif

public final class ClusterSystem: Distributed::DistributedActorSystem, Service, Sendable {
    public typealias ActorID = ClusterActorID
    public typealias InvocationEncoder = ClusterInvocationEncoder
    public typealias InvocationDecoder = ClusterInvocationDecoder
    public typealias ResultHandler = ClusterResultHandler
    public typealias SerializationRequirement = Codable

    public struct Settings: Sendable {
        public var nodeName: String
        public var bindHost: String = "127.0.0.1"
        public var bindPort: Int = 2552
        public var seedNodes: [Cluster.Endpoint] = []
        public var requestTimeout: Duration = .seconds(10)
        public var heartbeatInterval: Duration = .seconds(2)
        public var heartbeatTimeout: Duration = .seconds(6)
        public var logging: LoggingSettings
        public var logger: Logger {
            get { logging.baseLogger }
            set { logging.baseLogger = newValue }
        }
        public var plugins: PluginsSettings = PluginsSettings()
        public var autoLeaderElection: Cluster.AutoLeaderElectionSettings = .none
        #if SWIMDiscovery
        public var swim: SWIM.Settings = SWIM.Settings()
        #endif
        #if MutualTLS
        public var tls: ClusterTLSConfiguration? = nil
        #endif

        public init(nodeName: String) {
            self.nodeName = nodeName
            self.logging = LoggingSettings(baseLogger: Logger(label: "cluster.\(nodeName)"))
        }

        public init(name: String, host: String = "127.0.0.1", port: Int = 2552) {
            self.nodeName = name
            self.bindHost = host
            self.bindPort = port
            self.logging = LoggingSettings(baseLogger: Logger(label: "cluster.\(name)"))
        }
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
        var clusterEventContinuations: [UUID: AsyncStream<Cluster.Event>.Continuation] = [:]
        var hostedSingletonPassivators: [@Sendable () async -> Void] = []
    }

    public let name: String
    public let node: Cluster.Node
    public let settings: Settings
    public let receptionist: ClusterReceptionist
    private let localReceptionistStorage = Mutex<LocalReceptionist?>(nil)

    public var localReceptionist: LocalReceptionist {
        localReceptionistStorage.withLock { existing in
            if let existing {
                return existing
            }
            let created = LocalReceptionist(system: self)
            existing = created
            return created
        }
    }

    public var cluster: ClusterControl {
        ClusterControl(system: self)
    }

    public var singleton: ClusterSingletonControl {
        ClusterSingletonControl(system: self)
    }

    internal let state = Mutex(State())
    internal let transport: ClusterTransport
    #if SWIMDiscovery
    internal let swimAdapter: SWIMMembershipAdapter

    public var swimMembers: [Cluster.Node] {
        swimAdapter.members()
    }
    #endif
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
        #if SWIMDiscovery
        self.swimAdapter = SWIMMembershipAdapter(localNode: self.node, settings: resolved.swim, transport: self.transport)
        #endif

        self.receptionist.attach(system: self)

        try await self.transport.start(
            onFrame: { [weak self] frame, peer in
                await self?.handleInboundFrame(frame, from: peer)
            },
            onPeerConnected: { [weak self] peer in
                guard let self else { return }
                await self.syncReceptionist(to: peer)
                #if SWIMDiscovery
                await self.swimAdapter.peerConnected(peer)
                #endif
                self.emitClusterEvent(.membershipChange(Cluster.MembershipChange(
                    node: peer,
                    previousStatus: nil,
                    status: .up
                )))
            },
            onNodeDown: { [weak self] deadNode in
                await self?.handleNodeDown(deadNode)
            }
        )

        #if SWIMDiscovery
        self.swimAdapter.start(
            onMemberAlive: { [weak self] aliveNode in
                await self?.syncReceptionist(to: aliveNode)
            },
            onMemberDown: { [weak self] deadNode in
                await self?.handleNodeDown(deadNode)
            }
        )
        #endif

        if !resolved.seedNodes.isEmpty {
            self.transport.connectToSeedNodes(resolved.seedNodes)
        }
    }

    public convenience init(settings: ClusterSystemSettings) async {
        do {
            try await self.init(settings.nodeName) { $0 = settings }
        } catch {
            fatalError("ClusterSystem(settings:) failed to start: \(error)")
        }
    }

    public convenience init(_ name: String, settings: ClusterSystemSettings) async {
        do {
            try await self.init(name) { $0 = settings }
        } catch {
            fatalError("ClusterSystem(\(name), settings:) failed to start: \(error)")
        }
    }

    internal func registerHostedSingleton(_ passivate: @escaping @Sendable () async -> Void) {
        state.withLock { s in
            s.hostedSingletonPassivators.append(passivate)
        }
    }

    internal func makeClusterEventStream() -> AsyncStream<Cluster.Event> {
        let id = UUID()
        let (stream, continuation) = AsyncStream<Cluster.Event>.makeStream()
        let isShuttingDown = state.withLock { s -> Bool in
            if s.isShuttingDown { return true }
            s.clusterEventContinuations[id] = continuation
            return false
        }
        if isShuttingDown {
            continuation.finish()
            return stream
        }
        continuation.onTermination = { [weak self] _ in
            guard let self else { return }
            _ = self.state.withLock { $0.clusterEventContinuations.removeValue(forKey: id) }
        }
        return stream
    }

    internal func emitClusterEvent(_ event: Cluster.Event) {
        let continuations = state.withLock { Array($0.clusterEventContinuations.values) }
        for continuation in continuations {
            continuation.yield(event)
        }
    }

    public func run() async throws {
        for await _ in terminated {}
    }

    public func shutdown() async {
        let (alreadyShuttingDown, eventContinuations, passivators) = state.withLock {
            s -> (Bool, [AsyncStream<Cluster.Event>.Continuation], [@Sendable () async -> Void]) in
            if s.isShuttingDown { return (true, [], []) }
            s.isShuttingDown = true
            let conts = Array(s.clusterEventContinuations.values)
            s.clusterEventContinuations.removeAll()
            let passivators = s.hostedSingletonPassivators
            s.hostedSingletonPassivators.removeAll()
            return (false, conts, passivators)
        }
        guard !alreadyShuttingDown else { return }

        await withTaskCancellationShield {
            for passivate in passivators {
                await passivate()
            }
            #if SWIMDiscovery
            await swimAdapter.shutdown()
            #endif
            await transport.shutdown()
            for continuation in eventContinuations {
                continuation.finish()
            }
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
        let (localWatchers, remoteWatcherNodes, unwatchRemotes, hadActor) = state.withLock {
            s -> ([any LifecycleWatch], Set<Cluster.Node>, [ActorID], Bool) in
            let had = s.localActors.removeValue(forKey: id) != nil

            var unwatches: [ActorID] = []
            if let watchedSubjects = s.subjectsByWatcher.removeValue(forKey: id) {
                for subject in watchedSubjects {
                    s.watchersBySubject[subject]?.removeValue(forKey: id)
                    if s.watchersBySubject[subject]?.isEmpty == true {
                        s.watchersBySubject.removeValue(forKey: subject)
                    }
                    if subject.node != self.node {
                        unwatches.append(subject)
                    }
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
                    if s.subjectsByWatcher[watcherID]?.isEmpty == true {
                        s.subjectsByWatcher.removeValue(forKey: watcherID)
                    }
                } else {
                    remoteNodes.insert(watcherID.node)
                }
            }
            return (resolvedLocals, remoteNodes, unwatches, had)
        }

        if hadActor || !localWatchers.isEmpty || !remoteWatcherNodes.isEmpty || !unwatchRemotes.isEmpty {
            Swift::Task {
                await withTaskCancellationShield {
                    await self.receptionist.removeActor(id)
                    if let localReg = self.localReceptionistStorage.withLock({ $0 }) {
                        await localReg.terminated(actor: id)
                    }
                    for subject in unwatchRemotes {
                        await self.transport.sendControlFrame(
                            .unwatchActor(watcherID: id, subjectID: subject),
                            to: subject.node
                        )
                    }
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

    private func sendRemoteCallEnvelope(
        type: WireHeader.FrameType,
        targetID: ActorID,
        target: Distributed::RemoteCallTarget,
        invocation: inout InvocationEncoder
    ) async throws -> Data {
        let isShuttingDown = state.withLock { $0.isShuttingDown }
        if isShuttingDown {
            throw ClusterError.systemShuttingDown
        }
        let invocationData = try invocation.makePayload(targetIdentifier: target.identifier)
        let envelope = RemoteCallEnvelope(targetID: targetID, invocationData: invocationData)
        let envelopeData = try envelope.encodeBinary()
        let correlationID = transport.nextCorrelationID()
        let frame = WireFrame(type: type, correlationID: correlationID, sender: node, payload: envelopeData)
        return try await transport.performRemoteCall(
            frame: frame,
            to: targetID.node,
            target: targetID,
            timeout: RemoteCall.currentTimeout ?? settings.requestTimeout
        )
    }

    public func remoteCall<Act, Err, Res>(
        on actor: Act,
        target: Distributed::RemoteCallTarget,
        invocation: inout InvocationEncoder,
        throwing: Err.Type,
        returning: Res.Type
    ) async throws -> Res where Act: DistributedActor, Err: Error, Res: Codable, Act.ID == ActorID {
        let replyData = try await sendRemoteCallEnvelope(
            type: .remoteCall,
            targetID: actor.id,
            target: target,
            invocation: &invocation
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
        _ = try await sendRemoteCallEnvelope(
            type: .remoteCallVoid,
            targetID: actor.id,
            target: target,
            invocation: &invocation
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
            if s.watchersBySubject[subjectID]?.isEmpty == true {
                s.watchersBySubject.removeValue(forKey: subjectID)
            }
            s.subjectsByWatcher[watcherID]?.remove(subjectID)
            if s.subjectsByWatcher[watcherID]?.isEmpty == true {
                s.subjectsByWatcher.removeValue(forKey: watcherID)
            }
            if subjectID.node != self.node {
                s.remoteSubjectsByNode[subjectID.node]?.remove(subjectID)
                if s.remoteSubjectsByNode[subjectID.node]?.isEmpty == true {
                    s.remoteSubjectsByNode.removeValue(forKey: subjectID.node)
                }
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

    internal func syncReceptionist(to peer: Cluster.Node) async {
        guard peer != node else { return }
        for (keyID, actorID) in receptionist.allLocalRegistrations() {
            await transport.sendControlFrame(
                .receptionistSync(keyID: keyID, actorID: actorID, removed: false),
                to: peer
            )
        }
    }

    internal func handleNodeDown(_ deadNode: Cluster.Node) async {
        #if SWIMDiscovery
        swimAdapter.peerDisconnected(deadNode)
        #endif
        receptionist.removeRemoteActors(on: deadNode)
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
                    if s.subjectsByWatcher[watcherID]?.isEmpty == true {
                        s.subjectsByWatcher.removeValue(forKey: watcherID)
                    }
                }
            }
            for subject in Array(s.watchersBySubject.keys) {
                guard var entries = s.watchersBySubject[subject] else { continue }
                entries = entries.filter { $0.key.node != deadNode }
                if entries.isEmpty {
                    s.watchersBySubject.removeValue(forKey: subject)
                } else {
                    s.watchersBySubject[subject] = entries
                }
            }
            return result
        }
        for (subject, watcher) in watchersToNotify {
            await self.deliverTermination(to: watcher, subjectID: subject)
        }
        emitClusterEvent(.membershipChange(Cluster.MembershipChange(
            node: deadNode,
            previousStatus: .up,
            status: .down
        )))
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
                if s.watchersBySubject[envelope.subjectID]?.isEmpty == true {
                    s.watchersBySubject.removeValue(forKey: envelope.subjectID)
                }
            }
        case .actorTerminated:
            guard let envelope = try? decodeCodableValue(TerminatedEnvelope.self, from: frame.payload) else { return }
            await notifyLocalWatchers(subjectID: envelope.subjectID)
        case .receptionistSync:
            guard let envelope = try? decodeCodableValue(ReceptionistSyncEnvelope.self, from: frame.payload) else { return }
            receptionist.applyRemoteSync(keyID: envelope.keyID, actorID: envelope.actorID, removed: envelope.removed)
        case .swimGossip:
            #if SWIMDiscovery
            await swimAdapter.handleInboundGossip(frame.payload, from: peer)
            #endif
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
                if s.subjectsByWatcher[watcherID]?.isEmpty == true {
                    s.subjectsByWatcher.removeValue(forKey: watcherID)
                }
            }
            s.remoteSubjectsByNode[subjectID.node]?.remove(subjectID)
            if s.remoteSubjectsByNode[subjectID.node]?.isEmpty == true {
                s.remoteSubjectsByNode.removeValue(forKey: subjectID.node)
            }
            return result
        }
        for watcher in watchers {
            await self.deliverTermination(to: watcher, subjectID: subjectID)
        }
    }

    internal func handleRemoteCallFrame(_ frame: WireFrame, from peer: Cluster.Node) async {
        let envelope: RemoteCallEnvelope
        do {
            envelope = try RemoteCallEnvelope.decodeBinary(from: frame.payload)
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
        let envelope = ErrorEnvelope(clusterError: error, errorTypeName: nil, errorData: nil, message: "\(error)")
        guard let payload = try? encodeCodableValue(envelope) else { return }
        let frame = WireFrame(type: .error, correlationID: correlationID, sender: node, payload: payload)
        do {
            try await transport.sendFrame(frame, to: peer)
        } catch {
            // Peer unreachable while reporting an error; drop the frame.
        }
    }
}
