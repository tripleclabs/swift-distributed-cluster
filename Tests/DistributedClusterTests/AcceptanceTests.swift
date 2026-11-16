import Distributed
@testable import DistributedCluster
import Foundation
#if MutualTLS
import NIOSSL
#endif
import Testing

struct WorkerProblem: Error, Codable, Equatable, Sendable {
    let message: String
}

struct NonCodableProblem: Error, Sendable {
    let detail: String
}

distributed actor EchoWorker {
    typealias ActorSystem = ClusterSystem

    distributed func ping(_ message: String) -> String {
        "pong:\(message)"
    }

    distributed func echoGeneric<T: Codable & Sendable>(_ value: T) -> T {
        value
    }

    distributed func notifyOnly(_ value: Int) {
        _ = value
    }

    distributed func slowPing(_ message: String) async -> String {
        try? await Task.sleep(for: .seconds(3))
        return "slow:\(message)"
    }

    distributed func combine(_ text: String, _ count: Int, _ flag: Bool) -> String {
        "\(text)|\(count)|\(flag)"
    }

    distributed func sum(_ values: [Int]) -> Int {
        values.reduce(0, +)
    }

    distributed func failCustom(_ message: String) throws -> String {
        throw WorkerProblem(message: message)
    }

    distributed func failNonCodable(_ detail: String) throws -> String {
        throw NonCodableProblem(detail: detail)
    }

    distributed func failCluster() throws -> String {
        throw ClusterError.systemShuttingDown
    }
}

distributed actor TestSupervisor: LifecycleWatch {
    typealias ActorSystem = ClusterSystem

    private var terminatedIDs: [ClusterActorID] = []
    private var waiters: [CheckedContinuation<ClusterActorID, Never>] = []

    distributed func supervise(_ worker: EchoWorker) {
        watchTermination(of: worker)
    }

    distributed func unsupervise(_ worker: EchoWorker) {
        unwatchTermination(of: worker)
    }

    distributed func watchSelf() {
        watchTermination(of: self)
    }

    func terminated(actor id: ClusterActorID) async {
        if let waiter = waiters.first {
            waiters.removeFirst()
            waiter.resume(returning: id)
        } else {
            terminatedIDs.append(id)
        }
    }

    distributed func pendingTerminations() -> [ClusterActorID] {
        terminatedIDs
    }

    distributed func awaitNextTermination() async -> ClusterActorID {
        if !terminatedIDs.isEmpty {
            return terminatedIDs.removeFirst()
        }
        return await withCheckedContinuation { continuation in
            waiters.append(continuation)
        }
    }
}

@Suite("DistributedCluster Swift 6.4 Acceptance Tests")
struct DistributedClusterAcceptanceTests {

    @Test("AT-01: Local and Remote RPC Round-Trip (remoteCall & remoteCallVoid)")
    func remoteRPCRoundTrip() async throws {
        let nodeA = try await ClusterSystem("node-a") { $0.bindPort = 19101 }
        let nodeB = try await ClusterSystem("node-b") { $0.bindPort = 19102 }
        defer {
            await nodeA.shutdown()
            await nodeB.shutdown()
        }

        let workerOnB = EchoWorker(actorSystem: nodeB)
        let resolvedFromA = try EchoWorker.resolve(id: workerOnB.id, using: nodeA)

        let reply = try await resolvedFromA.ping("hello-otp")
        #expect(reply == "pong:hello-otp")

        try await resolvedFromA.notifyOnly(42)
    }

    @Test("AT-02: Local LifecycleWatch Fires on Worker Deallocation & Preserves Distinct Incarnation")
    func localLifecycleWatchAndIncarnation() async throws {
        let system = try await ClusterSystem("local-otp") { $0.bindPort = 19103 }
        defer { await system.shutdown() }

        let supervisor = TestSupervisor(actorSystem: system)
        var worker: EchoWorker? = EchoWorker(actorSystem: system)
        let firstID = try #require(worker?.id)

        try await supervisor.supervise(try #require(worker))

        worker = nil

        let deadID = try await supervisor.awaitNextTermination()
        #expect(deadID == firstID)

        let restartedWorker = EchoWorker(actorSystem: system)
        #expect(restartedWorker.id != firstID)
        #expect(restartedWorker.id.incarnation > firstID.incarnation)
    }

    @Test("AT-03: Cross-Node Remote LifecycleWatch Propagation")
    func remoteLifecycleWatchPropagation() async throws {
        let supervisorNode = try await ClusterSystem("sup-node") { $0.bindPort = 19104 }
        let workerNode = try await ClusterSystem("wrk-node") { $0.bindPort = 19105 }
        defer {
            await supervisorNode.shutdown()
            await workerNode.shutdown()
        }

        let supervisor = TestSupervisor(actorSystem: supervisorNode)
        var remoteWorker: EchoWorker? = EchoWorker(actorSystem: workerNode)
        let remoteID = try #require(remoteWorker?.id)

        let proxyOnSupNode = try EchoWorker.resolve(id: remoteID, using: supervisorNode)
        try await supervisor.supervise(proxyOnSupNode)

        try await Task.sleep(for: .milliseconds(150))

        remoteWorker = nil

        let observedDeadID = try await supervisor.awaitNextTermination()
        #expect(observedDeadID == remoteID)
    }

    @Test("AT-04: Abrupt Remote Node Shutdown Triggers Supervisor Fanout")
    func abruptNodeShutdownFanout() async throws {
        let supervisorNode = try await ClusterSystem("sup-node-2") { $0.bindPort = 19106 }
        let ephemeralNode = try await ClusterSystem("ephemeral-node") { $0.bindPort = 19107 }
        defer { await supervisorNode.shutdown() }

        let supervisor = TestSupervisor(actorSystem: supervisorNode)
        let worker = EchoWorker(actorSystem: ephemeralNode)
        let workerID = worker.id

        let proxy = try EchoWorker.resolve(id: workerID, using: supervisorNode)
        try await supervisor.supervise(proxy)
        try await Task.sleep(for: .milliseconds(150))

        await ephemeralNode.shutdown()
        withExtendedLifetime(worker) {}

        let observedDeadID = try await supervisor.awaitNextTermination()
        #expect(observedDeadID == workerID)
    }

    @Test("AT-05: Receptionist Keyed Registration & Automatic Cleanup on Exit")
    func receptionistLifecycle() async throws {
        let system = try await ClusterSystem("reception-node") { $0.bindPort = 19108 }
        defer { await system.shutdown() }

        let key = Reception.Key("workers", as: EchoWorker.self)
        var worker: EchoWorker? = EchoWorker(actorSystem: system)
        let workerID = try #require(worker?.id)

        await system.receptionist.register(try #require(worker), with: key)
        do {
            let active = await system.receptionist.lookup(key)
            #expect(active.map(\.id) == [workerID])
        }

        worker = nil
        try await Task.sleep(for: .milliseconds(100))

        let afterExit = await system.receptionist.lookup(key)
        #expect(afterExit.isEmpty)
    }

    @Test("AT-06: WireHeader Safe RawSpan Round-Trip & Truncation Rejection")
    func wireHeaderSafeSpanCodec() throws {
        let original = WireHeader(
            type: .remoteCall,
            correlationID: 0x0102_0304_0506_0708,
            payloadLength: 1024
        )
        let encoded: InlineArray<16, UInt8> = original.encode()

        let decoded = try WireHeader.decode(from: encoded.span.bytes)
        #expect(decoded == original)

        var truncated = InlineArray<5, UInt8>(repeating: 0)
        for i in 0..<5 {
            truncated[i] = encoded[i]
        }
        #expect(throws: ClusterError.self) {
            try WireHeader.decode(from: truncated.span.bytes)
        }
    }

    @Test("Edge: Watching an already-deallocated local actor delivers immediate noproc termination")
    func watchAlreadyDeadLocalActor() async throws {
        let system = try await ClusterSystem("noproc-node") { $0.bindPort = 19201 }
        defer { await system.shutdown() }

        let supervisor = TestSupervisor(actorSystem: system)
        var worker: EchoWorker? = EchoWorker(actorSystem: system)
        let deadID = try #require(worker?.id)

        worker = nil
        try await Task.sleep(for: .milliseconds(100))

        let ghost = try EchoWorker.resolve(id: deadID, using: system)
        try await supervisor.supervise(ghost)

        let observed = try await supervisor.awaitNextTermination()
        #expect(observed == deadID)
    }

    @Test("Edge: In-flight remoteCall fails fast with nodeUnreachable when peer shuts down mid-call")
    func inFlightCallFailsOnPeerShutdown() async throws {
        let callerNode = try await ClusterSystem("caller-node") { $0.bindPort = 19202 }
        let workerNode = try await ClusterSystem("slow-node") { $0.bindPort = 19203 }
        defer { await callerNode.shutdown() }

        let worker = EchoWorker(actorSystem: workerNode)
        let proxy = try EchoWorker.resolve(id: worker.id, using: callerNode)
        let expectedNode = workerNode.node

        let callTask = Task {
            try await proxy.slowPing("in-flight")
        }
        try await Task.sleep(for: .milliseconds(300))

        await workerNode.shutdown()
        withExtendedLifetime(worker) {}

        let result = await callTask.result
        switch result {
        case .success(let value):
            Issue.record("Expected nodeUnreachable failure, got success: \(value)")
        case .failure(let error):
            let clusterError = try #require(error as? ClusterError)
            #expect(clusterError == .nodeUnreachable(expectedNode))
        }
    }

    @Test("Edge: Multi-subscriber Receptionist listing stream updates")
    func receptionistListingMultiSubscriber() async throws {
        let system = try await ClusterSystem("listing-node") { $0.bindPort = 19204 }
        defer { await system.shutdown() }

        let key = Reception.Key("listed-workers", as: EchoWorker.self)
        let streamA = system.receptionist.listing(of: key)
        let streamB = system.receptionist.listing(of: key)

        var worker: EchoWorker? = EchoWorker(actorSystem: system)
        let workerID = try #require(worker?.id)
        await system.receptionist.register(try #require(worker), with: key)

        #expect(await listingContains(streamA, id: workerID))
        #expect(await listingContains(streamB, id: workerID))

        worker = nil

        async let emptyA = listingEventuallyEmpty(streamA)
        async let emptyB = listingEventuallyEmpty(streamB)
        #expect(await emptyA)
        #expect(await emptyB)

        let afterExit = await system.receptionist.lookup(key)
        #expect(afterExit.isEmpty)

        let freshStream = system.receptionist.listing(of: key)
        var iterator = freshStream.makeAsyncIterator()
        let first = await iterator.next()
        #expect(first?.isEmpty == true)
    }

    @Test("Remote thrown errors propagate to caller (custom and ClusterError)")
    func remoteThrowPropagation() async throws {
        let nodeA = try await ClusterSystem("throw-a") { $0.bindPort = 19301 }
        let nodeB = try await ClusterSystem("throw-b") { $0.bindPort = 19302 }
        defer {
            await nodeA.shutdown()
            await nodeB.shutdown()
        }
        let worker = EchoWorker(actorSystem: nodeB)
        let proxy = try EchoWorker.resolve(id: worker.id, using: nodeA)

        do {
            _ = try await proxy.failCustom("boom")
            Issue.record("Expected WorkerProblem to be thrown")
        } catch let error as WorkerProblem {
            #expect(error == WorkerProblem(message: "boom"))
        }

        do {
            _ = try await proxy.failNonCodable("unstructured")
            Issue.record("Expected fallback ClusterError to be thrown")
        } catch let error as ClusterError {
            guard case .decodingFailed(let message) = error else {
                Issue.record("Expected decodingFailed wrapping non-Codable error, got \(error)")
                return
            }
            #expect(message.contains("unstructured"))
        }

        do {
            _ = try await proxy.failCluster()
            Issue.record("Expected ClusterError to be thrown")
        } catch let error as ClusterError {
            #expect(error == .systemShuttingDown)
        }
    }

    @Test("Remote call times out with invocationTimeout")
    func remoteInvocationTimeout() async throws {
        let caller = try await ClusterSystem("timeout-a") {
            $0.bindPort = 19303
            $0.requestTimeout = .milliseconds(250)
        }
        let workerNode = try await ClusterSystem("timeout-b") { $0.bindPort = 19304 }
        defer {
            await caller.shutdown()
            await workerNode.shutdown()
        }
        let worker = EchoWorker(actorSystem: workerNode)
        let proxy = try EchoWorker.resolve(id: worker.id, using: caller)

        do {
            _ = try await proxy.slowPing("too-slow")
            Issue.record("Expected invocationTimeout")
        } catch let error as ClusterError {
            guard case .invocationTimeout(_, let target) = error else {
                Issue.record("Expected invocationTimeout, got \(error)")
                return
            }
            #expect(target == worker.id)
        }
    }

    @Test("Remote call to deallocated actor fails with remoteActorNotFound")
    func remoteCallDeadActor() async throws {
        let nodeA = try await ClusterSystem("dead-a") { $0.bindPort = 19305 }
        let nodeB = try await ClusterSystem("dead-b") { $0.bindPort = 19306 }
        defer {
            await nodeA.shutdown()
            await nodeB.shutdown()
        }
        var worker: EchoWorker? = EchoWorker(actorSystem: nodeB)
        let deadID = try #require(worker?.id)
        worker = nil
        try await Task.sleep(for: .milliseconds(150))

        let proxy = try EchoWorker.resolve(id: deadID, using: nodeA)
        do {
            _ = try await proxy.ping("anyone")
            Issue.record("Expected remoteActorNotFound")
        } catch let error as ClusterError {
            #expect(error == .remoteActorNotFound(deadID))
        }
    }

    @Test("Remote call to node with no listener fails with nodeUnreachable")
    func remoteCallGhostNode() async throws {
        let caller = try await ClusterSystem("ghost-a") { $0.bindPort = 19307 }
        defer { await caller.shutdown() }
        let ghostNode = Cluster.Node(
            name: "ghost",
            endpoint: Cluster.Endpoint(host: "127.0.0.1", port: 19399)
        )
        let ghostID = ClusterActorID(node: ghostNode, path: "EchoWorker-1", incarnation: 1)
        let proxy = try EchoWorker.resolve(id: ghostID, using: caller)
        do {
            _ = try await proxy.ping("anyone")
            Issue.record("Expected nodeUnreachable")
        } catch let error as ClusterError {
            #expect(error == .nodeUnreachable(ghostNode))
        }
    }

    @Test("Second system binding same port fails with bindFailed")
    func bindFailure() async throws {
        let first = try await ClusterSystem("bind-a") { $0.bindPort = 19308 }
        defer { await first.shutdown() }
        do {
            _ = try await ClusterSystem("bind-b") { $0.bindPort = 19308 }
            Issue.record("Expected bindFailed")
        } catch {
            guard case .bindFailed(let host, let port, _) = error else {
                Issue.record("Expected bindFailed, got \(error)")
                return
            }
            #expect(host == "127.0.0.1")
            #expect(port == 19308)
        }
    }

    @Test("Shutdown is idempotent, run() completes, calls after shutdown throw systemShuttingDown")
    func shutdownSemantics() async throws {
        let nodeA = try await ClusterSystem("shut-a") { $0.bindPort = 19309 }
        let nodeB = try await ClusterSystem("shut-b") { $0.bindPort = 19311 }
        defer { await nodeB.shutdown() }
        let worker = EchoWorker(actorSystem: nodeB)
        let proxy = try EchoWorker.resolve(id: worker.id, using: nodeA)

        let runTask = Task { try await nodeA.run() }
        await nodeA.shutdown()
        await nodeA.shutdown()
        let runResult = await runTask.result
        switch runResult {
        case .success:
            break
        case .failure(let error):
            Issue.record("run() failed unexpectedly: \(error)")
        }

        do {
            _ = try await proxy.ping("too-late")
            Issue.record("Expected systemShuttingDown")
        } catch let error as ClusterError {
            #expect(error == .systemShuttingDown)
        }
    }

    @Test("Local unwatchTermination stops termination delivery")
    func localUnwatch() async throws {
        let system = try await ClusterSystem("unwatch-local") { $0.bindPort = 19312 }
        defer { await system.shutdown() }
        let supervisor = TestSupervisor(actorSystem: system)
        var worker: EchoWorker? = EchoWorker(actorSystem: system)
        try await supervisor.supervise(try #require(worker))
        try await supervisor.unsupervise(try #require(worker))
        worker = nil
        #expect(try await expectNoTermination(supervisor, within: .milliseconds(400)))
    }

    @Test("Remote unwatchTermination stops termination delivery")
    func remoteUnwatch() async throws {
        let nodeA = try await ClusterSystem("unwatch-a") { $0.bindPort = 19313 }
        let nodeB = try await ClusterSystem("unwatch-b") { $0.bindPort = 19314 }
        defer {
            await nodeA.shutdown()
            await nodeB.shutdown()
        }
        let supervisor = TestSupervisor(actorSystem: nodeA)
        var worker: EchoWorker? = EchoWorker(actorSystem: nodeB)
        let proxy = try EchoWorker.resolve(id: try #require(worker?.id), using: nodeA)
        try await supervisor.supervise(proxy)
        try await Task.sleep(for: .milliseconds(150))
        try await supervisor.unsupervise(proxy)
        try await Task.sleep(for: .milliseconds(150))
        worker = nil
        #expect(try await expectNoTermination(supervisor, within: .milliseconds(400)))
    }

    @Test("Multiple watchers on same subject all receive termination; self-watch is ignored")
    func multipleWatchers() async throws {
        let system = try await ClusterSystem("multi-watch") { $0.bindPort = 19315 }
        defer { await system.shutdown() }
        let supervisorA = TestSupervisor(actorSystem: system)
        let supervisorB = TestSupervisor(actorSystem: system)
        try await supervisorA.watchSelf()
        var worker: EchoWorker? = EchoWorker(actorSystem: system)
        let workerID = try #require(worker?.id)
        try await supervisorA.supervise(try #require(worker))
        try await supervisorB.supervise(try #require(worker))
        worker = nil
        #expect(try await supervisorA.awaitNextTermination() == workerID)
        #expect(try await supervisorB.awaitNextTermination() == workerID)
    }

    @Test("Watcher deallocation cleans up its watches and system keeps working")
    func watcherDeathCleanup() async throws {
        let system = try await ClusterSystem("watcher-death") { $0.bindPort = 19316 }
        defer { await system.shutdown() }
        var ephemeralSupervisor: TestSupervisor? = TestSupervisor(actorSystem: system)
        var worker: EchoWorker? = EchoWorker(actorSystem: system)
        try await ephemeralSupervisor?.supervise(try #require(worker))
        ephemeralSupervisor = nil
        try await Task.sleep(for: .milliseconds(100))
        worker = nil
        try await Task.sleep(for: .milliseconds(100))

        let supervisor = TestSupervisor(actorSystem: system)
        var secondWorker: EchoWorker? = EchoWorker(actorSystem: system)
        let secondID = try #require(secondWorker?.id)
        try await supervisor.supervise(try #require(secondWorker))
        secondWorker = nil
        #expect(try await supervisor.awaitNextTermination() == secondID)
    }

    @Test("Watching remote actor that already died delivers termination (remote noproc)")
    func remoteWatchAlreadyDead() async throws {
        let nodeA = try await ClusterSystem("rnoproc-a") { $0.bindPort = 19317 }
        let nodeB = try await ClusterSystem("rnoproc-b") { $0.bindPort = 19318 }
        defer {
            await nodeA.shutdown()
            await nodeB.shutdown()
        }
        var worker: EchoWorker? = EchoWorker(actorSystem: nodeB)
        let deadID = try #require(worker?.id)
        worker = nil
        try await Task.sleep(for: .milliseconds(150))

        let supervisor = TestSupervisor(actorSystem: nodeA)
        let ghost = try EchoWorker.resolve(id: deadID, using: nodeA)
        try await supervisor.supervise(ghost)
        #expect(try await supervisor.awaitNextTermination() == deadID)
    }

    @Test("Multi-argument and collection remote calls round-trip")
    func multiArgRemoteCalls() async throws {
        let nodeA = try await ClusterSystem("multi-a") { $0.bindPort = 19319 }
        let nodeB = try await ClusterSystem("multi-b") { $0.bindPort = 19320 }
        defer {
            await nodeA.shutdown()
            await nodeB.shutdown()
        }
        let worker = EchoWorker(actorSystem: nodeB)
        let proxy = try EchoWorker.resolve(id: worker.id, using: nodeA)
        #expect(try await proxy.combine("otp", 42, true) == "otp|42|true")
        #expect(try await proxy.sum([1, 2, 3, 4]) == 10)
        #expect(try await proxy.sum([]) == 0)
    }

    @Test("Receptionist supports multiple keys and multiple workers per key")
    func receptionistMultipleKeys() async throws {
        let system = try await ClusterSystem("recept-multi") { $0.bindPort = 19321 }
        defer { await system.shutdown() }
        let keyA = Reception.Key("svc-a", as: EchoWorker.self)
        let keyB = Reception.Key("svc-b", as: EchoWorker.self)
        var first: EchoWorker? = EchoWorker(actorSystem: system)
        var second: EchoWorker? = EchoWorker(actorSystem: system)
        let firstID = try #require(first?.id)
        let secondID = try #require(second?.id)

        await system.receptionist.register(try #require(first), with: keyA)
        await system.receptionist.register(try #require(first), with: keyB)
        await system.receptionist.register(try #require(second), with: keyA)

        do {
            let a = await system.receptionist.lookup(keyA)
            #expect(Set(a.map(\.id)) == [firstID, secondID])
            let b = await system.receptionist.lookup(keyB)
            #expect(b.map(\.id) == [firstID])
        }

        first = nil
        try await Task.sleep(for: .milliseconds(100))
        let aAfter = await system.receptionist.lookup(keyA)
        #expect(aAfter.map(\.id) == [secondID])
        let bAfter = await system.receptionist.lookup(keyB)
        #expect(bAfter.isEmpty)
        second = nil
    }

    @Test("WireHeader rejects bad magic, unknown type, and oversized payload")
    func wireHeaderMalformedVariants() throws {
        let valid = WireHeader(type: .heartbeat, correlationID: 7, payloadLength: 3)
        let encoded = valid.encode()
        #expect(try WireHeader.decode(from: encoded.span.bytes) == valid)

        for frameType in [WireHeader.FrameType.remoteCallVoid, .reply, .error, .watchActor, .unwatchActor, .actorTerminated, .receptionistSync] {
            let header = WireHeader(type: frameType, correlationID: 99, payloadLength: 5)
            #expect(try WireHeader.decode(from: header.encode().span.bytes) == header)
        }

        var badMagic = valid.encode()
        badMagic[0] = 0xFF
        #expect(throws: ClusterError.self) { try WireHeader.decode(from: badMagic.span.bytes) }

        var badVersion = valid.encode()
        badVersion[2] = 99
        #expect(throws: ClusterError.self) { try WireHeader.decode(from: badVersion.span.bytes) }

        var badType = valid.encode()
        badType[3] = 0xFF
        #expect(throws: ClusterError.self) { try WireHeader.decode(from: badType.span.bytes) }

        let oversized = WireHeader(type: .reply, correlationID: 1, payloadLength: WireHeader.maxPayloadLength + 1)
        #expect(throws: ClusterError.self) { try WireHeader.decode(from: oversized.encode().span.bytes) }
    }

    @Test("Cluster models describe themselves and ClusterError round-trips through Codable")
    func modelsAndErrorCodable() throws {
        let endpoint = Cluster.Endpoint(host: "127.0.0.1", port: 9999)
        let node = Cluster.Node(name: "n1", endpoint: endpoint, nid: 42)
        #expect(node.description == "n1@127.0.0.1:9999#42")
        #expect(node.endpoint == endpoint)
        let actorID = ClusterActorID(node: node, path: "EchoWorker-1", incarnation: 3)
        #expect(actorID.description.contains("EchoWorker-1"))
        #expect(actorID.description.contains(":3"))

        let errors: [ClusterError] = [
            .bindFailed(host: "h", port: 1, reason: "r"),
            .nodeUnreachable(node),
            .remoteActorNotFound(actorID),
            .invocationTimeout(correlationID: 9, target: actorID),
            .encodingFailed("e"),
            .decodingFailed("d"),
            .malformedFrame("m"),
            .systemShuttingDown,
        ]
        for error in errors {
            let data = try JSONEncoder().encode(error)
            #expect(try JSONDecoder().decode(ClusterError.self, from: data) == error)
        }

        let idData = try JSONEncoder().encode(actorID)
        #expect(try JSONDecoder().decode(ClusterActorID.self, from: idData) == actorID)
        let nodeData = try JSONEncoder().encode(node)
        #expect(try JSONDecoder().decode(Cluster.Node.self, from: nodeData) == node)
    }

    @Test("Cross-node Receptionist lookup, seedNodes initial sync, and eviction on node down")
    func crossNodeReceptionistSyncAndEviction() async throws {
        let nodeA = try await ClusterSystem("rec-a") { $0.bindPort = 19322 }
        defer { await nodeA.shutdown() }

        let key = Reception.Key("remote-workers", as: EchoWorker.self)
        let workerOnA = EchoWorker(actorSystem: nodeA)
        await nodeA.receptionist.register(workerOnA, with: key)

        let nodeB = try await ClusterSystem("rec-b") {
            $0.bindPort = 19323
            $0.seedNodes = [Cluster.Endpoint(host: "127.0.0.1", port: 19322)]
        }
        defer { await nodeB.shutdown() }

        let streamB = nodeB.receptionist.listing(of: key)
        #expect(await listingContains(streamB, id: workerOnA.id))

        let resolved = await nodeB.receptionist.lookup(key)
        let remoteProxy = try #require(resolved.first(where: { $0.id == workerOnA.id }))
        #expect(try await remoteProxy.ping("via-receptionist") == "pong:via-receptionist")

        await nodeA.shutdown()
        #expect(await listingEventuallyEmpty(streamB))
        let afterDown = await nodeB.receptionist.lookup(key)
        #expect(afterDown.isEmpty)
    }

    @Test("Caller Task cancellation aborts in-flight remoteCall promptly")
    func callerTaskCancellationAbortsRemoteCall() async throws {
        let caller = try await ClusterSystem("cancel-a") {
            $0.bindPort = 19324
            $0.requestTimeout = .seconds(10)
        }
        let workerNode = try await ClusterSystem("cancel-b") { $0.bindPort = 19325 }
        defer {
            await caller.shutdown()
            await workerNode.shutdown()
        }

        let worker = EchoWorker(actorSystem: workerNode)
        let proxy = try EchoWorker.resolve(id: worker.id, using: caller)

        let start = ContinuousClock.now
        let task = Task {
            try await proxy.slowPing("cancel-me")
        }
        try await Task.sleep(for: .milliseconds(50))
        task.cancel()

        let result = await task.result
        let elapsed = ContinuousClock.now - start
        #expect(elapsed < .seconds(2))
        switch result {
        case .success(let value):
            Issue.record("Expected CancellationError, got \(value)")
        case .failure(let error):
            #expect(error is CancellationError)
        }
    }

    @Test("Bidirectional concurrent RPCs between two nodes succeed")
    func bidirectionalConcurrentRPC() async throws {
        let nodeA = try await ClusterSystem("bidi-a") { $0.bindPort = 19326 }
        let nodeB = try await ClusterSystem("bidi-b") { $0.bindPort = 19327 }
        defer {
            await nodeA.shutdown()
            await nodeB.shutdown()
        }

        let workerA = EchoWorker(actorSystem: nodeA)
        let workerB = EchoWorker(actorSystem: nodeB)
        let proxyBOnA = try EchoWorker.resolve(id: workerB.id, using: nodeA)
        let proxyAOnB = try EchoWorker.resolve(id: workerA.id, using: nodeB)

        async let replyFromB = proxyBOnA.ping("from-a")
        async let replyFromA = proxyAOnB.ping("from-b")

        #expect(try await replyFromB == "pong:from-a")
        #expect(try await replyFromA == "pong:from-b")
    }

    @Test("Generic distributed functions round-trip across nodes")
    func genericDistributedFuncRoundTrip() async throws {
        let nodeA = try await ClusterSystem("gen-a") { $0.bindPort = 19328 }
        let nodeB = try await ClusterSystem("gen-b") { $0.bindPort = 19329 }
        defer {
            await nodeA.shutdown()
            await nodeB.shutdown()
        }

        let worker = EchoWorker(actorSystem: nodeB)
        let proxy = try EchoWorker.resolve(id: worker.id, using: nodeA)

        #expect(try await proxy.echoGeneric("hello") == "hello")
        #expect(try await proxy.echoGeneric([1, 2, 3]) == [1, 2, 3])
        #expect(try await proxy.echoGeneric(WorkerProblem(message: "ok")) == WorkerProblem(message: "ok"))
    }

    @Test("Binary InvocationPayload, RemoteCallEnvelope, and BinaryCodec reject truncated inputs")
    func binaryCodecAndEnvelopeEdgeCases() throws {
        let node = Cluster.Node(name: "n1", endpoint: Cluster.Endpoint(host: "127.0.0.1", port: 2551), nid: 99)
        let actorID = ClusterActorID(node: node, path: "Worker-1", incarnation: 7)

        let payload = InvocationPayload(
            targetIdentifier: "ping(_:)",
            genericSubstitutions: [_mangledTypeName(String.self) ?? ""],
            arguments: [Data("arg1".utf8)],
            returnType: _mangledTypeName(Int.self) ?? "",
            errorType: _mangledTypeName(WorkerProblem.self) ?? ""
        )
        let encodedPayload = try payload.encodeBinary()
        let decodedPayload = try InvocationPayload.decodeBinary(from: encodedPayload)
        #expect(decodedPayload.targetIdentifier == payload.targetIdentifier)
        #expect(decodedPayload.genericSubstitutions == payload.genericSubstitutions)
        #expect(decodedPayload.arguments == payload.arguments)
        #expect(decodedPayload.returnType == payload.returnType)
        #expect(decodedPayload.errorType == payload.errorType)

        let envelope = RemoteCallEnvelope(targetID: actorID, invocationData: encodedPayload)
        let encodedEnvelope = try envelope.encodeBinary()
        let decodedEnvelope = try RemoteCallEnvelope.decodeBinary(from: encodedEnvelope)
        #expect(decodedEnvelope.targetID == actorID)
        #expect(decodedEnvelope.invocationData == encodedPayload)

        // Truncated InvocationPayload and RemoteCallEnvelope inputs must fail cleanly.
        for prefixLength in [0, 1, 5, encodedPayload.count - 1] {
            #expect(throws: ClusterError.self) {
                try InvocationPayload.decodeBinary(from: encodedPayload.prefix(prefixLength))
            }
        }
        for prefixLength in [0, 4, 16, encodedEnvelope.count - encodedPayload.count - 1] {
            #expect(throws: ClusterError.self) {
                try RemoteCallEnvelope.decodeBinary(from: encodedEnvelope.prefix(prefixLength))
            }
        }
    }

    @Test("ClusterInvocationDecoder handles generic, return, error types and failure paths")
    func invocationDecoderEdgeCases() throws {
        let validPayload = InvocationPayload(
            targetIdentifier: "op()",
            genericSubstitutions: [_mangledTypeName(Int.self) ?? ""],
            arguments: [try JSONEncoder().encode("hello")],
            returnType: _mangledTypeName(String.self) ?? "",
            errorType: _mangledTypeName(WorkerProblem.self) ?? ""
        )
        var decoder = ClusterInvocationDecoder(payload: validPayload)
        let subs = try decoder.decodeGenericSubstitutions()
        #expect(subs.count == 1)
        #expect(subs.first == Int.self)
        #expect(try decoder.decodeReturnType() == String.self)
        #expect(try decoder.decodeErrorType() == WorkerProblem.self)
        let arg: String = try decoder.decodeNextArgument()
        #expect(arg == "hello")

        // Exhausted arguments throws.
        #expect(throws: ClusterError.self) {
            let _: String = try decoder.decodeNextArgument()
        }

        // Unknown generic substitution throws.
        var badSubDecoder = ClusterInvocationDecoder(payload: InvocationPayload(
            targetIdentifier: "op()",
            genericSubstitutions: ["NotARealMangledTypeName"],
            arguments: [],
            returnType: "",
            errorType: ""
        ))
        #expect(throws: ClusterError.self) {
            try badSubDecoder.decodeGenericSubstitutions()
        }
        #expect(try badSubDecoder.decodeReturnType() == nil)
        #expect(try badSubDecoder.decodeErrorType() == nil)

        // Corrupt error frame data falls back to ClusterError.decodingFailed.
        let fallback = decodeThrownError(from: Data("not-json".utf8))
        guard let clusterErr = fallback as? ClusterError, case .decodingFailed = clusterErr else {
            Issue.record("Expected ClusterError.decodingFailed fallback, got \(fallback)")
            return
        }
    }

    #if SWIMDiscovery
    @Test("SWIMDiscovery tracks peer membership and removes dead nodes")
    func swimDiscoveryTracksMembershipAndRemoval() async throws {
        let nodeA = try await ClusterSystem("swim-a") {
            $0.bindPort = 19330
            $0.swim.probeInterval = .milliseconds(100)
        }
        defer { await nodeA.shutdown() }

        let nodeB = try await ClusterSystem("swim-b") {
            $0.bindPort = 19331
            $0.seedNodes = [Cluster.Endpoint(host: "127.0.0.1", port: 19330)]
            $0.swim.probeInterval = .milliseconds(100)
        }
        defer { await nodeB.shutdown() }

        var discovered = false
        for _ in 0..<30 {
            if nodeA.swimMembers.contains(nodeB.node) && nodeB.swimMembers.contains(nodeA.node) {
                discovered = true
                break
            }
            try await Task.sleep(for: .milliseconds(50))
        }
        #expect(discovered)

        await nodeB.shutdown()
        var evicted = false
        for _ in 0..<30 {
            if !nodeA.swimMembers.contains(nodeB.node) {
                evicted = true
                break
            }
            try await Task.sleep(for: .milliseconds(50))
        }
        #expect(evicted)
    }
    #endif

    #if MutualTLS
    @Test("MutualTLS encrypts inter-node RPCs and rejects untrusted peers")
    func mutualTLSRoundTripAndRejection() async throws {
        let tempDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("dc-mtls-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: tempDir) }

        let certPath = tempDir.appendingPathComponent("node.crt").path
        let keyPath = tempDir.appendingPathComponent("node.key").path
        let rogueCertPath = tempDir.appendingPathComponent("rogue.crt").path
        let rogueKeyPath = tempDir.appendingPathComponent("rogue.key").path

        try generateSelfSignedCert(certPath: certPath, keyPath: keyPath, san: "IP:127.0.0.1")
        try generateSelfSignedCert(certPath: rogueCertPath, keyPath: rogueKeyPath, san: "IP:127.0.0.1")

        let trustedCerts = try NIOSSLCertificate.fromPEMFile(certPath)
        let trustedCert = try #require(trustedCerts.first)
        let trustedKey = try NIOSSLPrivateKey(file: keyPath, format: .pem)
        let rogueCerts = try NIOSSLCertificate.fromPEMFile(rogueCertPath)
        let rogueCert = try #require(rogueCerts.first)
        let rogueKey = try NIOSSLPrivateKey(file: rogueKeyPath, format: .pem)

        let trustedTLS = ClusterTLSConfiguration.mutualTLS(
            certificateChain: [.certificate(trustedCert)],
            privateKey: .privateKey(trustedKey),
            trustRoots: .certificates([trustedCert])
        )

        let nodeA = try await ClusterSystem("mtls-a") {
            $0.bindPort = 19332
            $0.tls = trustedTLS
        }
        let nodeB = try await ClusterSystem("mtls-b") {
            $0.bindPort = 19333
            $0.tls = trustedTLS
        }
        defer {
            await nodeA.shutdown()
            await nodeB.shutdown()
        }

        let worker = EchoWorker(actorSystem: nodeB)
        let proxy = try EchoWorker.resolve(id: worker.id, using: nodeA)
        #expect(try await proxy.ping("encrypted") == "pong:encrypted")

        // A rogue node with an untrusted certificate must fail to connect.
        let rogueNode = try await ClusterSystem("mtls-rogue") {
            $0.bindPort = 19334
            $0.requestTimeout = .seconds(1)
            $0.tls = ClusterTLSConfiguration.mutualTLS(
                certificateChain: [.certificate(rogueCert)],
                privateKey: .privateKey(rogueKey),
                trustRoots: .certificates([trustedCert])
            )
        }
        defer { await rogueNode.shutdown() }

        let rogueProxy = try EchoWorker.resolve(id: worker.id, using: rogueNode)
        do {
            _ = try await rogueProxy.ping("should-fail")
            Issue.record("Expected mTLS handshake failure for untrusted client certificate")
        } catch {
            // Expected: handshake failure closes channel and fails call.
        }
    }
    #endif
}

#if MutualTLS
private func generateSelfSignedCert(certPath: String, keyPath: String, san: String) throws {
    let process = Process()
    process.executableURL = URL(fileURLWithPath: "/usr/bin/openssl")
    process.arguments = [
        "req", "-x509", "-newkey", "rsa:2048", "-nodes",
        "-keyout", keyPath,
        "-out", certPath,
        "-days", "1",
        "-subj", "/CN=127.0.0.1",
        "-addext", "subjectAltName=\(san)"
    ]
    process.standardOutput = Pipe()
    process.standardError = Pipe()
    try process.run()
    process.waitUntilExit()
    if process.terminationStatus != 0 {
        throw ClusterError.bindFailed(host: "127.0.0.1", port: 0, reason: "openssl exited with \(process.terminationStatus)")
    }
}
#endif

func expectNoTermination(_ supervisor: TestSupervisor, within duration: Duration) async throws -> Bool {
    try await Task.sleep(for: duration)
    return try await supervisor.pendingTerminations().isEmpty
}

func listingEventuallyEmpty(_ stream: AsyncStream<Set<EchoWorker>>) async -> Bool {
    await withTaskGroup(of: Bool.self) { group in
        group.addTask {
            var iterator = stream.makeAsyncIterator()
            for _ in 0..<20 {
                guard let set = await iterator.next() else { return false }
                if set.isEmpty {
                    return true
                }
            }
            return false
        }
        group.addTask {
            try? await Task.sleep(for: .seconds(5))
            return false
        }
        let result = await group.next() ?? false
        group.cancelAll()
        return result
    }
}

func listingContains(_ stream: AsyncStream<Set<EchoWorker>>, id: ClusterActorID) async -> Bool {
    await withTaskGroup(of: Bool.self) { group in
        group.addTask {
            var iterator = stream.makeAsyncIterator()
            for _ in 0..<10 {
                guard let set = await iterator.next() else { return false }
                if set.map(\.id).contains(id) {
                    return true
                }
            }
            return false
        }
        group.addTask {
            try? await Task.sleep(for: .seconds(5))
            return false
        }
        let result = await group.next() ?? false
        group.cancelAll()
        return result
    }
}
