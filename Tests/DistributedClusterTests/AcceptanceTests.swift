import Distributed
import DistributedCluster
import Testing

distributed actor EchoWorker {
    typealias ActorSystem = ClusterSystem

    distributed func ping(_ message: String) -> String {
        "pong:\(message)"
    }

    distributed func notifyOnly(_ value: Int) {
        _ = value
    }

    distributed func slowPing(_ message: String) async -> String {
        try? await Task.sleep(for: .seconds(3))
        return "slow:\(message)"
    }
}

distributed actor TestSupervisor: LifecycleWatch {
    typealias ActorSystem = ClusterSystem

    private var terminatedIDs: [ClusterActorID] = []
    private var waiters: [CheckedContinuation<ClusterActorID, Never>] = []

    distributed func supervise(_ worker: EchoWorker) {
        watchTermination(of: worker)
    }

    func terminated(actor id: ClusterActorID) async {
        if let waiter = waiters.first {
            waiters.removeFirst()
            waiter.resume(returning: id)
        } else {
            terminatedIDs.append(id)
        }
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
        let encoded: InlineArray<15, UInt8> = original.encode()

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
