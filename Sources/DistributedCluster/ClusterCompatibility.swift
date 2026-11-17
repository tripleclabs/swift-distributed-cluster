@_exported import Distributed
import Foundation
import Logging

public typealias ClusterSystemSettings = ClusterSystem.Settings

extension ClusterSystem.Settings {
    public struct LoggingSettings: Sendable {
        public var baseLogger: Logger
        public var logLevel: Logger.Level {
            get { baseLogger.logLevel }
            set { baseLogger.logLevel = newValue }
        }

        public init(baseLogger: Logger) {
            self.baseLogger = baseLogger
        }
    }

    public struct PluginsSettings: Sendable {
        public mutating func install(plugin: any ClusterPlugin) {}
    }
}

public protocol ClusterPlugin: Sendable {}

public protocol ClusterSingleton: DistributedActor where ActorSystem == ClusterSystem {
    func activateSingleton() async
    func passivateSingleton() async
}

public struct ClusterSingletonPlugin: ClusterPlugin, Sendable {
    public init() {}
}

public struct ClusterSingletonControl: Sendable {
    private let system: ClusterSystem

    internal init(system: ClusterSystem) {
        self.system = system
    }

    @discardableResult
    public func host<S: ClusterSingleton>(
        name: String,
        makeInstance: @Sendable (ClusterSystem) async throws -> S
    ) async throws -> S {
        let key = Reception.Key<S>(S.self, id: "singleton:\(name)")
        if let existing = await system.receptionist.lookup(key).first {
            return existing
        }
        let instance = try await makeInstance(system)
        await system.receptionist.register(instance, with: key)
        await instance.whenLocal { local in
            await local.activateSingleton()
        }
        system.registerHostedSingleton {
            await instance.whenLocal { local in
                await local.passivateSingleton()
            }
        }
        return instance
    }
}

extension Cluster.Endpoint {
    public init(systemName: String, host: String, port: Int) {
        self.init(host: host, port: port)
    }
}

extension Cluster {
    public enum AutoLeaderElectionSettings: Sendable, Hashable {
        case none
        case lowestReachable(minNumberOfMembers: Int)
    }

    public enum MemberStatus: Int, Sendable, Hashable, Comparable {
        case joining = 0
        case up = 1
        case leaving = 2
        case down = 3
        case removed = 4

        public static func < (lhs: MemberStatus, rhs: MemberStatus) -> Bool {
            lhs.rawValue < rhs.rawValue
        }
    }

    public struct Member: Sendable, Hashable {
        public let node: Cluster.Node
        public let status: MemberStatus

        public init(node: Cluster.Node, status: MemberStatus) {
            self.node = node
            self.status = status
        }
    }

    public struct MembershipSnapshot: Sendable, Hashable {
        public let members: [Member]

        public var count: Int { members.count }
        public var isEmpty: Bool { members.isEmpty }

        public func members(atLeast status: MemberStatus) -> [Member] {
            members.filter { $0.status >= status }
        }

        public func members(atMost status: MemberStatus) -> [Member] {
            members.filter { $0.status <= status }
        }

        public func member(_ node: Cluster.Node) -> Member? {
            members.first { $0.node == node }
        }

        public init(members: [Member]) {
            self.members = members
        }
    }

    public struct MembershipChange: Sendable, Hashable {
        public let node: Cluster.Node
        public let previousStatus: MemberStatus?
        public let status: MemberStatus

        public init(node: Cluster.Node, previousStatus: MemberStatus?, status: MemberStatus) {
            self.node = node
            self.previousStatus = previousStatus
            self.status = status
        }
    }

    public enum Event: Sendable, Hashable {
        case membershipChange(MembershipChange)
    }
}

public struct ClusterControl: Sendable {
    private let system: ClusterSystem

    internal init(system: ClusterSystem) {
        self.system = system
    }

    public var node: Cluster.Node {
        system.node
    }

    public func join(host: String, port: Int) {
        system.transport.connectToSeedNodes([Cluster.Endpoint(host: host, port: port)])
    }

    public func join(endpoint: Cluster.Endpoint) {
        system.transport.connectToSeedNodes([endpoint])
    }

    public func join(node: Cluster.Node) {
        system.transport.connectToSeedNodes([node.endpoint])
    }

    @discardableResult
    public func joined(node: Cluster.Node, within timeout: Duration) async throws -> Cluster.Member {
        let deadline = ContinuousClock.now + timeout
        while ContinuousClock.now < deadline {
            let snapshot = await membershipSnapshot
            if let member = snapshot.member(node) {
                return member
            }
            try await Swift::Task.sleep(for: .milliseconds(25))
        }
        throw ClusterError.nodeUnreachable(node)
    }

    public var membershipSnapshot: Cluster.MembershipSnapshot {
        get async {
            var seen = Set<Cluster.Node>([system.node])
            var members: [Cluster.Member] = [
                Cluster.Member(node: system.node, status: .up)
            ]
            for peer in system.transport.knownPeers() where seen.insert(peer).inserted {
                members.append(Cluster.Member(node: peer, status: .up))
            }
            #if SWIMDiscovery
            for peer in system.swimMembers where seen.insert(peer).inserted {
                members.append(Cluster.Member(node: peer, status: .up))
            }
            #endif
            return Cluster.MembershipSnapshot(members: members)
        }
    }

    public var events: AsyncStream<Cluster.Event> {
        system.makeClusterEventStream()
    }
}

public enum DistributedReception {
    public typealias Key<Act: DistributedActor> = Reception.Key<Act> where Act.ID == ClusterActorID
}

extension Reception.Key {
    public init(_ actorType: Act.Type, id: String) {
        self.init(id, as: actorType)
    }
}

extension ClusterReceptionist {
    public func checkIn<Act: DistributedActor>(
        _ actor: Act,
        with key: Reception.Key<Act>
    ) async where Act.ID == ClusterActorID {
        await register(actor, with: key)
    }
}

public enum RemoteCall {
    @TaskLocal internal static var currentTimeout: Duration?

    public static func with<Result>(
        timeout: Duration,
        remoteCall: () async throws -> Result
    ) async throws -> Result {
        try await $currentTimeout.withValue(timeout) {
            try await remoteCall()
        }
    }
}
