@_exported import Distributed
import Foundation
import Logging

public typealias ClusterSystemSettings = ClusterSystem.Settings

extension ClusterSystem.Settings {
    public var logging: Logger {
        get { logger }
        set { logger = newValue }
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
        case down = 2
        case removed = 3

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
