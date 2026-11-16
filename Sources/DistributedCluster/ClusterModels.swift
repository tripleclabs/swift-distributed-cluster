import Distributed

public enum Cluster {
    public struct Endpoint: Hashable, Codable, Sendable {
        public let host: String
        public let port: Int

        public init(host: String, port: Int) {
            self.host = host
            self.port = port
        }
    }

    public struct Node: Hashable, Codable, Sendable, CustomStringConvertible {
        public let name: String
        public let endpoint: Endpoint
        public let nid: UInt64

        public init(name: String, endpoint: Endpoint, nid: UInt64 = .random(in: 1...UInt64.max)) {
            self.name = name
            self.endpoint = endpoint
            self.nid = nid
        }

        public var description: String {
            "\(name)@\(endpoint.host):\(endpoint.port)#\(nid)"
        }
    }
}

public struct ClusterActorID: Hashable, Codable, Sendable, CustomStringConvertible {
    public let node: Cluster.Node
    public let path: String
    public let incarnation: UInt64

    public init(node: Cluster.Node, path: String, incarnation: UInt64) {
        self.node = node
        self.path = path
        self.incarnation = incarnation
    }

    public var description: String {
        "\(node)/\(path):\(incarnation)"
    }
}
