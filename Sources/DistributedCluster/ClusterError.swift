public enum ClusterError: Error, Sendable, Codable, Hashable {
    case bindFailed(host: String, port: Int, reason: String)
    case nodeUnreachable(Cluster.Node)
    case remoteActorNotFound(ClusterActorID)
    case invocationTimeout(correlationID: UInt64, target: ClusterActorID)
    case encodingFailed(String)
    case decodingFailed(String)
    case malformedFrame(String)
    case systemShuttingDown
}
