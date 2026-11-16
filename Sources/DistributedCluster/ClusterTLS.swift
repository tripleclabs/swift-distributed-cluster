#if MutualTLS
import NIOSSL

public struct ClusterTLSConfiguration: Sendable {
    public var server: TLSConfiguration
    public var client: TLSConfiguration
    public var serverHostname: String?

    public init(
        server: TLSConfiguration,
        client: TLSConfiguration,
        serverHostname: String? = nil
    ) {
        self.server = server
        self.client = client
        self.serverHostname = serverHostname
    }

    public static func mutualTLS(
        certificateChain: [NIOSSLCertificateSource],
        privateKey: NIOSSLPrivateKeySource,
        trustRoots: NIOSSLTrustRoots,
        certificateVerification: CertificateVerification = .fullVerification,
        serverHostname: String? = nil
    ) -> ClusterTLSConfiguration {
        var server = TLSConfiguration.makeServerConfiguration(
            certificateChain: certificateChain,
            privateKey: privateKey
        )
        server.trustRoots = trustRoots
        server.certificateVerification = certificateVerification

        var client = TLSConfiguration.makeClientConfiguration()
        client.certificateChain = certificateChain
        client.privateKey = privateKey
        client.trustRoots = trustRoots
        client.certificateVerification = certificateVerification

        return ClusterTLSConfiguration(
            server: server,
            client: client,
            serverHostname: serverHostname
        )
    }
}
#endif
