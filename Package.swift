// swift-tools-version: 6.4
import PackageDescription

let package = Package(
    name: "swift-distributed-cluster",
    platforms: [
        .macOS(.v27),
        .iOS(.v27)
    ],
    products: [
        .library(
            name: "DistributedCluster",
            targets: ["DistributedCluster"]
        )
    ],
    traits: [
        .trait(
            name: "SWIMDiscovery",
            description: "Enables peer-to-peer SWIM gossip membership via swift-cluster-membership."
        ),
        .default(enabledTraits: [])
    ],
    dependencies: [
        .package(url: "https://github.com/apple/swift-nio.git", from: "2.77.0"),
        .package(url: "https://github.com/apple/swift-log.git", from: "1.6.0"),
        .package(url: "https://github.com/swift-server/swift-service-lifecycle.git", from: "2.6.0"),
        .package(url: "https://github.com/apple/swift-cluster-membership.git", from: "0.5.0")
    ],
    targets: [
        .target(
            name: "DistributedCluster",
            dependencies: [
                .product(name: "NIOCore", package: "swift-nio"),
                .product(name: "NIOPosix", package: "swift-nio"),
                .product(name: "Logging", package: "swift-log"),
                .product(name: "ServiceLifecycle", package: "swift-service-lifecycle"),
                .product(
                    name: "SWIM",
                    package: "swift-cluster-membership",
                    condition: .when(traits: ["SWIMDiscovery"])
                )
            ],
            swiftSettings: [
                .swiftLanguageMode(.v6),
                .enableUpcomingFeature("NonisolatedNonsendingByDefault")
            ]
        ),
        .testTarget(
            name: "DistributedClusterTests",
            dependencies: ["DistributedCluster"],
            swiftSettings: [
                .swiftLanguageMode(.v6)
            ]
        )
    ]
)
