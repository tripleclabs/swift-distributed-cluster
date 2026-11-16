# Technical Specification: `DistributedCluster` (Swift 6.4 Drop-In Runtime)

| Metadata | Value |
| :--- | :--- |
| **Document ID** | `SPEC-DC-6.4` |
| **Status** | Proposed / Ready for Implementation |
| **Target Toolchain** | Swift 6.4 (`swift-tools-version: 6.4`, Swift 6 Language Mode) |
| **Build System** | SwiftPM 6.4 (`Swift Build` default engine) |
| **Compatibility Target** | Source-compatible drop-in for `DistributedCluster` APIs consumed by `swift-OTP` |
| **Clean Build Budget** | $\le 5.0\text{ s}$ (down from $\sim 90\text{--}180\text{ s}$ with legacy `apple/swift-distributed-actors`) |

---

## 1. Executive Summary & Goals

The upstream `apple/swift-distributed-actors` package remains stalled at `1.0.0-beta.3` and carries pre-concurrency Akka-inspired internals (`ActorShell`, custom mailboxes, `EventLoopFuture` chains, and `swift-protobuf` descriptors) that severely degrade compile times and complicate strict Swift 6 concurrency compliance.

This specification defines a clean-room, production-grade replacement module named **`DistributedCluster`** built exclusively on Swift's first-party `Distributed` module and **Swift 6.4** concurrency and memory-safety primitives.

### Primary Objectives
1. **Zero-Rewrite Migration for `swift-OTP`:** Preserve the public module name (`DistributedCluster`), `ClusterSystem`, `Cluster.Node`, `ClusterActorID`, `LifecycleWatch`, and `Receptionist` contracts so `swift-OTP` supervisors, monitors, links, and GenServers compile without structural changes.
2. **Native Cooperative Runtime:** Delegate all actor scheduling, reentrancy, and mailbox execution directly to Swift's native `distributed actor` runtime.
3. **Strict Swift 6.4 API Enforcement:** Eliminate all legacy locks, `Unsafe*` pointer APIs, and unstructured cleanup hacks in favor of `Synchronization.Mutex`, `RawSpan`, `InlineArray`, `UniqueBox`, `weak let`, `await` in `defer`, and `withTaskCancellationShield`.
4. **Minimal Dependency Graph:** Reduce external dependencies from 12+ packages to 3 core packages (`swift-nio`, `swift-log`, `swift-service-lifecycle`), with `swift-cluster-membership` (SWIM) isolated behind an optional package trait.

---

## 2. Package Manifest & Swift 6.4 Compiler Standards

### 2.1 `Package.swift` Contract

```swift
// swift-tools-version: 6.4
import PackageDescription

let package = Package(
    name: "swift-distributed-cluster",
    platforms: [
        .macOS(.v15),
        .iOS(.v18),
        .linux
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
```

### 2.2 Mandatory Swift 6.4 Language & Stdlib Enforcement

All pull requests targeting `DistributedCluster` **must** adhere to the following API rules:

| Capability | Enforced Swift 6.4 API | Prohibited Legacy Pattern |
| :--- | :--- | :--- |
| **Synchronous Locking** | `Synchronization.Mutex<State>` / `Atomic<UInt64>` | `NSLock`, `DispatchQueue`, `NIOConcurrencyHelpers.NIOLock` |
| **Teardown & Cleanup** | `defer { await withTaskCancellationShield { ... } }` (SE-0493, SE-0504) | Unstructured `Task.detached { ... }` in `defer` or `deinit` |
| **Weak Actor References** | `weak let` inside `Sendable` registry structs (Swift 6.3+) | `nonisolated(unsafe) weak var` wrappers |
| **Optional Existentials** | `any LifecycleWatch?`, `some ClusterTransport?` (SE-0521) | Parenthesized `(any LifecycleWatch)?` |
| **Binary Buffer Access** | `RawSpan`, `Span`, `InlineArray<N, UInt8>`, `withTemporaryAllocation` (SE-0525, SE-0524) | `UnsafeRawBufferPointer`, `withUnsafeBytes` |
| **Move-Only State** | `UniqueBox<T>`, `UniqueArray<T>`, `~Copyable` (SE-0517, SE-0527) | Reference-counted class boxes for internal continuations |
| **Error Boundaries** | Typed throws `throws(ClusterError)` on internal/transport APIs | Untyped `throws` across internal codec/transport layers |
| **Module Disambiguation** | Module selectors `Distributed::DistributedActorSystem`, `Swift::Task` (SE-0491) | Typealias workarounds for module name collisions |
| **Test Suite** | `import Testing` (`@Suite`, `@Test`, `#expect`, `#require`) | Legacy `XCTestCase` subclasses |

---

## 3. System Architecture

```text
+-----------------------------------------------------------------------+
|                         swift-OTP Layer                               |
|   Supervisors  |  GenServer  |  Process.monitor / link  |  Registry   |
+-----------------------------------------------------------------------+
                                   |
                                   v
+-----------------------------------------------------------------------+
|                  DistributedCluster (Swift 6.4)                       |
|                                                                       |
|  +------------------------+    +-----------------------------------+  |
|  |     ClusterSystem      |    |          LifecycleWatch           |  |
|  | (DistributedActorSystem)|<-->|  - Local resignID hook            |  |
|  |  - Mutex<ActorTable>   |    |  - Remote ActorTerminated frame   |  |
|  |  - Weak actor refs     |    |  - Node-down fanout               |  |
|  +------------------------+    +-----------------------------------+  |
|              |                                   |                    |
|  +------------------------+    +-----------------------------------+  |
|  |      Receptionist      |    |         WireFrameCodec            |  |
|  |  - Keyed AsyncStreams  |    |  - InlineArray<13, UInt8> header  |  |
|  |  - Delta sync frames   |    |  - Safe RawSpan load/store        |  |
|  +------------------------+    +-----------------------------------+  |
|                                                  |                    |
|                                +-----------------------------------+  |
|                                |      NIOAsyncChannel Transport    |  |
|                                |  - DiscardingTaskGroup per peer   |  |
|                                |  - Cancellation-shielded teardown |  |
|                                +-----------------------------------+  |
+-----------------------------------------------------------------------+
                                   |
                                   v
+-----------------------------------------------------------------------+
|         Swift 6.4 Standard Library `Distributed` & Executor           |
+-----------------------------------------------------------------------+
```

### Architectural Removals
* **No Custom Mailboxes:** Swift's default actor executor manages message queuing and serial execution for every `distributed actor`.
* **No Type-Erased `ActorShell`:** Local actors are stored via concurrency-safe `weak let` references inside `Mutex`-protected registry entries.
* **No `EventLoopFuture` Plumbing:** All network I/O uses `NIOAsyncChannel` structured async sequences inside `withThrowingDiscardingTaskGroup`.

---

## 4. Public API Contracts (Drop-In Surface)

### 4.1 Node, Endpoint, and Actor Identity

To support Erlang/OTP supervisor restart strategies (`one_for_one`, `one_for_all`, `rest_for_one`), `ClusterActorID` includes a strictly monotonic `incarnation` counter. If a supervised worker terminates and is restarted under the same logical name, its new `ClusterActorID` is guaranteed distinct from the dead instance.

```swift
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
```

### 4.2 `ClusterSystem` (`DistributedActorSystem`)

`ClusterSystem` implements `Distributed::DistributedActorSystem` and conforms to `ServiceLifecycle.Service` for clean integration with server boot/shutdown lifecycles.

```swift
import Distributed
import Logging
import ServiceLifecycle
import Synchronization

public final class ClusterSystem: Distributed::DistributedActorSystem, Service, Sendable {
    public typealias ActorID = ClusterActorID
    public typealias InvocationEncoder = ClusterInvocationEncoder
    public typealias InvocationDecoder = ClusterInvocationDecoder
    public typealias ResultHandler = ClusterResultHandler
    public typealias SerializationRequirement = any Codable

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

    /// Concurrency-safe weak reference wrapper (Swift 6.3+ `weak let`).
    private struct WeakActorBox: Sendable {
        weak let actor: any DistributedActor?
    }

    private struct WeakWatcherBox: Sendable {
        weak let watcher: any LifecycleWatch?
    }

    private struct State: Sendable {
        var localActors: [ActorID: WeakActorBox] = [:]
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

    private let state = Mutex(State())
    private let transport: ClusterTransport
    private let terminatedContinuation: AsyncStream<Void>.Continuation
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

        try await self.transport.start(
            onFrame: { [weak self] frame, peer in
                await self?.handleInboundFrame(frame, from: peer)
            },
            onNodeDown: { [weak self] deadNode in
                await self?.handleNodeDown(deadNode)
            }
        )
    }

    // MARK: - ServiceLifecycle Conformance

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

    // MARK: - DistributedActorSystem Identity & Registry

    public func assignID<Act>(_ actorType: Act.Type) -> ActorID where Act: DistributedActor {
        state.withLock { s in
            let inc = s.nextIncarnation
            s.nextIncarnation &+= 1
            let path = "\(String(describing: actorType))-\(inc)"
            return ClusterActorID(node: node, path: path, incarnation: inc)
        }
    }

    public func actorReady<Act>(_ actor: Act) where Act: DistributedActor, Act.ID == ActorID {
        state.withLock { s in
            s.localActors[actor.id] = WeakActorBox(actor: actor)
        }
    }

    public func resignID(_ id: ActorID) {
        let (localWatchers, remoteWatcherNodes) = state.withLock { s -> ([any LifecycleWatch], Set<Cluster.Node>) in
            s.localActors.removeValue(forKey: id)

            // Clean up any watches this dying actor held on other subjects
            if let watchedSubjects = s.subjectsByWatcher.removeValue(forKey: id) {
                for subject in watchedSubjects {
                    s.watchersBySubject[subject]?.removeValue(forKey: id)
                }
            }

            // Collect all watchers monitoring this dying actor
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
            return (resolvedLocals, remoteNodes)
        }

        // Notify local OTP supervisors & remote nodes without risking cancellation loss
        if !localWatchers.isEmpty || !remoteWatcherNodes.isEmpty {
            Swift::Task {
                await withTaskCancellationShield {
                    await self.receptionist.removeActor(id)
                    for watcher in localWatchers {
                        await watcher.terminated(actor: id)
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
            // Returning nil instructs the Swift runtime to synthesize a remote proxy
            return nil
        }
        return state.withLock { s in
            s.localActors[id]?.actor as? Act
        }
    }
}
```

### 4.3 `LifecycleWatch` (OTP Supervision & `Process.monitor/1`)

`LifecycleWatch` preserves the exact method signatures expected by `swift-OTP` supervisors and process monitors.

```swift
import Distributed

public protocol LifecycleWatch: DistributedActor where ActorSystem == ClusterSystem {
    func terminated(actor id: ClusterSystem.ActorID) async
}

extension LifecycleWatch {
    /// Registers `self` (the watcher/supervisor) to receive `terminated(actor:)`
    /// when `subject` deinitializes, resigns its ID, or its host node disconnects.
    public func watchTermination<Act: DistributedActor>(
        of subject: Act,
        file: String = #fileID,
        line: UInt = #line
    ) where Act.ID == ClusterSystem.ActorID {
        self.actorSystem.registerWatch(watcher: self, subjectID: subject.id)
    }

    /// Cancels an active termination watch on `subject`.
    public func unwatchTermination<Act: DistributedActor>(
        of subject: Act
    ) where Act.ID == ClusterSystem.ActorID {
        self.actorSystem.unregisterWatch(watcherID: self.id, subjectID: subject.id)
    }
}
```

#### Watch Registration Semantics (`ClusterSystem.registerWatch`)
1. **Self-Watch Guard:** If `watcher.id == subjectID`, ignore immediately (prevents self-deadlock).
2. **Local Subject Already Dead:** Inside `state.withLock`, if `subjectID.node == self.node` and `s.localActors[subjectID]?.actor == nil`, the subject has already exited (`:noproc` in Erlang terms). Immediately schedule `await watcher.terminated(actor: subjectID)` in a cancellation-shielded task.
3. **Remote Subject Registration:** If `subjectID.node != self.node`, record `(watcher.id -> subjectID)` in `remoteSubjectsByNode[subjectID.node]` and dispatch a `.watchActor(watcherID: watcher.id, subjectID: subjectID)` control frame to `subjectID.node`. If `subjectID.node` is already known unreachable or rejects the watch with `:noproc`, immediately fire `await watcher.terminated(actor: subjectID)`.

### 4.4 `Receptionist` API Contract

```swift
import Distributed
import Synchronization

public enum Reception {
    public struct Key<Act: DistributedActor>: Hashable, Sendable
    where Act.ID == ClusterActorID {
        public let id: String

        public init(_ id: String, as actorType: Act.Type = Act.self) {
            self.id = id
        }
    }
}

public final class ClusterReceptionist: Sendable {
    // Provides drop-in registration and AsyncSequence streaming for OTP named registries
    public func register<Act: DistributedActor>(
        _ actor: Act,
        with key: Reception.Key<Act>
    ) async where Act.ID == ClusterActorID

    public func lookup<Act: DistributedActor>(
        _ key: Reception.Key<Act>,
        as actorType: Act.Type = Act.self
    ) async -> Set<Act> where Act.ID == ClusterActorID

    public func listing<Act: DistributedActor>(
        of key: Reception.Key<Act>,
        as actorType: Act.Type = Act.self
    ) -> AsyncStream<Set<Act>> where Act.ID == ClusterActorID
}
```

### 4.5 Typed Error Hierarchy (`ClusterError`)

```swift
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
```

---

## 5. Wire Protocol & Zero-Unsafe Binary Framing

To eliminate `swift-protobuf` code generation and `UnsafeRawBufferPointer` boilerplate, `DistributedCluster` uses a fixed **13-byte binary header** backed by Swift 6.4's `InlineArray<13, UInt8>` and safe `RawSpan` loading APIs (SE-0525).

### 5.1 Frame Layout

```text
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|         Magic (0xDC64)        |  FrameType    |               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+               +
|                     CorrelationID (64-bit)                    |
|                                               +-+-+-+-+-+-+-+-+
|                                               |  PayloadLen   |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|   PayloadLen (cont., 32-bit)  |      Payload Bytes ...        |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

* **`Magic` (`UInt16`, Big-Endian):** `0xDC64` (identifies `DistributedCluster` Swift 6.4 protocol).
* **`FrameType` (`UInt8`):**
  * `0x01`: `remoteCall` (expects reply)
  * `0x02`: `remoteCallVoid` (expects ack or error)
  * `0x03`: `reply` (serialized return value)
  * `0x04`: `error` (serialized `ClusterError` or remote thrown error)
  * `0x05`: `watchActor` (`watcherID`, `subjectID`)
  * `0x06`: `unwatchActor` (`watcherID`, `subjectID`)
  * `0x07`: `actorTerminated` (`subjectID`)
  * `0x08`: `receptionistSync` (key/ID delta)
  * `0x09`: `heartbeat` (liveness ping/pong)
* **`CorrelationID` (`UInt64`, Big-Endian):** Monotonic `Atomic<UInt64>` request identifier.
* **`PayloadLength` (`UInt32`, Big-Endian):** Byte length of payload (`maxFrameSize = 16 MiB` default).

### 5.2 Safe `RawSpan` Header Encoder / Decoder (Zero `Unsafe*` APIs)

```swift
public struct WireHeader: Sendable, Equatable {
    public static let magic: UInt16 = 0xDC64
    public static let byteCount = 13

    public enum FrameType: UInt8, Sendable {
        case remoteCall = 0x01
        case remoteCallVoid = 0x02
        case reply = 0x03
        case error = 0x04
        case watchActor = 0x05
        case unwatchActor = 0x06
        case actorTerminated = 0x07
        case receptionistSync = 0x08
        case heartbeat = 0x09
    }

    public let type: FrameType
    public let correlationID: UInt64
    public let payloadLength: UInt32

    public func encode() -> InlineArray<13, UInt8> {
        var bytes = InlineArray<13, UInt8>(repeating: 0)
        bytes[0] = UInt8(truncatingIfNeeded: Self.magic >> 8)
        bytes[1] = UInt8(truncatingIfNeeded: Self.magic)
        bytes[2] = type.rawValue

        for i in 0..<8 {
            let shift = (7 - i) * 8
            bytes[3 + i] = UInt8(truncatingIfNeeded: correlationID >> shift)
        }

        for i in 0..<4 {
            let shift = (3 - i) * 8
            bytes[11 + i] = UInt8(truncatingIfNeeded: payloadLength >> shift)
        }
        return bytes
    }

    public static func decode(from span: RawSpan) throws(ClusterError) -> WireHeader {
        guard span.byteCount >= Self.byteCount else {
            throw .malformedFrame("Header truncated: expected \(Self.byteCount) bytes, got \(span.byteCount)")
        }

        // Safe bounds-checked loads via Swift 6.4 RawSpan (SE-0525)
        let rawMagic = UInt16(bigEndian: span.load(fromByteOffset: 0, as: UInt16.self))
        guard rawMagic == Self.magic else {
            throw .malformedFrame("Invalid magic bytes: 0x\(String(rawMagic, radix: 16))")
        }

        let rawType = span.load(fromByteOffset: 2, as: UInt8.self)
        guard let frameType = FrameType(rawValue: rawType) else {
            throw .malformedFrame("Unknown frame type: 0x\(String(rawType, radix: 16))")
        }

        let correlationID = UInt64(bigEndian: span.load(fromByteOffset: 3, as: UInt64.self))
        let payloadLength = UInt32(bigEndian: span.load(fromByteOffset: 11, as: UInt32.self))

        return WireHeader(
            type: frameType,
            correlationID: correlationID,
            payloadLength: payloadLength
        )
    }
}
```

---

## 6. Transport & Concurrency Lifecycle (Swift 6.4)

### 6.1 Connection Supervision with `defer { await ... }` & Cancellation Shields

Every inbound and outbound peer connection runs inside a structured child task of a `DiscardingTaskGroup`. Using Swift 6.4's asynchronous `defer` (SE-0493) paired with `withTaskCancellationShield` (SE-0504), connection teardown and OTP node-down fanout are guaranteed to execute atomically even when the parent server task group is cancelled during shutdown or failure.

```swift
import NIOCore
import NIOPosix

internal func supervisePeerChannel(
    _ channel: NIOAsyncChannel<WireFrame, WireFrame>,
    peer: Cluster.Node,
    onFrame: @escaping @Sendable (WireFrame, Cluster.Node) async -> Void,
    onNodeDown: @escaping @Sendable (Cluster.Node) async -> Void
) async {
    defer {
        await withTaskCancellationShield {
            try? await channel.channel.close()
            await onNodeDown(peer)
        }
    }

    do {
        try await withThrowingDiscardingTaskGroup { group in
            for try await frame in channel.inbound {
                group.addTask {
                    await onFrame(frame, peer)
                }
            }
        }
    } catch {
        // Transport closed or reset; `defer` guarantees `onNodeDown(peer)` fires once.
    }
}
```

### 6.2 Pending RPC Continuation Management

In-flight `remoteCall` requests are indexed by `correlationID` in a `Mutex`-guarded map. If `onNodeDown(peer)` triggers while RPCs to `peer` are awaiting replies, all matching continuations are immediately resumed with `ClusterError.nodeUnreachable(peer)` in $O(K)$ time (where $K$ is the number of in-flight calls to that peer), preventing hung callers.

---

## 7. OTP Supervision & Failure Semantics Matrix

To ensure `swift-OTP` supervisors behave identically to Erlang/OTP under all fault conditions, `DistributedCluster` enforces the following delivery matrix:

| Failure Scenario | Detection Mechanism | Delivered Signal | Delivery Complexity |
| :--- | :--- | :--- | :--- |
| **Normal worker stop / `deinit`** | Swift runtime invokes `resignID(_:)` | `watcher.terminated(actor: id)` | $O(W)$ local watchers |
| **Worker crashes inside `distributed func`** | Caller receives thrown error; if actor is released by supervisor, `resignID(_:)` fires | Error to caller + `terminated(actor:)` upon release | $O(W)$ |
| **Watching an already-dead local actor** | `localActors[id]` lookup returns `nil` in `registerWatch` | Immediate `watcher.terminated(actor: id)` (`:noproc`) | $O(1)$ |
| **Remote worker deinitializes cleanly** | Remote `resignID` sends `.actorTerminated(id)` frame | `watcher.terminated(actor: id)` | $O(1)$ network frame |
| **Remote node graceful `shutdown()`** | Peer closes TCP channel cleanly; `defer` triggers `onNodeDown` | `terminated(actor:)` for all `remoteSubjectsByNode[peer]` | $O(S \cdot W)$ |
| **Abrupt node crash / network partition** | TCP RST or missed `heartbeatTimeout` closes channel; `defer` triggers `onNodeDown` | `terminated(actor:)` for all `remoteSubjectsByNode[peer]` (`:noconnection`) | $O(S \cdot W)$ |
| **Supervised worker restarted under same name** | `assignID` increments `incarnation` | New `ClusterActorID` does not collide with stale `terminated` events | $O(1)$ |

*(Where $W$ is the number of watchers on a subject and $S$ is the number of watched subjects on a failed node.)*

---

## 8. Acceptance Test Suite (`Swift Testing` / Swift 6.4)

All acceptance tests **must** be implemented using Swift's `Testing` framework (`import Testing`) and pass under `swift test --repeat-until fail --maximum-repetitions 50` (SwiftPM 6.4 test repetition) to verify zero data races or flaky timing windows.

```swift
import Distributed
import DistributedCluster
import Testing

// MARK: - Test Actors

distributed actor EchoWorker {
    typealias ActorSystem = ClusterSystem

    distributed func ping(_ message: String) -> String {
        "pong:\(message)"
    }

    distributed func notifyOnly(_ value: Int) {
        _ = value
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

// MARK: - Acceptance Suite

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

        // Drop last strong reference -> triggers `resignID`
        worker = nil

        let deadID = try await supervisor.awaitNextTermination()
        #expect(deadID == firstID)

        // Simulate OTP supervisor restarting worker
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

        // Allow watch control frame to reach workerNode
        try await Task.sleep(for: .milliseconds(50))

        // Terminate worker on remote node
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
        try await Task.sleep(for: .milliseconds(50))

        // Keep strong ref to worker on ephemeralNode, but shut down the entire node
        await withExtendedLifetime(worker) {
            await ephemeralNode.shutdown()
        }

        let observedDeadID = try await supervisor.awaitNextTermination()
        #expect(observedDeadID == workerID)
    }

    @Test("AT-05: Receptionist Keyed Registration & Automatic Cleanup on Exit")
    func receptionistLifecycle() async throws {
        let system = try await ClusterSystem("reception-node") { $0.bindPort = 19108 }
        defer { await system.shutdown() }

        let key = Reception.Key("workers", as: EchoWorker.self)
        var worker: EchoWorker? = EchoWorker(actorSystem: system)
        let unwrapped = try #require(worker)

        await system.receptionist.register(unwrapped, with: key)
        let active = await system.receptionist.lookup(key)
        #expect(active.map(\.id) == [unwrapped.id])

        // Releasing worker must automatically evict it from the Receptionist
        worker = nil
        try await Task.sleep(for: .milliseconds(50))

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
        let encoded: InlineArray<13, UInt8> = original.encode()

        let decoded = try encoded.span.withUnsafeBytes { rawBuffer -> WireHeader in
            // Convert via RawSpan safe view
            let span = RawSpan(_unsafeBytes: rawBuffer)
            return try WireHeader.decode(from: span)
        }
        #expect(decoded == original)
    }
}
```

### Non-Functional CI Gates
* **AT-07 (Clean Build Time):** `swift build --build-system swiftbuild` on a clean `.build` directory must complete in $\le 5.0\text{ s}$ on standard developer hardware.
* **AT-08 (Zero-Unsafe Audit):** `grep -rn "Unsafe" Sources/DistributedCluster` must return `0` matches (outside `RawSpan` standard library bridging if required).
* **AT-09 (Data-Race Stress Gate):** `swift test --sanitize=thread` must pass with zero warnings across all concurrency tests.

---

## 9. Phased Delivery & Migration Plan

1. **Milestone 1 — Local Runtime & `swift-OTP` Compilation (Days 1–3):**
   * Implement `ClusterSystem`, `ClusterActorID`, `LifecycleWatch`, and `ClusterReceptionist` for single-node execution.
   * Point `swift-OTP`'s `Package.swift` to the local `DistributedCluster` package and verify 100% of existing single-node OTP supervision and GenServer tests pass.
2. **Milestone 2 — `NIOAsyncChannel` Transport & Remote Watch Propagation (Days 4–7):**
   * Implement `WireHeader`, `ClusterInvocationEncoder`/`Decoder`, `ClusterTransport`, and remote `.watchActor` / `.actorTerminated` / `onNodeDown` propagation.
   * Execute `AT-01` through `AT-06` under Thread Sanitizer (`--sanitize=thread`) and SwiftPM 6.4 `--repeat-until fail`.
3. **Milestone 3 — Optional SWIM Trait & Production Cutover (Days 8–10):**
   * Wire `SWIMDiscovery` trait for dynamic node discovery (if required by deployment topology) and decommission `apple/swift-distributed-actors` from `Package.resolved`.