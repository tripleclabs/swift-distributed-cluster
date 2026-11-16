# swift-distributed-cluster

A lightweight, source-compatible drop-in replacement for `apple/swift-distributed-actors`, built for [Swift 6.4](https://www.swift.org) on the first-party `Distributed` module. It is tailored for Erlang/OTP-style supervision (`swift-OTP`): supervisors, monitors, links, and named registries work against `ClusterSystem`, `LifecycleWatch`, and the `Receptionist` without structural changes.

Design goals:

- **Native cooperative runtime** — all scheduling, reentrancy, and mailbox behaviour is delegated to Swift's `distributed actor` runtime. No custom mailboxes, no type-erased actor shells, no protobuf code generation.
- **Modern Swift 6.4 primitives only** — `Synchronization.Mutex` / `Atomic`, `InlineArray` + `RawSpan` binary framing, `weak let` actor registries, typed `throws(ClusterError)` boundaries, and cancellation-shielded structured teardown. Zero `Unsafe*` APIs in the library sources.
- **Minimal dependency graph** — `swift-nio` (transport), `swift-log`, and `swift-service-lifecycle` only. SWIM gossip membership (`SWIMDiscovery`) and mutual TLS (`MutualTLS`) are isolated behind optional package traits.

## Requirements

- Swift 6.4 toolchain
- Linux (verified: `swift:6.4` container image, aarch64) or macOS 27 / iOS 27

## Installation

Add the package to your `Package.swift`:

```swift
dependencies: [
    .package(url: "https://github.com/example/swift-distributed-cluster.git", from: "0.1.0")
],
targets: [
    .target(
        name: "YourTarget",
        dependencies: [
            .product(name: "DistributedCluster", package: "swift-distributed-cluster")
        ]
    )
]
```

## Quick start

Define a distributed worker and call it from another node:

```swift
import Distributed
import DistributedCluster

distributed actor EchoWorker {
    typealias ActorSystem = ClusterSystem

    distributed func ping(_ message: String) -> String {
        "pong:\(message)"
    }
}

let nodeA = try await ClusterSystem("node-a") { $0.bindPort = 2551 }
let nodeB = try await ClusterSystem("node-b") { $0.bindPort = 2552 }

let workerOnB = EchoWorker(actorSystem: nodeB)
let proxyOnA = try EchoWorker.resolve(id: workerOnB.id, using: nodeA)

let reply = try await proxyOnA.ping("hello-otp")   // "pong:hello-otp"

await nodeA.shutdown()
await nodeB.shutdown()
```

### Supervision (`LifecycleWatch`)

Conform a supervisor to `LifecycleWatch` and watch workers with `watchTermination(of:)` — the OTP `Process.monitor` analogue. Termination is delivered on normal deallocation, on remote node failure, and immediately (`:noproc`) if the subject is already dead.

```swift
distributed actor Supervisor: LifecycleWatch {
    typealias ActorSystem = ClusterSystem

    distributed func supervise(_ worker: EchoWorker) {
        watchTermination(of: worker)
    }

    func terminated(actor id: ClusterActorID) async {
        // restart the worker, escalate, log, ...
    }
}
```

`ClusterActorID` carries a monotonic `incarnation`, so a supervised worker restarted under the same logical name never collides with a stale termination event.

### Named registry (`Receptionist`)

```swift
let key = Reception.Key("workers", as: EchoWorker.self)
await system.receptionist.register(worker, with: key)

let active: Set<EchoWorker> = await system.receptionist.lookup(key)

for await current in system.receptionist.listing(of: key) {
    // live updates; dead actors are evicted automatically
}
```

## Architecture

```text
swift-OTP: supervisors | GenServer | Process.monitor/link | Registry
                              |
                 DistributedCluster (this package)
   ClusterSystem (DistributedActorSystem, Service)
     |- Mutex-guarded weak actor registry + LifecycleWatch fanout
     |- ClusterReceptionist (keyed registry, AsyncStream listings)
     |- Invocation codec (Codable envelopes, correlation IDs)
     '- NIOAsyncChannel transport (discarding task group per peer)
                              |
        Swift Distributed runtime (native actor executor)
```

- **Failure detection:** every peer connection is supervised in a discarding task group with missed-heartbeat deadline enforcement (`Settings.heartbeatTimeout`); channel teardown or SWIM unreachability fans out `terminated(actor:)` to all watchers of that node's actors, evicts remote receptionist entries, and fails in-flight RPC continuations immediately with `ClusterError.nodeUnreachable`.
- **Wire protocol:** 16-byte versioned binary header (magic `0xDC64`, 1-byte protocol `version`, frame type, 64-bit correlation ID, payload length, 16 MiB max) encoded/decoded with `InlineArray` and safe `RawSpan` byte assembly — no unsafe pointer APIs and no nested JSON envelopes. Frame types cover remote calls (including generic `distributed func`s and typed `Codable` error propagation), replies, errors, watch/unwatch, termination, receptionist sync, heartbeat ping/pong, and SWIM gossip.
- **Optional traits:**
  - `SWIMDiscovery`: enables peer-to-peer SWIM gossip via `swift-cluster-membership` (`Settings.swim`, `ClusterSystem.swimMembers`).
  - `MutualTLS`: enables TLS / mTLS transport encryption via `swift-nio-ssl` (`Settings.tls`, `ClusterTLSConfiguration.mutualTLS(...)`).
- **Lifecycle:** `ClusterSystem` conforms to `ServiceLifecycle.Service`; `shutdown()` is idempotent and shielded from cancellation.

## Errors

All transport, framing, and lifecycle boundaries throw the typed `ClusterError`:

| Case | Meaning |
| :--- | :--- |
| `bindFailed(host:port:reason:)` | Listener could not bind |
| `nodeUnreachable(Cluster.Node)` | Peer down or unreachable; in-flight calls fail with this |
| `remoteActorNotFound(ClusterActorID)` | Target actor no longer lives on its node |
| `invocationTimeout(correlationID:target:)` | No reply within `Settings.requestTimeout` |
| `encodingFailed` / `decodingFailed` | Codable (de)serialization failure |
| `malformedFrame` | Bad magic, unknown frame type, or oversized payload |
| `systemShuttingDown` | Call attempted during/after `shutdown()` |

## Development

```bash
swift build --build-system swiftbuild
swift test
swift test --traits SWIMDiscovery
swift test --traits MutualTLS
swift test --traits SWIMDiscovery,MutualTLS
swift test --sanitize=thread
swift test --repeat-until fail --maximum-repetitions 25
```

Test coverage (Swift Testing suite `AT-01`…`AT-06`, multi-node loopback TCP/mTLS tests, plus binary codec, cancellation, and lifecycle edge tests in `Tests/DistributedClusterTests`):

```bash
swift test --enable-code-coverage
xcrun llvm-cov report \
  .build/out/Products/Debug/DistributedClusterTests.xctest/Contents/MacOS/DistributedClusterTests \
  -instr-profile=.build/out/Products/Debug/codecov/default.profdata \
  Sources/DistributedCluster
```

Current status: 30 tests (default traits), 31 tests with `SWIMDiscovery` or `MutualTLS`, 32 tests with `--traits SWIMDiscovery,MutualTLS`; 94.60% line coverage of `Sources/DistributedCluster` (default traits).

## Limitations

- Multi-node tests run in-process across distinct `127.0.0.1` TCP ports rather than separate OS processes.
- Non-`Codable` errors thrown by remote `distributed func`s cannot be reconstructed by type and surface as `ClusterError.decodingFailed` with the remote error description.

## Specification

Implemented against `SPEC-DC-6.4` (see `SPEC.md`). Deviations worth knowing: the binary header is 16 bytes (2-byte magic + 1-byte version + 1-byte frame type + 8-byte correlation ID + 4-byte payload length), and Apple deployment targets are macOS/iOS 27 because `InlineArray`/`RawSpan` and `withTaskCancellationShield` require them — neither affects Linux.
