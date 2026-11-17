import Distributed
import Foundation

/// Namespace for local receptionist keys and registration semantics.
public enum LocalReception {
    /// Strongly typed local registration key.
    ///
    /// Keys are string-identified at runtime and type-checked at compile time.
    /// This prevents accidental cross-type lookups under the same raw key ID.
    public struct Key<Act: DistributedActor>: Sendable, Hashable, CustomStringConvertible where Act.ActorSystem == ClusterSystem {
        public let id: String

        public init(_ id: String) {
            self.id = id
        }

        public var description: String {
            storageID
        }

        /// Internal key used for storage to prevent cross-type collisions for the same raw id.
        internal var storageID: String {
            "\(id)|\(String(reflecting: Act.self))"
        }
    }

    /// Registration strategy when checking an actor into the local receptionist.
    public enum CheckInMode: Sendable {
        /// Add this actor under the key, co-existing with existing registrations.
        case append
        /// Require this actor to be the only actor under the key.
        case unique
    }
}

public extension LocalReception.Key {
    /// Convenience for human-readable key construction.
    static func name(_ str: String) -> Self {
        .init(str)
    }
}

/// Errors thrown by `LocalReceptionist` operations.
public enum LocalReceptionistError: Error, Sendable {
    /// Attempted `.unique` registration for a key that is already occupied.
    case duplicateRegistration(key: String)
    /// Attempted to register an actor whose node does not match this actor system's local node.
    case remoteActorRegistration(id: ClusterSystem.ActorID)
}

/// Node-local actor registry with Erlang-style check-in, lookup, and lifecycle cleanup.
public actor LocalReceptionist {
    public typealias ActorSystem = ClusterSystem
    public typealias ActorID = ClusterSystem.ActorID

    private var storage: [String: Set<ActorID>] = [:]
    private var observers: [String: [UUID: @Sendable (Set<ActorID>) -> Void]] = [:]

    public nonisolated let actorSystem: ClusterSystem

    public init(system: ClusterSystem) {
        self.actorSystem = system
    }

    /// Register a local actor under a key.
    public func checkIn<Act>(
        _ actor: Act,
        key: LocalReception.Key<Act>,
        mode: LocalReception.CheckInMode = .append
    ) throws where Act: DistributedActor, Act.ActorSystem == ClusterSystem {
        guard actor.id.node == actorSystem.node else {
            throw LocalReceptionistError.remoteActorRegistration(id: actor.id)
        }

        let keyID = key.storageID
        var currentIDs = storage[keyID] ?? []

        if mode == .unique, !currentIDs.isEmpty {
            if currentIDs.count > 1 || currentIDs.first != actor.id {
                throw LocalReceptionistError.duplicateRegistration(key: key.id)
            }
        }

        guard !currentIDs.contains(actor.id) else {
            return
        }

        currentIDs.insert(actor.id)
        storage[keyID] = currentIDs
        notifyObservers(for: keyID)
    }

    /// Remove a specific actor from a key.
    public func checkOut<Act>(_ actor: Act, key: LocalReception.Key<Act>)
    where Act: DistributedActor, Act.ActorSystem == ClusterSystem {
        let keyID = key.storageID
        guard var ids = storage[keyID], ids.remove(actor.id) != nil else {
            return
        }

        if ids.isEmpty {
            storage.removeValue(forKey: keyID)
        } else {
            storage[keyID] = ids
        }
        notifyObservers(for: keyID)
    }

    /// Resolve and return all live actors currently checked in under `key`.
    public func lookup<Act>(_ key: LocalReception.Key<Act>) -> Set<Act>
    where Act: DistributedActor, Act.ActorSystem == ClusterSystem {
        actors(for: key.storageID)
    }

    /// Stream listings with default behavior of immediate initial emission.
    public func listing<Act>(of key: LocalReception.Key<Act>) -> AsyncStream<Set<Act>>
    where Act: DistributedActor, Act.ActorSystem == ClusterSystem {
        listing(of: key, emitInitial: true)
    }

    /// Stream listings for `key`, optionally emitting the current snapshot immediately.
    public func listing<Act>(
        of key: LocalReception.Key<Act>,
        emitInitial: Bool
    ) -> AsyncStream<Set<Act>>
    where Act: DistributedActor, Act.ActorSystem == ClusterSystem {
        let keyID = key.storageID
        let initialIDs = storage[keyID] ?? []
        return AsyncStream { continuation in
            let token = UUID()

            observers[keyID, default: [:]][token] = { [actorSystem] ids in
                let resolved = ids.compactMap { id in
                    try? Act.resolve(id: id, using: actorSystem)
                }
                continuation.yield(Set(resolved))
            }

            if emitInitial {
                let resolved = initialIDs.compactMap { id in
                    try? Act.resolve(id: id, using: actorSystem)
                }
                continuation.yield(Set(resolved))
            }

            continuation.onTermination = { [weak self] _ in
                Swift::Task {
                    await self?.removeObserver(for: keyID, token: token)
                }
            }
        }
    }

    /// Removes a terminated actor ID from all keys.
    public func terminated(actor id: ActorID) {
        for (key, ids) in storage {
            guard ids.contains(id) else {
                continue
            }

            var newIDs = ids
            newIDs.remove(id)

            if newIDs.isEmpty {
                storage.removeValue(forKey: key)
            } else {
                storage[key] = newIDs
            }
            notifyObservers(for: key)
        }
    }

    private func removeObserver(for key: String, token: UUID) {
        guard var perKey = observers[key] else {
            return
        }
        perKey.removeValue(forKey: token)
        if perKey.isEmpty {
            observers.removeValue(forKey: key)
        } else {
            observers[key] = perKey
        }
    }

    private func notifyObservers(for key: String) {
        let ids = storage[key] ?? []
        guard let callbacks = observers[key]?.values else {
            return
        }
        for callback in callbacks {
            callback(ids)
        }
    }

    private func actors<Act>(for key: String) -> Set<Act>
    where Act: DistributedActor, Act.ActorSystem == ClusterSystem {
        guard let ids = storage[key] else {
            return []
        }
        let resolved = ids.compactMap { id in
            try? Act.resolve(id: id, using: actorSystem)
        }
        return Set(resolved)
    }
}
