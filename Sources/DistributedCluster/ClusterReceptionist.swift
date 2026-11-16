import Distributed
import Foundation
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
    internal struct WeakSystemBox: Sendable {
        weak let system: ClusterSystem?
    }

    internal struct State {
        var local: [String: [ClusterActorID: WeakActorBox]] = [:]
        var remote: [String: Set<ClusterActorID>] = [:]
        var subscribers: [String: [UUID: @Sendable () -> Void]] = [:]
        var systemBox: WeakSystemBox?
    }

    internal let localNode: Cluster.Node
    internal let state = Mutex(State())

    public init(localNode: Cluster.Node) {
        self.localNode = localNode
    }

    internal func attach(system: ClusterSystem) {
        state.withLock { s in
            s.systemBox = WeakSystemBox(system: system)
        }
    }

    public func register<Act: DistributedActor>(
        _ actor: Act,
        with key: Reception.Key<Act>
    ) async where Act.ID == ClusterActorID {
        let changed = state.withLock { s -> Bool in
            var entries = s.local[key.id] ?? [:]
            entries[actor.id] = WeakActorBox(actor: actor)
            s.local[key.id] = entries
            return true
        }
        if changed {
            notifySubscribers(keyID: key.id)
            if let system = state.withLock({ $0.systemBox?.system }) {
                await system.broadcastReceptionistSync(keyID: key.id, actorID: actor.id, removed: false)
            }
        }
    }

    public func lookup<Act: DistributedActor>(
        _ key: Reception.Key<Act>,
        as actorType: Act.Type = Act.self
    ) async -> Set<Act> where Act.ID == ClusterActorID {
        let (localSnapshot, remoteIDs, system) = state.withLock { s -> ([ClusterActorID: WeakActorBox], Set<ClusterActorID>, ClusterSystem?) in
            let entries = s.local[key.id] ?? [:]
            return (entries, s.remote[key.id] ?? [], s.systemBox?.system)
        }
        var result = Set<Act>()
        for (_, box) in localSnapshot {
            if let actor = box.actor, let casted = actor as? Act {
                result.insert(casted)
            }
        }
        if let actorSystem = system as? Act.ActorSystem {
            for id in remoteIDs {
                if let resolved = try? Act.resolve(id: id, using: actorSystem) {
                    result.insert(resolved)
                }
            }
        }
        return result
    }

    public func listing<Act: DistributedActor>(
        of key: Reception.Key<Act>,
        as actorType: Act.Type = Act.self
    ) -> AsyncStream<Set<Act>> where Act.ID == ClusterActorID {
        let subscriberID = UUID()
        let (stream, continuation) = AsyncStream<Set<Act>>.makeStream()
        let (triggerStream, triggerContinuation) = AsyncStream<Void>.makeStream(
            bufferingPolicy: .bufferingNewest(1)
        )

        state.withLock { s in
            var subs = s.subscribers[key.id] ?? [:]
            subs[subscriberID] = {
                triggerContinuation.yield(())
            }
            s.subscribers[key.id] = subs
        }

        let consumerTask = Swift::Task { [weak self] in
            for await _ in triggerStream {
                guard let self else { break }
                let current = await self.lookup(key, as: actorType)
                continuation.yield(current)
            }
        }

        continuation.onTermination = { [weak self] _ in
            triggerContinuation.finish()
            consumerTask.cancel()
            guard let self else { return }
            self.state.withLock { s in
                s.subscribers[key.id]?.removeValue(forKey: subscriberID)
                if s.subscribers[key.id]?.isEmpty == true {
                    s.subscribers.removeValue(forKey: key.id)
                }
            }
        }

        triggerContinuation.yield(())
        return stream
    }

    internal func removeActor(_ id: ClusterActorID) async {
        let changedKeys = state.withLock { s -> [String] in
            var changed: [String] = []
            for keyID in Array(s.local.keys) {
                if s.local[keyID]?.removeValue(forKey: id) != nil {
                    if s.local[keyID]?.isEmpty == true {
                        s.local.removeValue(forKey: keyID)
                    }
                    changed.append(keyID)
                }
            }
            return changed
        }
        for keyID in changedKeys {
            notifySubscribers(keyID: keyID)
        }
        if !changedKeys.isEmpty, let system = state.withLock({ $0.systemBox?.system }) {
            for keyID in changedKeys {
                await system.broadcastReceptionistSync(keyID: keyID, actorID: id, removed: true)
            }
        }
    }

    internal func removeRemoteActors(on deadNode: Cluster.Node) {
        let changedKeys = state.withLock { s -> [String] in
            var changed: [String] = []
            for keyID in Array(s.remote.keys) {
                guard var ids = s.remote[keyID] else { continue }
                let before = ids.count
                ids = ids.filter { $0.node != deadNode }
                if ids.count != before {
                    if ids.isEmpty {
                        s.remote.removeValue(forKey: keyID)
                    } else {
                        s.remote[keyID] = ids
                    }
                    changed.append(keyID)
                }
            }
            return changed
        }
        for keyID in changedKeys {
            notifySubscribers(keyID: keyID)
        }
    }

    internal func allLocalRegistrations() -> [(keyID: String, actorID: ClusterActorID)] {
        state.withLock { s in
            var items: [(String, ClusterActorID)] = []
            for (keyID, entries) in s.local {
                for (actorID, box) in entries where box.actor != nil {
                    items.append((keyID, actorID))
                }
            }
            return items
        }
    }

    internal func applyRemoteSync(keyID: String, actorID: ClusterActorID, removed: Bool) {
        state.withLock { s in
            var ids = s.remote[keyID] ?? []
            if removed {
                ids.remove(actorID)
            } else {
                ids.insert(actorID)
            }
            if ids.isEmpty {
                s.remote.removeValue(forKey: keyID)
            } else {
                s.remote[keyID] = ids
            }
        }
        notifySubscribers(keyID: keyID)
    }

    internal func notifySubscribers(keyID: String) {
        let subs = state.withLock { s -> [UUID: @Sendable () -> Void] in
            s.subscribers[keyID] ?? [:]
        }
        for (_, handler) in subs {
            handler()
        }
    }
}
