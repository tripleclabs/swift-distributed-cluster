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
    internal struct WeakActorBox: Sendable {
        weak let actor: any DistributedActor?
    }

    internal struct WeakSystemBox: Sendable {
        weak let system: ClusterSystem?
    }

    internal struct State {
        var local: [String: [ClusterActorID: WeakActorBox]] = [:]
        var remote: [String: Set<ClusterActorID>] = [:]
        var subscribers: [String: [UUID: @Sendable (Set<ClusterActorID>) -> Void]] = [:]
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
        if let system {
            for id in remoteIDs {
                if let resolved = try? system.resolve(id: id, as: Act.self) {
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
        state.withLock { s in
            var subs = s.subscribers[key.id] ?? [:]
            subs[subscriberID] = { [weak self] _ in
                guard let self else { return }
                Swift::Task {
                    let current = await self.lookup(key, as: actorType)
                    continuation.yield(current)
                }
            }
            s.subscribers[key.id] = subs
        }
        continuation.onTermination = { [weak self] _ in
            guard let self else { return }
            self.state.withLock { s in
                _ = s.subscribers[key.id]?.removeValue(forKey: subscriberID)
            }
        }
        Swift::Task { [weak self] in
            guard let self else { return }
            let current = await self.lookup(key, as: actorType)
            continuation.yield(current)
        }
        return stream
    }

    internal func removeActor(_ id: ClusterActorID) async {
        let changedKeys = state.withLock { s -> [String] in
            var changed: [String] = []
            for keyID in Array(s.local.keys) {
                if s.local[keyID]?.removeValue(forKey: id) != nil {
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

    internal func applyRemoteSync(keyID: String, actorID: ClusterActorID, removed: Bool) {
        state.withLock { s in
            var ids = s.remote[keyID] ?? []
            if removed {
                ids.remove(actorID)
            } else {
                ids.insert(actorID)
            }
            s.remote[keyID] = ids
        }
        notifySubscribers(keyID: keyID)
    }

    internal func notifySubscribers(keyID: String) {
        let (subs, ids) = state.withLock { s -> ([UUID: @Sendable (Set<ClusterActorID>) -> Void], Set<ClusterActorID>) in
            var all = Set<ClusterActorID>()
            if let entries = s.local[keyID] {
                for (actorID, box) in entries where box.actor != nil {
                    all.insert(actorID)
                }
            }
            if let remoteIDs = s.remote[keyID] {
                all.formUnion(remoteIDs)
            }
            return (s.subscribers[keyID] ?? [:], all)
        }
        for (_, handler) in subs {
            handler(ids)
        }
    }
}
