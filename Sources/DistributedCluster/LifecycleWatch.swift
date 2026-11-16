import Distributed

public protocol LifecycleWatch: DistributedActor where ActorSystem == ClusterSystem {
    func terminated(actor id: ClusterSystem.ActorID) async
}

extension LifecycleWatch {
    public func watchTermination<Act: DistributedActor>(
        of subject: Act,
        file: String = #fileID,
        line: UInt = #line
    ) where Act.ID == ClusterSystem.ActorID {
        self.actorSystem.registerWatch(watcher: self, subjectID: subject.id)
    }

    public func unwatchTermination<Act: DistributedActor>(
        of subject: Act
    ) where Act.ID == ClusterSystem.ActorID {
        self.actorSystem.unregisterWatch(watcherID: self.id, subjectID: subject.id)
    }
}
