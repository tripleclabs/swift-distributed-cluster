#if SWIMDiscovery
import ClusterMembership
import Foundation
import Logging
import SWIM
import Synchronization

// MARK: - Node Conversion

extension Cluster.Node {
    internal var swimNode: ClusterMembership.Node {
        ClusterMembership.Node(
            protocol: "dc",
            name: name,
            host: endpoint.host,
            port: endpoint.port,
            uid: nid
        )
    }

    internal init?(swimNode: ClusterMembership.Node) {
        guard let uid = swimNode.uid else { return nil }
        self.init(
            name: swimNode.name ?? "\(swimNode.host):\(swimNode.port)",
            endpoint: Cluster.Endpoint(host: swimNode.host, port: swimNode.port),
            nid: uid
        )
    }
}

// MARK: - Wire DTOs

internal enum SWIMWireStatus: Codable, Sendable {
    case alive(incarnation: UInt64)
    case suspect(incarnation: UInt64, suspectedBy: [Cluster.Node])
    case unreachable(incarnation: UInt64)
    case dead

    init(from status: SWIM.Status) {
        switch status {
        case .alive(let inc):
            self = .alive(incarnation: inc)
        case .suspect(let inc, let by):
            self = .suspect(incarnation: inc, suspectedBy: by.compactMap(Cluster.Node.init(swimNode:)))
        case .unreachable(let inc):
            self = .unreachable(incarnation: inc)
        case .dead:
            self = .dead
        }
    }

    func toSWIMStatus() -> SWIM.Status {
        switch self {
        case .alive(let inc):
            return .alive(incarnation: inc)
        case .suspect(let inc, let by):
            return .suspect(incarnation: inc, suspectedBy: Set(by.map(\.swimNode)))
        case .unreachable(let inc):
            return .unreachable(incarnation: inc)
        case .dead:
            return .dead
        }
    }
}

internal struct SWIMWireMember: Codable, Sendable {
    var node: Cluster.Node
    var status: SWIMWireStatus
    var protocolPeriod: UInt64

    init(from member: SWIM.Member) {
        self.node = Cluster.Node(swimNode: member.node)
            ?? Cluster.Node(
                name: member.node.name ?? member.node.host,
                endpoint: Cluster.Endpoint(host: member.node.host, port: member.node.port),
                nid: member.node.uid ?? 0
            )
        self.status = SWIMWireStatus(from: member.status)
        self.protocolPeriod = member.protocolPeriod
    }

    func toSWIMMember() -> SWIM.Member {
        SWIM.Member(
            node: node.swimNode,
            status: status.toSWIMStatus(),
            protocolPeriod: protocolPeriod
        )
    }
}

internal enum SWIMWireGossipPayload: Codable, Sendable {
    case none
    case membership([SWIMWireMember])

    init(from payload: SWIM.GossipPayload) {
        switch payload {
        case .none:
            self = .none
        case .membership(let members):
            self = .membership(members.map(SWIMWireMember.init(from:)))
        }
    }

    func toSWIMGossipPayload() -> SWIM.GossipPayload {
        switch self {
        case .none:
            return .none
        case .membership(let members):
            return .membership(members.map { $0.toSWIMMember() })
        }
    }
}

internal enum SWIMWireMessage: Codable, Sendable {
    case ping(
        origin: Cluster.Node,
        payload: SWIMWireGossipPayload,
        sequenceNumber: SWIM.SequenceNumber
    )
    case ack(
        target: Cluster.Node,
        incarnation: SWIM.Incarnation,
        payload: SWIMWireGossipPayload,
        sequenceNumber: SWIM.SequenceNumber
    )
    case nack(
        target: Cluster.Node,
        sequenceNumber: SWIM.SequenceNumber
    )
    case pingRequest(
        origin: Cluster.Node,
        target: Cluster.Node,
        payload: SWIMWireGossipPayload,
        sequenceNumber: SWIM.SequenceNumber
    )
}

// MARK: - SWIMMembershipAdapter

internal final class SWIMMembershipAdapter: Sendable {
    private struct State {
        var instance: SWIM.Instance
        var tickTask: Swift::Task<Void, Never>?
        var pendingPings: [SWIM.SequenceNumber: (target: ClusterMembership.Node, task: Swift::Task<Void, Never>)] = [:]
        var isShutdown: Bool = false
        var onMemberAlive: (@Sendable (Cluster.Node) async -> Void)?
        var onMemberDown: (@Sendable (Cluster.Node) async -> Void)?
    }

    private let localNode: Cluster.Node
    private let transport: ClusterTransport
    private let state: Mutex<State>
    private let initialSeq = Atomic<UInt32>(1_000_000)

    init(localNode: Cluster.Node, settings: SWIM.Settings, transport: ClusterTransport) {
        var resolved = settings
        resolved.node = localNode.swimNode
        let instance = SWIM.Instance(settings: resolved, myself: localNode.swimNode)
        self.localNode = localNode
        self.transport = transport
        self.state = Mutex(State(instance: instance))
    }

    func start(
        onMemberAlive: @escaping @Sendable (Cluster.Node) async -> Void,
        onMemberDown: @escaping @Sendable (Cluster.Node) async -> Void
    ) {
        let task = Swift::Task { [weak self] in
            while !Swift::Task.isCancelled {
                guard let self else { break }
                let delay = await self.runPeriodicTick()
                guard let delay else { break }
                do {
                    try await Swift::Task.sleep(for: delay)
                } catch {
                    break
                }
            }
        }
        let cancelled = state.withLock { s -> Bool in
            if s.isShutdown { return true }
            s.onMemberAlive = onMemberAlive
            s.onMemberDown = onMemberDown
            s.tickTask = task
            return false
        }
        if cancelled {
            task.cancel()
        }
    }

    func peerConnected(_ peer: Cluster.Node) async {
        let seq = initialSeq.wrappingAdd(1, ordering: .sequentiallyConsistent).newValue
        let selfMember = SWIM.Member(
            node: localNode.swimNode,
            status: .alive(incarnation: 0),
            protocolPeriod: 0
        )
        let msg = SWIMWireMessage.ping(
            origin: localNode,
            payload: SWIMWireGossipPayload(from: .membership([selfMember])),
            sequenceNumber: seq
        )
        await sendSWIMMessage(msg, to: peer)
    }

    func peerDisconnected(_ peer: Cluster.Node) {
        _ = state.withLock { s in
            s.instance.confirmDead(node: peer.swimNode)
        }
    }

    func members() -> [Cluster.Node] {
        let selfSwimNode = localNode.swimNode
        return state.withLock { s in
            s.instance.members.compactMap { member in
                if member.node == selfSwimNode || member.isDead {
                    return nil
                }
                return Cluster.Node(swimNode: member.node)
            }
        }
    }

    func handleInboundGossip(_ data: Data, from peer: Cluster.Node) async {
        guard let message = try? decodeCodableValue(SWIMWireMessage.self, from: data) else { return }
        switch message {
        case .ping(let origin, let payload, let seq):
            let directives = state.withLock { s in
                s.instance.onPing(
                    pingOrigin: origin.swimNode,
                    payload: payload.toSWIMGossipPayload(),
                    sequenceNumber: seq
                )
            }
            for directive in directives {
                switch directive {
                case .gossipProcessed(let gossipDirective):
                    await handleGossipDirective(gossipDirective)
                case .sendAck(let to, let pingedTarget, let incarnation, let replyPayload, let ackSeq):
                    let targetNode = Cluster.Node(swimNode: pingedTarget) ?? localNode
                    let toNode = Cluster.Node(swimNode: to) ?? origin
                    await sendSWIMMessage(
                        .ack(
                            target: targetNode,
                            incarnation: incarnation,
                            payload: SWIMWireGossipPayload(from: replyPayload),
                            sequenceNumber: ackSeq
                        ),
                        to: toNode
                    )
                }
            }

        case .ack(let target, let incarnation, let payload, let seq):
            let timeoutTask = state.withLock { s in
                s.pendingPings.removeValue(forKey: seq)?.task
            }
            timeoutTask?.cancel()
            let directives = state.withLock { s in
                s.instance.onPingResponse(
                    response: .ack(
                        target: target.swimNode,
                        incarnation: incarnation,
                        payload: payload.toSWIMGossipPayload(),
                        sequenceNumber: seq
                    ),
                    pingRequestOrigin: nil,
                    pingRequestSequenceNumber: nil
                )
            }
            for directive in directives {
                await handlePingResponseDirective(directive)
            }

        case .nack(let target, let seq):
            let timeoutTask = state.withLock { s in
                s.pendingPings.removeValue(forKey: seq)?.task
            }
            timeoutTask?.cancel()
            let directives = state.withLock { s in
                s.instance.onPingResponse(
                    response: .nack(target: target.swimNode, sequenceNumber: seq),
                    pingRequestOrigin: nil,
                    pingRequestSequenceNumber: nil
                )
            }
            for directive in directives {
                await handlePingResponseDirective(directive)
            }

        case .pingRequest(let origin, let target, let payload, let seq):
            let directives = state.withLock { s in
                s.instance.onPingRequest(
                    target: target.swimNode,
                    pingRequestOrigin: origin.swimNode,
                    payload: payload.toSWIMGossipPayload(),
                    sequenceNumber: seq
                )
            }
            for directive in directives {
                switch directive {
                case .gossipProcessed(let gossipDirective):
                    await handleGossipDirective(gossipDirective)
                case .sendPing(let probeTarget, let probePayload, _, _, _, let probeSeq):
                    if let targetNode = Cluster.Node(swimNode: probeTarget) {
                        await sendSWIMMessage(
                            .ping(
                                origin: localNode,
                                payload: SWIMWireGossipPayload(from: probePayload),
                                sequenceNumber: probeSeq
                            ),
                            to: targetNode
                        )
                    }
                }
            }
        }
    }

    func shutdown() async {
        let (tickTask, pingTasks) = state.withLock { s -> (Swift::Task<Void, Never>?, [Swift::Task<Void, Never>]) in
            s.isShutdown = true
            let tick = s.tickTask
            s.tickTask = nil
            let pings = s.pendingPings.values.map(\.task)
            s.pendingPings.removeAll()
            s.onMemberAlive = nil
            s.onMemberDown = nil
            return (tick, pings)
        }
        tickTask?.cancel()
        for task in pingTasks {
            task.cancel()
        }
        await tickTask?.value
        for task in pingTasks {
            await task.value
        }
    }

    // MARK: - Directive Helpers

    private func runPeriodicTick() async -> Duration? {
        let (isShutdown, directives) = state.withLock { s -> (Bool, [SWIM.Instance.PeriodicPingTickDirective]) in
            if s.isShutdown { return (true, []) }
            return (false, s.instance.onPeriodicPingTick())
        }
        guard !isShutdown else { return nil }

        var nextDelay: Duration = .seconds(1)
        for directive in directives {
            switch directive {
            case .membershipChanged(let event):
                await handleMembershipEvent(event)
            case .sendPing(let target, let payload, let timeout, let seq):
                if let targetNode = Cluster.Node(swimNode: target) {
                    schedulePingTimeout(target: target, timeout: timeout, sequenceNumber: seq)
                    await sendSWIMMessage(
                        .ping(
                            origin: localNode,
                            payload: SWIMWireGossipPayload(from: payload),
                            sequenceNumber: seq
                        ),
                        to: targetNode
                    )
                }
            case .scheduleNextTick(let delay):
                nextDelay = delay
            }
        }
        return nextDelay
    }

    private func schedulePingTimeout(target: ClusterMembership.Node, timeout: Duration, sequenceNumber: SWIM.SequenceNumber) {
        let task = Swift::Task { [weak self] in
            do {
                try await Swift::Task.sleep(for: timeout)
            } catch {
                return
            }
            guard let self else { return }
            let removed = self.state.withLock { s in
                s.pendingPings.removeValue(forKey: sequenceNumber) != nil
            }
            guard removed else { return }
            let directives = self.state.withLock { s in
                s.instance.onPingResponse(
                    response: .timeout(
                        target: target,
                        pingRequestOrigin: nil,
                        timeout: timeout,
                        sequenceNumber: sequenceNumber
                    ),
                    pingRequestOrigin: nil,
                    pingRequestSequenceNumber: nil
                )
            }
            for directive in directives {
                await self.handlePingResponseDirective(directive)
            }
        }
        let inserted = state.withLock { s -> Bool in
            if s.isShutdown { return false }
            s.pendingPings[sequenceNumber] = (target, task)
            return true
        }
        if !inserted {
            task.cancel()
        }
    }

    private func handlePingResponseDirective(_ directive: SWIM.Instance.PingResponseDirective) async {
        switch directive {
        case .gossipProcessed(let gossipDirective):
            await handleGossipDirective(gossipDirective)
        case .sendPingRequests(let reqDirective):
            guard let targetNode = Cluster.Node(swimNode: reqDirective.target) else { return }
            for detail in reqDirective.requestDetails {
                if let proxyNode = Cluster.Node(swimNode: detail.peerToPingRequestThrough) {
                    await sendSWIMMessage(
                        .pingRequest(
                            origin: localNode,
                            target: targetNode,
                            payload: SWIMWireGossipPayload(from: detail.payload),
                            sequenceNumber: detail.sequenceNumber
                        ),
                        to: proxyNode
                    )
                }
            }
        case .sendAck(let to, let seq, let pingedTarget, let incarnation, let payload):
            if let toNode = Cluster.Node(swimNode: to) {
                let targetNode = Cluster.Node(swimNode: pingedTarget) ?? localNode
                await sendSWIMMessage(
                    .ack(
                        target: targetNode,
                        incarnation: incarnation,
                        payload: SWIMWireGossipPayload(from: payload),
                        sequenceNumber: seq
                    ),
                    to: toNode
                )
            }
        case .sendNack(let to, let seq, let target):
            if let toNode = Cluster.Node(swimNode: to) {
                let targetNode = Cluster.Node(swimNode: target) ?? localNode
                await sendSWIMMessage(.nack(target: targetNode, sequenceNumber: seq), to: toNode)
            }
        }
    }

    private func handleGossipDirective(_ directive: SWIM.Instance.GossipProcessedDirective) async {
        if case .applied(let event?) = directive {
            await handleMembershipEvent(event)
        }
    }

    private func handleMembershipEvent(_ event: SWIM.MemberStatusChangedEvent) async {
        guard let node = Cluster.Node(swimNode: event.member.node), node != localNode else {
            return
        }
        if event.member.isDead || event.member.isUnreachable {
            let onDown = state.withLock { $0.onMemberDown }
            await onDown?(node)
        } else if event.member.isAlive && event.previousStatus == nil {
            let onAlive = state.withLock { $0.onMemberAlive }
            await onAlive?(node)
        }
    }

    private func sendSWIMMessage(_ message: SWIMWireMessage, to node: Cluster.Node) async {
        guard let payload = try? encodeCodableValue(message) else { return }
        let frame = WireFrame(type: .swimGossip, correlationID: 0, sender: localNode, payload: payload)
        try? await transport.sendFrame(frame, to: node)
    }
}
#endif
