import Foundation
import NIOCore

public struct WireHeader: Sendable, Equatable {
    public static let magic: UInt16 = 0xDC64
    public static let currentVersion: UInt8 = 1
    public static let byteCount = 16
    public static let maxPayloadLength: UInt32 = 16 * 1024 * 1024

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
        case swimGossip = 0x0A
    }

    public let version: UInt8
    public let type: FrameType
    public let correlationID: UInt64
    public let payloadLength: UInt32

    public init(
        version: UInt8 = Self.currentVersion,
        type: FrameType,
        correlationID: UInt64,
        payloadLength: UInt32
    ) {
        self.version = version
        self.type = type
        self.correlationID = correlationID
        self.payloadLength = payloadLength
    }

    public func encode() -> InlineArray<16, UInt8> {
        var bytes = InlineArray<16, UInt8>(repeating: 0)
        bytes[0] = UInt8(truncatingIfNeeded: Self.magic >> 8)
        bytes[1] = UInt8(truncatingIfNeeded: Self.magic)
        bytes[2] = version
        bytes[3] = type.rawValue

        for i in 0..<8 {
            let shift = (7 - i) * 8
            bytes[4 + i] = UInt8(truncatingIfNeeded: correlationID >> shift)
        }

        for i in 0..<4 {
            let shift = (3 - i) * 8
            bytes[12 + i] = UInt8(truncatingIfNeeded: payloadLength >> shift)
        }
        return bytes
    }

    public static func decode(from span: RawSpan) throws(ClusterError) -> WireHeader {
        guard span.byteCount >= Self.byteCount else {
            throw .malformedFrame("Header truncated: expected \(Self.byteCount) bytes, got \(span.byteCount)")
        }

        let rawMagic = (UInt16(span[0]) << 8) | UInt16(span[1])
        guard rawMagic == Self.magic else {
            throw .malformedFrame("Invalid magic bytes: 0x\(String(rawMagic, radix: 16))")
        }

        let rawVersion = span[2]
        guard rawVersion == Self.currentVersion else {
            throw .malformedFrame("Unsupported wire protocol version: \(rawVersion)")
        }

        let rawType = span[3]
        guard let frameType = FrameType(rawValue: rawType) else {
            throw .malformedFrame("Unknown frame type: 0x\(String(rawType, radix: 16))")
        }

        var correlationID: UInt64 = 0
        for i in 0..<8 {
            correlationID = (correlationID << 8) | UInt64(span[4 + i])
        }

        var payloadLength: UInt32 = 0
        for i in 0..<4 {
            payloadLength = (payloadLength << 8) | UInt32(span[12 + i])
        }

        guard payloadLength <= Self.maxPayloadLength else {
            throw .malformedFrame("Payload length \(payloadLength) exceeds maximum \(Self.maxPayloadLength)")
        }

        return WireHeader(
            version: rawVersion,
            type: frameType,
            correlationID: correlationID,
            payloadLength: payloadLength
        )
    }
}

public struct WireFrame: Sendable, Codable {
    public var type: WireHeader.FrameType
    public var correlationID: UInt64
    public var sender: Cluster.Node
    public var payload: Data

    public init(type: WireHeader.FrameType, correlationID: UInt64, sender: Cluster.Node, payload: Data) {
        self.type = type
        self.correlationID = correlationID
        self.sender = sender
        self.payload = payload
    }
}

extension WireHeader.FrameType: Codable {}

// MARK: - Binary Wire Codec Helpers

internal enum BinaryCodec {
    static func writeString(_ string: String, to buffer: inout ByteBuffer) throws(ClusterError) {
        let utf8 = string.utf8
        guard utf8.count <= Int(UInt16.max) else {
            throw .encodingFailed("String exceeds maximum binary length (\(UInt16.max) bytes)")
        }
        buffer.writeInteger(UInt16(utf8.count))
        buffer.writeString(string)
    }

    static func readString(from buffer: inout ByteBuffer) throws(ClusterError) -> String {
        guard let length: UInt16 = buffer.readInteger() else {
            throw .malformedFrame("Truncated string length")
        }
        guard let string = buffer.readString(length: Int(length)) else {
            throw .malformedFrame("Truncated string payload")
        }
        return string
    }

    static func writeData(_ data: Data, to buffer: inout ByteBuffer) throws(ClusterError) {
        guard data.count <= Int(UInt32.max) else {
            throw .encodingFailed("Data exceeds maximum binary length (\(UInt32.max) bytes)")
        }
        buffer.writeInteger(UInt32(data.count))
        buffer.writeBytes(data)
    }

    static func readData(from buffer: inout ByteBuffer) throws(ClusterError) -> Data {
        guard let length: UInt32 = buffer.readInteger() else {
            throw .malformedFrame("Truncated data length")
        }
        guard let bytes = buffer.readBytes(length: Int(length)) else {
            throw .malformedFrame("Truncated data payload")
        }
        return Data(bytes)
    }

    static func writeNode(_ node: Cluster.Node, to buffer: inout ByteBuffer) throws(ClusterError) {
        buffer.writeInteger(node.nid)
        buffer.writeInteger(Int64(node.endpoint.port))
        try writeString(node.name, to: &buffer)
        try writeString(node.endpoint.host, to: &buffer)
    }

    static func readNode(from buffer: inout ByteBuffer) throws(ClusterError) -> Cluster.Node {
        guard let nid: UInt64 = buffer.readInteger(),
              let port: Int64 = buffer.readInteger() else {
            throw .malformedFrame("Truncated node header")
        }
        let name = try readString(from: &buffer)
        let host = try readString(from: &buffer)
        return Cluster.Node(
            name: name,
            endpoint: Cluster.Endpoint(host: host, port: Int(port)),
            nid: nid
        )
    }

    static func writeActorID(_ actorID: ClusterActorID, to buffer: inout ByteBuffer) throws(ClusterError) {
        try writeNode(actorID.node, to: &buffer)
        buffer.writeInteger(actorID.incarnation)
        try writeString(actorID.path, to: &buffer)
    }

    static func readActorID(from buffer: inout ByteBuffer) throws(ClusterError) -> ClusterActorID {
        let node = try readNode(from: &buffer)
        guard let incarnation: UInt64 = buffer.readInteger() else {
            throw .malformedFrame("Truncated actor incarnation")
        }
        let path = try readString(from: &buffer)
        return ClusterActorID(node: node, path: path, incarnation: incarnation)
    }
}

internal struct WireFrameDecoder: ByteToMessageDecoder {
    typealias InboundOut = WireFrame

    mutating func decode(context: ChannelHandlerContext, buffer: inout ByteBuffer) throws -> DecodingState {
        guard buffer.readableBytes >= WireHeader.byteCount else {
            return .needMoreData
        }
        guard let headerBytes = buffer.getBytes(at: buffer.readerIndex, length: WireHeader.byteCount) else {
            throw ClusterError.malformedFrame("Unable to read header bytes")
        }
        var headerArray = InlineArray<16, UInt8>(repeating: 0)
        for i in 0..<WireHeader.byteCount {
            headerArray[i] = headerBytes[i]
        }
        let header = try WireHeader.decode(from: headerArray.span.bytes)
        let totalLength = WireHeader.byteCount + Int(header.payloadLength)
        guard buffer.readableBytes >= totalLength else {
            return .needMoreData
        }
        buffer.moveReaderIndex(forwardBy: WireHeader.byteCount)
        guard var bodySlice = buffer.readSlice(length: Int(header.payloadLength)) else {
            throw ClusterError.malformedFrame("Unable to read frame body")
        }
        let sender = try BinaryCodec.readNode(from: &bodySlice)
        let remaining = bodySlice.readableBytes
        let payload = remaining > 0 ? Data(bodySlice.readBytes(length: remaining) ?? []) : Data()
        let frame = WireFrame(
            type: header.type,
            correlationID: header.correlationID,
            sender: sender,
            payload: payload
        )
        context.fireChannelRead(wrapInboundOut(frame))
        return .continue
    }

    mutating func decodeLast(
        context: ChannelHandlerContext,
        buffer: inout ByteBuffer,
        seenEOF: Bool
    ) throws -> DecodingState {
        while try decode(context: context, buffer: &buffer) == .continue {}
        return .needMoreData
    }
}

internal struct WireFrameEncoder: MessageToByteEncoder {
    typealias OutboundIn = WireFrame

    func encode(data: WireFrame, out: inout ByteBuffer) throws {
        var body = ByteBufferAllocator().buffer(capacity: 64 + data.payload.count)
        try BinaryCodec.writeNode(data.sender, to: &body)
        body.writeBytes(data.payload)

        guard body.readableBytes <= Int(WireHeader.maxPayloadLength) else {
            throw ClusterError.malformedFrame(
                "Encoded frame length \(body.readableBytes) exceeds maximum \(WireHeader.maxPayloadLength)"
            )
        }
        let header = WireHeader(
            type: data.type,
            correlationID: data.correlationID,
            payloadLength: UInt32(body.readableBytes)
        )
        let headerBytes = header.encode()
        for i in 0..<WireHeader.byteCount {
            out.writeInteger(headerBytes[i])
        }
        out.writeBuffer(&body)
    }
}
