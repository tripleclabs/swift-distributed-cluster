import Foundation
import NIOCore

public struct WireHeader: Sendable, Equatable {
    public static let magic: UInt16 = 0xDC64
    public static let byteCount = 15
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
    }

    public let type: FrameType
    public let correlationID: UInt64
    public let payloadLength: UInt32

    public init(type: FrameType, correlationID: UInt64, payloadLength: UInt32) {
        self.type = type
        self.correlationID = correlationID
        self.payloadLength = payloadLength
    }

    public func encode() -> InlineArray<15, UInt8> {
        var bytes = InlineArray<15, UInt8>(repeating: 0)
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

        let rawMagic = (UInt16(span[0]) << 8) | UInt16(span[1])
        guard rawMagic == Self.magic else {
            throw .malformedFrame("Invalid magic bytes: 0x\(String(rawMagic, radix: 16))")
        }

        let rawType = span[2]
        guard let frameType = FrameType(rawValue: rawType) else {
            throw .malformedFrame("Unknown frame type: 0x\(String(rawType, radix: 16))")
        }

        var correlationID: UInt64 = 0
        for i in 0..<8 {
            correlationID = (correlationID << 8) | UInt64(span[3 + i])
        }

        var payloadLength: UInt32 = 0
        for i in 0..<4 {
            payloadLength = (payloadLength << 8) | UInt32(span[11 + i])
        }

        guard payloadLength <= Self.maxPayloadLength else {
            throw .malformedFrame("Payload length \(payloadLength) exceeds maximum \(Self.maxPayloadLength)")
        }

        return WireHeader(
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

internal struct WireFrameEnvelope: Codable {
    var sender: Cluster.Node
    var payload: Data
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
        var headerArray = InlineArray<15, UInt8>(repeating: 0)
        for i in 0..<WireHeader.byteCount {
            headerArray[i] = headerBytes[i]
        }
        let header = try WireHeader.decode(from: headerArray.span.bytes)
        let totalLength = WireHeader.byteCount + Int(header.payloadLength)
        guard buffer.readableBytes >= totalLength else {
            return .needMoreData
        }
        buffer.moveReaderIndex(forwardBy: WireHeader.byteCount)
        guard let payloadBytes = buffer.readBytes(length: Int(header.payloadLength)) else {
            throw ClusterError.malformedFrame("Unable to read payload bytes")
        }
        let envelope: WireFrameEnvelope
        do {
            envelope = try JSONDecoder().decode(WireFrameEnvelope.self, from: Data(payloadBytes))
        } catch {
            throw ClusterError.malformedFrame("Unable to decode frame envelope: \(error)")
        }
        let frame = WireFrame(
            type: header.type,
            correlationID: header.correlationID,
            sender: envelope.sender,
            payload: envelope.payload
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
        let envelope = WireFrameEnvelope(sender: data.sender, payload: data.payload)
        let envelopeData: Data
        do {
            envelopeData = try JSONEncoder().encode(envelope)
        } catch {
            throw ClusterError.encodingFailed("Unable to encode frame envelope: \(error)")
        }
        let header = WireHeader(
            type: data.type,
            correlationID: data.correlationID,
            payloadLength: UInt32(envelopeData.count)
        )
        let headerBytes = header.encode()
        for i in 0..<WireHeader.byteCount {
            out.writeInteger(headerBytes[i])
        }
        out.writeBytes(envelopeData)
    }
}
