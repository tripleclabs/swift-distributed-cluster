import Distributed
import Foundation
import NIOCore

public struct InvocationPayload: Codable, Sendable {
    public var targetIdentifier: String
    public var genericSubstitutions: [String]
    public var arguments: [Data]
    public var returnType: String
    public var errorType: String

    public init(
        targetIdentifier: String,
        genericSubstitutions: [String],
        arguments: [Data],
        returnType: String,
        errorType: String
    ) {
        self.targetIdentifier = targetIdentifier
        self.genericSubstitutions = genericSubstitutions
        self.arguments = arguments
        self.returnType = returnType
        self.errorType = errorType
    }

    internal func encodeBinary() throws(ClusterError) -> Data {
        var buffer = ByteBufferAllocator().buffer(capacity: 128)
        try BinaryCodec.writeString(targetIdentifier, to: &buffer)
        try BinaryCodec.writeString(returnType, to: &buffer)
        try BinaryCodec.writeString(errorType, to: &buffer)

        guard genericSubstitutions.count <= Int(UInt16.max) else {
            throw .encodingFailed("Too many generic substitutions")
        }
        buffer.writeInteger(UInt16(genericSubstitutions.count))
        for sub in genericSubstitutions {
            try BinaryCodec.writeString(sub, to: &buffer)
        }

        guard arguments.count <= Int(UInt16.max) else {
            throw .encodingFailed("Too many invocation arguments")
        }
        buffer.writeInteger(UInt16(arguments.count))
        for arg in arguments {
            try BinaryCodec.writeData(arg, to: &buffer)
        }
        return Data(buffer.readableBytesView)
    }

    internal static func decodeBinary(from data: Data) throws(ClusterError) -> InvocationPayload {
        var buffer = ByteBufferAllocator().buffer(capacity: data.count)
        buffer.writeBytes(data)

        let targetIdentifier = try BinaryCodec.readString(from: &buffer)
        let returnType = try BinaryCodec.readString(from: &buffer)
        let errorType = try BinaryCodec.readString(from: &buffer)

        guard let subCount: UInt16 = buffer.readInteger() else {
            throw .decodingFailed("Truncated generic substitution count")
        }
        var genericSubstitutions: [String] = []
        genericSubstitutions.reserveCapacity(Int(subCount))
        for _ in 0..<subCount {
            genericSubstitutions.append(try BinaryCodec.readString(from: &buffer))
        }

        guard let argCount: UInt16 = buffer.readInteger() else {
            throw .decodingFailed("Truncated argument count")
        }
        var arguments: [Data] = []
        arguments.reserveCapacity(Int(argCount))
        for _ in 0..<argCount {
            arguments.append(try BinaryCodec.readData(from: &buffer))
        }

        return InvocationPayload(
            targetIdentifier: targetIdentifier,
            genericSubstitutions: genericSubstitutions,
            arguments: arguments,
            returnType: returnType,
            errorType: errorType
        )
    }
}

internal struct RemoteCallEnvelope: Sendable {
    var targetID: ClusterActorID
    var invocationData: Data

    func encodeBinary() throws(ClusterError) -> Data {
        var buffer = ByteBufferAllocator().buffer(capacity: 64 + invocationData.count)
        try BinaryCodec.writeActorID(targetID, to: &buffer)
        buffer.writeBytes(invocationData)
        return Data(buffer.readableBytesView)
    }

    static func decodeBinary(from data: Data) throws(ClusterError) -> RemoteCallEnvelope {
        var buffer = ByteBufferAllocator().buffer(capacity: data.count)
        buffer.writeBytes(data)
        let targetID = try BinaryCodec.readActorID(from: &buffer)
        let remaining = buffer.readableBytes
        let invocationData = remaining > 0 ? Data(buffer.readBytes(length: remaining) ?? []) : Data()
        return RemoteCallEnvelope(targetID: targetID, invocationData: invocationData)
    }
}

internal struct ErrorEnvelope: Codable, Sendable {
    var clusterError: ClusterError?
    var errorTypeName: String?
    var errorData: Data?
    var message: String
}

internal func decodeThrownError(from data: Data) -> any Error {
    guard let envelope = try? decodeCodableValue(ErrorEnvelope.self, from: data) else {
        return ClusterError.decodingFailed("Remote error frame could not be decoded")
    }
    if let clusterError = envelope.clusterError {
        return clusterError
    }
    if let typeName = envelope.errorTypeName,
       let errorData = envelope.errorData,
       let resolvedType = _typeByName(typeName) as? any (Error & Decodable).Type {
        func open<E: Error & Decodable>(_ type: E.Type) throws -> E {
            try JSONDecoder().decode(E.self, from: errorData)
        }
        if let decoded = try? open(resolvedType) {
            return decoded
        }
    }
    return ClusterError.decodingFailed("Remote error: \(envelope.message)")
}

internal func encodeCodableValue<T: Codable>(_ value: T) throws(ClusterError) -> Data {
    do {
        return try JSONEncoder().encode(value)
    } catch {
        throw .encodingFailed("Failed to encode \(T.self): \(error)")
    }
}

internal func encodeAnyCodable(_ value: any Codable) throws(ClusterError) -> Data {
    func open<U: Codable>(_ opened: U) throws(ClusterError) -> Data {
        try encodeCodableValue(opened)
    }
    return try open(value)
}

internal func decodeCodableValue<T>(_ type: T.Type, from data: Data) throws(ClusterError) -> T {
    guard let codableType = T.self as? any Codable.Type else {
        throw .decodingFailed("Type \(T.self) does not conform to Codable")
    }
    func open<U: Codable>(_ openedType: U.Type) throws(ClusterError) -> U {
        do {
            return try JSONDecoder().decode(U.self, from: data)
        } catch {
            throw .decodingFailed("Failed to decode \(U.self): \(error)")
        }
    }
    let decoded = try open(codableType)
    guard let casted = decoded as? T else {
        throw .decodingFailed("Decoded value does not match expected type \(T.self)")
    }
    return casted
}

public struct ClusterInvocationEncoder: Distributed::DistributedTargetInvocationEncoder, Sendable {
    public typealias SerializationRequirement = Codable

    public var genericSubstitutions: [String] = []
    public var argumentsData: [Data] = []
    public var returnTypeName: String = ""
    public var errorTypeName: String = ""

    public init() {}

    public mutating func recordGenericSubstitution<T>(_ type: T.Type) throws {
        genericSubstitutions.append(_mangledTypeName(type) ?? "")
    }

    public mutating func recordArgument<Value>(_ argument: Distributed::RemoteCallArgument<Value>) throws where Value: Codable {
        argumentsData.append(try encodeCodableValue(argument.value))
    }

    public mutating func recordReturnType<R>(_ type: R.Type) throws where R: Codable {
        returnTypeName = _mangledTypeName(type) ?? ""
    }

    public mutating func recordErrorType<E>(_ type: E.Type) throws where E: Error {
        errorTypeName = _mangledTypeName(type) ?? ""
    }

    public mutating func doneRecording() throws {}

    public func makePayload(targetIdentifier: String) throws(ClusterError) -> Data {
        let payload = InvocationPayload(
            targetIdentifier: targetIdentifier,
            genericSubstitutions: genericSubstitutions,
            arguments: argumentsData,
            returnType: returnTypeName,
            errorType: errorTypeName
        )
        return try payload.encodeBinary()
    }
}

public struct ClusterInvocationDecoder: Distributed::DistributedTargetInvocationDecoder, Sendable {
    public typealias SerializationRequirement = Codable

    public let payload: InvocationPayload
    public var nextArgumentIndex: Int = 0

    public init(payload: InvocationPayload) {
        self.payload = payload
    }

    public init(data: Data) throws(ClusterError) {
        self.payload = try InvocationPayload.decodeBinary(from: data)
    }

    public mutating func decodeGenericSubstitutions() throws -> [any Any.Type] {
        var resolved: [any Any.Type] = []
        resolved.reserveCapacity(payload.genericSubstitutions.count)
        for name in payload.genericSubstitutions {
            guard !name.isEmpty, let type = _typeByName(name) else {
                throw ClusterError.decodingFailed("Unable to resolve generic substitution type '\(name)'")
            }
            resolved.append(type)
        }
        return resolved
    }

    public mutating func decodeNextArgument<Argument>() throws -> Argument where Argument: Codable {
        guard nextArgumentIndex < payload.arguments.count else {
            throw ClusterError.decodingFailed("No more arguments available at index \(nextArgumentIndex)")
        }
        let data = payload.arguments[nextArgumentIndex]
        nextArgumentIndex += 1
        return try decodeCodableValue(Argument.self, from: data)
    }

    public mutating func decodeErrorType() throws -> (any Any.Type)? {
        guard !payload.errorType.isEmpty else { return nil }
        return _typeByName(payload.errorType)
    }

    public mutating func decodeReturnType() throws -> (any Any.Type)? {
        guard !payload.returnType.isEmpty else { return nil }
        return _typeByName(payload.returnType)
    }
}

public struct ClusterResultHandler: Distributed::DistributedTargetInvocationResultHandler, Sendable {
    public typealias SerializationRequirement = Codable

    public let correlationID: UInt64
    internal let replySink: @Sendable (WireHeader.FrameType, UInt64, Data) async -> Void

    public init(
        correlationID: UInt64,
        replySink: @escaping @Sendable (WireHeader.FrameType, UInt64, Data) async -> Void
    ) {
        self.correlationID = correlationID
        self.replySink = replySink
    }

    public func onReturn<Success>(value: Success) async throws where Success: Codable {
        do {
            let data = try encodeCodableValue(value)
            await replySink(.reply, correlationID, data)
        } catch {
            let envelope = ErrorEnvelope(clusterError: error, errorTypeName: nil, errorData: nil, message: "\(error)")
            let data = (try? encodeCodableValue(envelope)) ?? Data()
            await replySink(.error, correlationID, data)
        }
    }

    public func onReturnVoid() async throws {
        await replySink(.reply, correlationID, Data())
    }

    public func onThrow<Err>(error: Err) async throws where Err: Error {
        let envelope: ErrorEnvelope
        if let clusterError = error as? ClusterError {
            envelope = ErrorEnvelope(clusterError: clusterError, errorTypeName: nil, errorData: nil, message: "\(clusterError)")
        } else if let codableError = error as? any (Error & Codable),
                  let typeName = _mangledTypeName(type(of: codableError)),
                  let encoded = try? encodeAnyCodable(codableError) {
            envelope = ErrorEnvelope(
                clusterError: nil,
                errorTypeName: typeName,
                errorData: encoded,
                message: String(describing: error)
            )
        } else {
            envelope = ErrorEnvelope(clusterError: nil, errorTypeName: nil, errorData: nil, message: String(describing: error))
        }
        let data = (try? encodeCodableValue(envelope)) ?? Data()
        await replySink(.error, correlationID, data)
    }
}
