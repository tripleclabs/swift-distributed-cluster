import Distributed
import Foundation

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
}

internal struct RemoteCallEnvelope: Codable, Sendable {
    var targetID: ClusterActorID
    var invocationData: Data
}

internal struct ErrorEnvelope: Codable, Sendable {
    var clusterError: ClusterError?
    var message: String
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
        genericSubstitutions.append(String(reflecting: type))
    }

    public mutating func recordArgument<Value>(_ argument: Distributed::RemoteCallArgument<Value>) throws where Value: Codable {
        argumentsData.append(try encodeCodableValue(argument.value))
    }

    public mutating func recordReturnType<R>(_ type: R.Type) throws where R: Codable {
        returnTypeName = String(reflecting: type)
    }

    public mutating func recordErrorType<E>(_ type: E.Type) throws where E: Error {
        errorTypeName = String(reflecting: type)
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
        return try encodeCodableValue(payload)
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
        self.payload = try decodeCodableValue(InvocationPayload.self, from: data)
    }

    public mutating func decodeGenericSubstitutions() throws -> [any Any.Type] {
        []
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
        nil
    }

    public mutating func decodeReturnType() throws -> (any Any.Type)? {
        nil
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
            let envelope = ErrorEnvelope(clusterError: error, message: "\(error)")
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
            envelope = ErrorEnvelope(clusterError: clusterError, message: "\(clusterError)")
        } else {
            envelope = ErrorEnvelope(clusterError: nil, message: String(describing: error))
        }
        let data = (try? encodeCodableValue(envelope)) ?? Data()
        await replySink(.error, correlationID, data)
    }
}
