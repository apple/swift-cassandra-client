//===----------------------------------------------------------------------===//
//
// This source file is part of the Swift Cassandra Client open source project
//
// Copyright (c) 2022 Apple Inc. and the Swift Cassandra Client project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of Swift Cassandra Client project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

@_implementationOnly import CDataStaxDriver

extension CassandraClient {
  /// Possible ``CassandraClient`` errors.
  public struct Error: Swift.Error, Equatable, CustomStringConvertible {
    private enum Code: Equatable {
      case rowsExhausted
      case disconnected

      // lib errors
      case badParams(String)
      case noStreams(String)
      case unableToInit(String)
      case messageEncode(String)
      case hostResolution(String)
      case unexpectedResponse(String)
      case requestQueueFull(String)
      case noAvailableIOThread(String)
      case writeError(String)
      case noHostsAvailable(String)
      case indexOutOfBounds(String)
      case invalidItemCount(String)
      case invalidValueType(String)
      case requestTimedOut(String)
      case unableToSetKeyspace(String)
      case callbackAlreadySet(String)
      case invalidStatementType(String)
      case nameDoesNotExist(String)
      case unableToDetermineProtocol(String)
      case nullValue(String)
      case notImplemented(String)
      case unableToConnect(String)
      case unableToClose(String)
      case noPagingState(String)
      case parameterUnset(String)
      case invalidErrorResultType(String)
      case invalidFutureType(String)
      case internalError(String)
      case invalidCustomType(String)
      case invalidData(String)
      case notEnoughData(String)
      case invalidState(String)
      case noCustomPayload(String)
      // server errors
      case serverError(String)
      case protocolError(String)
      case badCredentials(String)
      case serverUnavailable(String)
      case serverOverloaded(String)
      case serverBootstrapping(String)
      case truncateError(String)
      case writeTimeout(String)
      case readTimeout(String)
      case readFailure(String)
      case functionFailure(String)
      case writeFailure(String)
      case syntaxError(String)
      case unauthorized(String)
      case invalidQuery(String)
      case serverConfigError(String)
      case alreadyExists(String)
      case unprepared(String)
      // ssl errors
      case sslInvalidCert(String)
      case sslInvalidPrivateKey(String)
      case sslNoPeerCert(String)
      case sslInvalidPeerCert(String)
      case sslIdentityMismatch(String)
      case sslProtocolError(String)
      case sslClosed(String)
      // catch all
      case other(code: UInt32, description: String?)
    }

    private var code: Code

    private init(code: Code) {
      self.code = code
    }

    init(_ future: OpaquePointer) {
      let errorCode = cass_future_error_code(future)
      var messageRaw: UnsafePointer<CChar>?
      var messageLength = Int()
      cass_future_error_message(future, &messageRaw, &messageLength)
      let message = messageRaw.map { String(cString: $0) }
      self.init(errorCode, message: message)
    }

    init(_ error: CassError, message: String? = .none) {
      let message = message ?? ""
      switch error {
      case CASS_ERROR_LIB_BAD_PARAMS:
        self = .badParams(message)
      case CASS_ERROR_LIB_NO_STREAMS:
        self = .noStreams(message)
      case CASS_ERROR_LIB_UNABLE_TO_INIT:
        self = .unableToInit(message)
      case CASS_ERROR_LIB_MESSAGE_ENCODE:
        self = .messageEncode(message)
      case CASS_ERROR_LIB_HOST_RESOLUTION:
        self = .hostResolution(message)
      case CASS_ERROR_LIB_UNEXPECTED_RESPONSE:
        self = .unexpectedResponse(message)
      case CASS_ERROR_LIB_REQUEST_QUEUE_FULL:
        self = .requestQueueFull(message)
      case CASS_ERROR_LIB_NO_AVAILABLE_IO_THREAD:
        self = .noAvailableIOThread(message)
      case CASS_ERROR_LIB_WRITE_ERROR:
        self = .writeError(message)
      case CASS_ERROR_LIB_NO_HOSTS_AVAILABLE:
        self = .noHostsAvailable(message)
      case CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS:
        self = .indexOutOfBounds(message)
      case CASS_ERROR_LIB_INVALID_ITEM_COUNT:
        self = .invalidItemCount(message)
      case CASS_ERROR_LIB_INVALID_VALUE_TYPE:
        self = .invalidValueType(message)
      case CASS_ERROR_LIB_REQUEST_TIMED_OUT:
        self = .requestTimedOut(message)
      case CASS_ERROR_LIB_UNABLE_TO_SET_KEYSPACE:
        self = .unableToSetKeyspace(message)
      case CASS_ERROR_LIB_CALLBACK_ALREADY_SET:
        self = .callbackAlreadySet(message)
      case CASS_ERROR_LIB_INVALID_STATEMENT_TYPE:
        self = .invalidStatementType(message)
      case CASS_ERROR_LIB_NAME_DOES_NOT_EXIST:
        self = .nameDoesNotExist(message)
      case CASS_ERROR_LIB_UNABLE_TO_DETERMINE_PROTOCOL:
        self = .unableToDetermineProtocol(message)
      case CASS_ERROR_LIB_NULL_VALUE:
        self = .nullValue(message)
      case CASS_ERROR_LIB_NOT_IMPLEMENTED:
        self = .notImplemented(message)
      case CASS_ERROR_LIB_UNABLE_TO_CONNECT:
        self = .unableToConnect(message)
      case CASS_ERROR_LIB_UNABLE_TO_CLOSE:
        self = .unableToClose(message)
      case CASS_ERROR_LIB_NO_PAGING_STATE:
        self = .noPagingState(message)
      case CASS_ERROR_LIB_PARAMETER_UNSET:
        self = .parameterUnset(message)
      case CASS_ERROR_LIB_INVALID_ERROR_RESULT_TYPE:
        self = .invalidErrorResultType(message)
      case CASS_ERROR_LIB_INVALID_FUTURE_TYPE:
        self = .invalidFutureType(message)
      case CASS_ERROR_LIB_INTERNAL_ERROR:
        self = .internalError(message)
      case CASS_ERROR_LIB_INVALID_CUSTOM_TYPE:
        self = .invalidCustomType(message)
      case CASS_ERROR_LIB_INVALID_DATA:
        self = .invalidData(message)
      case CASS_ERROR_LIB_NOT_ENOUGH_DATA:
        self = .notEnoughData(message)
      case CASS_ERROR_LIB_INVALID_STATE:
        self = .invalidState(message)
      case CASS_ERROR_LIB_NO_CUSTOM_PAYLOAD:
        self = .noCustomPayload(message)
      case CASS_ERROR_SERVER_SERVER_ERROR:
        self = .serverError(message)
      case CASS_ERROR_SERVER_PROTOCOL_ERROR:
        self = .protocolError(message)
      case CASS_ERROR_SERVER_BAD_CREDENTIALS:
        self = .badCredentials(message)
      case CASS_ERROR_SERVER_UNAVAILABLE:
        self = .serverUnavailable(message)
      case CASS_ERROR_SERVER_OVERLOADED:
        self = .serverOverloaded(message)
      case CASS_ERROR_SERVER_IS_BOOTSTRAPPING:
        self = .serverBootstrapping(message)
      case CASS_ERROR_SERVER_TRUNCATE_ERROR:
        self = .truncateError(message)
      case CASS_ERROR_SERVER_WRITE_TIMEOUT:
        self = .writeTimeout(message)
      case CASS_ERROR_SERVER_READ_TIMEOUT:
        self = .readTimeout(message)
      case CASS_ERROR_SERVER_READ_FAILURE:
        self = .readFailure(message)
      case CASS_ERROR_SERVER_FUNCTION_FAILURE:
        self = .functionFailure(message)
      case CASS_ERROR_SERVER_WRITE_FAILURE:
        self = .writeFailure(message)
      case CASS_ERROR_SERVER_SYNTAX_ERROR:
        self = .syntaxError(message)
      case CASS_ERROR_SERVER_UNAUTHORIZED:
        self = .unauthorized(message)
      case CASS_ERROR_SERVER_INVALID_QUERY:
        self = .invalidQuery(message)
      case CASS_ERROR_SERVER_CONFIG_ERROR:
        self = .serverConfigError(message)
      case CASS_ERROR_SERVER_ALREADY_EXISTS:
        self = .alreadyExists(message)
      case CASS_ERROR_SERVER_UNPREPARED:
        self = .unprepared(message)
      case CASS_ERROR_SSL_INVALID_CERT:
        self = .sslInvalidCert(message)
      case CASS_ERROR_SSL_INVALID_PRIVATE_KEY:
        self = .sslInvalidPrivateKey(message)
      case CASS_ERROR_SSL_NO_PEER_CERT:
        self = .sslNoPeerCert(message)
      case CASS_ERROR_SSL_INVALID_PEER_CERT:
        self = .sslInvalidPeerCert(message)
      case CASS_ERROR_SSL_IDENTITY_MISMATCH:
        self = .sslIdentityMismatch(message)
      case CASS_ERROR_SSL_PROTOCOL_ERROR:
        self = .sslProtocolError(message)
      case CASS_ERROR_SSL_CLOSED:
        self = .sslClosed(message)
      default:
        self = .other(code: error.rawValue, description: message)
      }
    }

    public var description: String {
      switch self.code {
      case .rowsExhausted:
        return "Rows exhausted"
      case .disconnected:
        return "Disconnected"
      case .badParams(let description):
        return "Bad parameters: \(description)"
      case .noStreams(let description):
        return "No streams available: \(description)"
      case .unableToInit(let description):
        return "Unable to initialize: \(description)"
      case .messageEncode(let description):
        return "Unable to encode message: \(description)"
      case .hostResolution(let description):
        return "Unable to resolve host: \(description)"
      case .unexpectedResponse(let description):
        return "Unexpected response from server: \(description)"
      case .requestQueueFull(let description):
        return "The request queue is full: \(description)"
      case .noAvailableIOThread(let description):
        return "No available IO threads: \(description)"
      case .writeError(let description):
        return "Write error: \(description)"
      case .noHostsAvailable(let description):
        return "No hosts available: \(description)"
      case .indexOutOfBounds(let description):
        return "Index out of bounds: \(description)"
      case .invalidItemCount(let description):
        return "Invalid item count: \(description)"
      case .invalidValueType(let description):
        return "Invalid value type: \(description)"
      case .requestTimedOut(let description):
        return "Request timed out: \(description)"
      case .unableToSetKeyspace(let description):
        return "Unable to set keyspace: \(description)"
      case .callbackAlreadySet(let description):
        return "Callback already set: \(description)"
      case .invalidStatementType(let description):
        return "Invalid statement type: \(description)"
      case .nameDoesNotExist(let description):
        return "No value or column for name: \(description)"
      case .unableToDetermineProtocol(let description):
        return "Unable to find supported protocol version: \(description)"
      case .nullValue(let description):
        return "NULL value specified: \(description)"
      case .notImplemented(let description):
        return "Not implemented: \(description)"
      case .unableToConnect(let description):
        return "Unable to connect: \(description)"
      case .unableToClose(let description):
        return "Unable to close: \(description)"
      case .noPagingState(let description):
        return "No paging state: \(description)"
      case .parameterUnset(let description):
        return "Parameter unset: \(description)"
      case .invalidErrorResultType(let description):
        return "Invalid error result type: \(description)"
      case .invalidFutureType(let description):
        return "Invalid future type: \(description)"
      case .internalError(let description):
        return "Internal error: \(description)"
      case .invalidCustomType(let description):
        return "Invalid custom type: \(description)"
      case .invalidData(let description):
        return "Invalid data: \(description)"
      case .notEnoughData(let description):
        return "Not enough data: \(description)"
      case .invalidState(let description):
        return "Invalid state: \(description)"
      case .noCustomPayload(let description):
        return "No custom payload: \(description)"
      case .serverError(let description):
        return "Server error: \(description)"
      case .protocolError(let description):
        return "Protocol error: \(description)"
      case .badCredentials(let description):
        return "Bad credentials: \(description)"
      case .serverUnavailable(let description):
        return "Server unavailable: \(description)"
      case .serverOverloaded(let description):
        return "Server overloaded: \(description)"
      case .serverBootstrapping(let description):
        return "Server bootstrapping: \(description)"
      case .truncateError(let description):
        return "Truncate error: \(description)"
      case .writeTimeout(let description):
        return "Write timeout: \(description)"
      case .readTimeout(let description):
        return "Read timeout: \(description)"
      case .readFailure(let description):
        return "Read failure: \(description)"
      case .functionFailure(let description):
        return "Function failure: \(description)"
      case .writeFailure(let description):
        return "Write failure: \(description)"
      case .syntaxError(let description):
        return "Syntax error: \(description)"
      case .unauthorized(let description):
        return "Unauthorized: \(description)"
      case .invalidQuery(let description):
        return "Invalid query: \(description)"
      case .serverConfigError(let description):
        return "Server configuration error: \(description)"
      case .alreadyExists(let description):
        return "Already exists: \(description)"
      case .unprepared(let description):
        return "Unprepared: \(description)"
      case .sslInvalidCert(let description):
        return "Unable to load certificate: \(description)"
      case .sslInvalidPrivateKey(let description):
        return "Unable to load private key: \(description)"
      case .sslNoPeerCert(let description):
        return "No peer certificate: \(description)"
      case .sslInvalidPeerCert(let description):
        return "Invalid peer certificate: \(description)"
      case .sslIdentityMismatch(let description):
        return "Certificate does not match host or IP address: \(description)"
      case .sslProtocolError(let description):
        return "Protocol error: \(description)"
      case .sslClosed(let description):
        return "Connection closed: \(description)"
      case .other(let code, let description):
        return "Other (\(code)): \(description ?? "unknown")"
      }
    }

    public var shortDescription: String {
      switch self.code {
      case .rowsExhausted:
        return "Rows exhausted"
      case .disconnected:
        return "Disconnected"
      case .badParams:
        return "Bad parameters"
      case .noStreams:
        return "No streams available"
      case .unableToInit:
        return "Unable to initialize"
      case .messageEncode:
        return "Unable to encode message"
      case .hostResolution:
        return "Unable to resolve host"
      case .unexpectedResponse:
        return "Unexpected response from server"
      case .requestQueueFull:
        return "The request queue is full"
      case .noAvailableIOThread:
        return "No available IO threads"
      case .writeError:
        return "Write error"
      case .noHostsAvailable:
        return "No hosts available"
      case .indexOutOfBounds:
        return "Index out of bounds"
      case .invalidItemCount:
        return "Invalid item count"
      case .invalidValueType:
        return "Invalid value type"
      case .requestTimedOut:
        return "Request timed out"
      case .unableToSetKeyspace:
        return "Unable to set keyspace"
      case .callbackAlreadySet:
        return "Callback already set"
      case .invalidStatementType:
        return "Invalid statement type"
      case .nameDoesNotExist:
        return "No value or column for name"
      case .unableToDetermineProtocol:
        return "Unable to find supported protocol version"
      case .nullValue:
        return "NULL value specified"
      case .notImplemented:
        return "Not implemented"
      case .unableToConnect:
        return "Unable to connect"
      case .unableToClose:
        return "Unable to close"
      case .noPagingState:
        return "No paging state"
      case .parameterUnset:
        return "Parameter unset"
      case .invalidErrorResultType:
        return "Invalid error result type"
      case .invalidFutureType:
        return "Invalid future type"
      case .internalError:
        return "Internal error"
      case .invalidCustomType:
        return "Invalid custom type"
      case .invalidData:
        return "Invalid data"
      case .notEnoughData:
        return "Not enough data"
      case .invalidState:
        return "Invalid state"
      case .noCustomPayload:
        return "No custom payload"
      case .serverError:
        return "Server error"
      case .protocolError:
        return "Protocol error"
      case .badCredentials:
        return "Bad credentials"
      case .serverUnavailable:
        return "Server unavailable"
      case .serverOverloaded:
        return "Server overloaded"
      case .serverBootstrapping:
        return "Server bootstrapping"
      case .truncateError:
        return "Truncate error"
      case .writeTimeout:
        return "Write timeout"
      case .readTimeout:
        return "Read timeout"
      case .readFailure:
        return "Read failure"
      case .functionFailure:
        return "Function failure"
      case .writeFailure:
        return "Write failure"
      case .syntaxError:
        return "Syntax error"
      case .unauthorized:
        return "Unauthorized"
      case .invalidQuery:
        return "Invalid query"
      case .serverConfigError:
        return "Server configuration error"
      case .alreadyExists:
        return "Already exists"
      case .unprepared:
        return "Unprepared"
      case .sslInvalidCert:
        return "Unable to load certificate"
      case .sslInvalidPrivateKey:
        return "Unable to load private key"
      case .sslNoPeerCert:
        return "No peer certificate"
      case .sslInvalidPeerCert:
        return "Invalid peer certificate"
      case .sslIdentityMismatch:
        return "Certificate does not match host or IP address"
      case .sslProtocolError:
        return "Protocol error"
      case .sslClosed:
        return "Connection closed"
      case .other(let code, _):
        return "Other (\(code))"
      }
    }

    /// All rows for a query result have been consumed.
    public static let rowsExhausted = Error(code: .rowsExhausted)

    /// Unexpected client connection state.
    public static let disconnected = Error(code: .disconnected)

    // lib errors
    public static func badParams(_ description: String) -> Error {
      .init(code: .badParams(description))
    }

    public static func noStreams(_ description: String) -> Error {
      .init(code: .noStreams(description))
    }

    public static func unableToInit(_ description: String) -> Error {
      .init(code: .unableToInit(description))
    }

    public static func messageEncode(_ description: String) -> Error {
      .init(code: .messageEncode(description))
    }

    public static func hostResolution(_ description: String) -> Error {
      .init(code: .hostResolution(description))
    }

    public static func unexpectedResponse(_ description: String) -> Error {
      .init(code: .unexpectedResponse(description))
    }

    public static func requestQueueFull(_ description: String) -> Error {
      .init(code: .requestQueueFull(description))
    }

    public static func noAvailableIOThread(_ description: String) -> Error {
      .init(code: .noAvailableIOThread(description))
    }

    public static func writeError(_ description: String) -> Error {
      .init(code: .writeError(description))
    }

    public static func noHostsAvailable(_ description: String) -> Error {
      .init(code: .noHostsAvailable(description))
    }

    public static func indexOutOfBounds(_ description: String) -> Error {
      .init(code: .indexOutOfBounds(description))
    }

    public static func invalidItemCount(_ description: String) -> Error {
      .init(code: .invalidItemCount(description))
    }

    public static func invalidValueType(_ description: String) -> Error {
      .init(code: .invalidValueType(description))
    }

    public static func requestTimedOut(_ description: String) -> Error {
      .init(code: .requestTimedOut(description))
    }

    public static func unableToSetKeyspace(_ description: String) -> Error {
      .init(code: .unableToSetKeyspace(description))
    }

    public static func callbackAlreadySet(_ description: String) -> Error {
      .init(code: .callbackAlreadySet(description))
    }

    public static func invalidStatementType(_ description: String) -> Error {
      .init(code: .invalidStatementType(description))
    }

    public static func nameDoesNotExist(_ description: String) -> Error {
      .init(code: .nameDoesNotExist(description))
    }

    public static func unableToDetermineProtocol(_ description: String) -> Error {
      .init(code: .unableToDetermineProtocol(description))
    }

    public static func nullValue(_ description: String) -> Error {
      .init(code: .nullValue(description))
    }

    public static func notImplemented(_ description: String) -> Error {
      .init(code: .notImplemented(description))
    }

    public static func unableToConnect(_ description: String) -> Error {
      .init(code: .unableToConnect(description))
    }

    public static func unableToClose(_ description: String) -> Error {
      .init(code: .unableToClose(description))
    }

    public static func noPagingState(_ description: String) -> Error {
      .init(code: .noPagingState(description))
    }

    public static func parameterUnset(_ description: String) -> Error {
      .init(code: .parameterUnset(description))
    }

    public static func invalidErrorResultType(_ description: String) -> Error {
      .init(code: .invalidErrorResultType(description))
    }

    public static func invalidFutureType(_ description: String) -> Error {
      .init(code: .invalidFutureType(description))
    }

    public static func internalError(_ description: String) -> Error {
      .init(code: .internalError(description))
    }

    public static func invalidCustomType(_ description: String) -> Error {
      .init(code: .invalidCustomType(description))
    }

    public static func invalidData(_ description: String) -> Error {
      .init(code: .invalidData(description))
    }

    public static func notEnoughData(_ description: String) -> Error {
      .init(code: .notEnoughData(description))
    }

    public static func invalidState(_ description: String) -> Error {
      .init(code: .invalidState(description))
    }

    public static func noCustomPayload(_ description: String) -> Error {
      .init(code: .noCustomPayload(description))
    }

    // server errors
    public static func serverError(_ description: String) -> Error {
      .init(code: .serverError(description))
    }

    public static func protocolError(_ description: String) -> Error {
      .init(code: .protocolError(description))
    }

    public static func badCredentials(_ description: String) -> Error {
      .init(code: .badCredentials(description))
    }

    public static func serverUnavailable(_ description: String) -> Error {
      .init(code: .serverUnavailable(description))
    }

    public static func serverOverloaded(_ description: String) -> Error {
      .init(code: .serverOverloaded(description))
    }

    public static func serverBootstrapping(_ description: String) -> Error {
      .init(code: .serverBootstrapping(description))
    }

    public static func truncateError(_ description: String) -> Error {
      .init(code: .truncateError(description))
    }

    public static func writeTimeout(_ description: String) -> Error {
      .init(code: .writeTimeout(description))
    }

    public static func readTimeout(_ description: String) -> Error {
      .init(code: .readTimeout(description))
    }

    public static func readFailure(_ description: String) -> Error {
      .init(code: .readFailure(description))
    }

    public static func functionFailure(_ description: String) -> Error {
      .init(code: .functionFailure(description))
    }

    public static func writeFailure(_ description: String) -> Error {
      .init(code: .writeFailure(description))
    }

    public static func syntaxError(_ description: String) -> Error {
      .init(code: .syntaxError(description))
    }

    public static func unauthorized(_ description: String) -> Error {
      .init(code: .unauthorized(description))
    }

    public static func invalidQuery(_ description: String) -> Error {
      .init(code: .invalidQuery(description))
    }

    public static func serverConfigError(_ description: String) -> Error {
      .init(code: .serverConfigError(description))
    }

    public static func alreadyExists(_ description: String) -> Error {
      .init(code: .alreadyExists(description))
    }

    public static func unprepared(_ description: String) -> Error {
      .init(code: .unprepared(description))
    }

    // ssl errors
    public static func sslInvalidCert(_ description: String) -> Error {
      .init(code: .sslInvalidCert(description))
    }

    public static func sslInvalidPrivateKey(_ description: String) -> Error {
      .init(code: .sslInvalidPrivateKey(description))
    }

    public static func sslNoPeerCert(_ description: String) -> Error {
      .init(code: .sslNoPeerCert(description))
    }

    public static func sslInvalidPeerCert(_ description: String) -> Error {
      .init(code: .sslInvalidPeerCert(description))
    }

    public static func sslIdentityMismatch(_ description: String) -> Error {
      .init(code: .sslIdentityMismatch(description))
    }

    public static func sslProtocolError(_ description: String) -> Error {
      .init(code: .sslProtocolError(description))
    }

    public static func sslClosed(_ description: String) -> Error {
      .init(code: .sslClosed(description))
    }

    public static func other(code: UInt32, description: String?) -> Error {
      .init(code: .other(code: code, description: description))
    }
  }
}

extension CassandraClient {
  public struct ConfigurationError: Swift.Error, CustomStringConvertible {
    public let message: String

    public var description: String {
      "Configuration error: \(self.message)"
    }
  }
}
