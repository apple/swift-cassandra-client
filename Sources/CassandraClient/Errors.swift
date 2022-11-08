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

public extension CassandraClient {
    /// Possible ``CassandraClient`` errors.
    struct Error: Swift.Error, Equatable, CustomStringConvertible {
        private enum Code: Equatable {
            case rowsExhausted
            case disconnected
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
            default:
                self = .other(code: error.rawValue, description: message)
            }

            // FIXME: map rest of errors
            /*
             XX(CASS_ERROR_SOURCE_LIB, CASS_ERROR_LIB_BAD_PARAMS, 1, "Bad parameters") \
             XX(CASS_ERROR_SOURCE_LIB, CASS_ERROR_LIB_NO_STREAMS, 2, "No streams available") \
             XX(CASS_ERROR_SOURCE_LIB, CASS_ERROR_LIB_UNABLE_TO_INIT, 3, "Unable to initialize") \
             XX(CASS_ERROR_SOURCE_LIB, CASS_ERROR_LIB_MESSAGE_ENCODE, 4, "Unable to encode message") \
             XX(CASS_ERROR_SOURCE_LIB, CASS_ERROR_LIB_HOST_RESOLUTION, 5, "Unable to resolve host") \
             XX(CASS_ERROR_SOURCE_LIB, CASS_ERROR_LIB_UNEXPECTED_RESPONSE, 6, "Unexpected response from server") \
             XX(CASS_ERROR_SOURCE_LIB, CASS_ERROR_LIB_REQUEST_QUEUE_FULL, 7, "The request queue is full") \
             XX(CASS_ERROR_SOURCE_LIB, CASS_ERROR_LIB_NO_AVAILABLE_IO_THREAD, 8, "No available IO threads") \
             XX(CASS_ERROR_SOURCE_LIB, CASS_ERROR_LIB_WRITE_ERROR, 9, "Write error") \
             XX(CASS_ERROR_SOURCE_LIB, CASS_ERROR_LIB_NO_HOSTS_AVAILABLE, 10, "No hosts available") \
             XX(CASS_ERROR_SOURCE_LIB, CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS, 11, "Index out of bounds") \
             XX(CASS_ERROR_SOURCE_LIB, CASS_ERROR_LIB_INVALID_ITEM_COUNT, 12, "Invalid item count") \
             XX(CASS_ERROR_SOURCE_LIB, CASS_ERROR_LIB_INVALID_VALUE_TYPE, 13, "Invalid value type") \
             XX(CASS_ERROR_SOURCE_LIB, CASS_ERROR_LIB_REQUEST_TIMED_OUT, 14, "Request timed out") \
             XX(CASS_ERROR_SOURCE_LIB, CASS_ERROR_LIB_UNABLE_TO_SET_KEYSPACE, 15, "Unable to set keyspace") \
             XX(CASS_ERROR_SOURCE_LIB, CASS_ERROR_LIB_CALLBACK_ALREADY_SET, 16, "Callback already set") \
             XX(CASS_ERROR_SOURCE_LIB, CASS_ERROR_LIB_INVALID_STATEMENT_TYPE, 17, "Invalid statement type") \
             XX(CASS_ERROR_SOURCE_LIB, CASS_ERROR_LIB_NAME_DOES_NOT_EXIST, 18, "No value or column for name") \
             XX(CASS_ERROR_SOURCE_LIB, CASS_ERROR_LIB_UNABLE_TO_DETERMINE_PROTOCOL, 19, "Unable to find supported protocol version") \
             XX(CASS_ERROR_SOURCE_LIB, CASS_ERROR_LIB_NULL_VALUE, 20, "NULL value specified") \
             XX(CASS_ERROR_SOURCE_LIB, CASS_ERROR_LIB_NOT_IMPLEMENTED, 21, "Not implemented") \
             XX(CASS_ERROR_SOURCE_LIB, CASS_ERROR_LIB_UNABLE_TO_CONNECT, 22, "Unable to connect") \
             XX(CASS_ERROR_SOURCE_LIB, CASS_ERROR_LIB_UNABLE_TO_CLOSE, 23, "Unable to close") \
             XX(CASS_ERROR_SOURCE_LIB, CASS_ERROR_LIB_NO_PAGING_STATE, 24, "No paging state") \
             XX(CASS_ERROR_SOURCE_LIB, CASS_ERROR_LIB_PARAMETER_UNSET, 25, "Parameter unset") \
             XX(CASS_ERROR_SOURCE_LIB, CASS_ERROR_LIB_INVALID_ERROR_RESULT_TYPE, 26, "Invalid error result type") \
             XX(CASS_ERROR_SOURCE_LIB, CASS_ERROR_LIB_INVALID_FUTURE_TYPE, 27, "Invalid future type") \
             XX(CASS_ERROR_SOURCE_LIB, CASS_ERROR_LIB_INTERNAL_ERROR, 28, "Internal error") \
             XX(CASS_ERROR_SOURCE_LIB, CASS_ERROR_LIB_INVALID_CUSTOM_TYPE, 29, "Invalid custom type") \
             XX(CASS_ERROR_SOURCE_LIB, CASS_ERROR_LIB_INVALID_DATA, 30, "Invalid data") \
             XX(CASS_ERROR_SOURCE_LIB, CASS_ERROR_LIB_NOT_ENOUGH_DATA, 31, "Not enough data") \
             XX(CASS_ERROR_SOURCE_LIB, CASS_ERROR_LIB_INVALID_STATE, 32, "Invalid state") \
             XX(CASS_ERROR_SOURCE_LIB, CASS_ERROR_LIB_NO_CUSTOM_PAYLOAD, 33, "No custom payload") \

             XX(CASS_ERROR_SOURCE_SERVER, CASS_ERROR_SERVER_SERVER_ERROR, 0x0000, "Server error") \
             XX(CASS_ERROR_SOURCE_SERVER, CASS_ERROR_SERVER_PROTOCOL_ERROR, 0x000A, "Protocol error") \
             XX(CASS_ERROR_SOURCE_SERVER, CASS_ERROR_SERVER_BAD_CREDENTIALS, 0x0100, "Bad credentials") \
             XX(CASS_ERROR_SOURCE_SERVER, CASS_ERROR_SERVER_UNAVAILABLE, 0x1000, "Unavailable") \
             XX(CASS_ERROR_SOURCE_SERVER, CASS_ERROR_SERVER_OVERLOADED, 0x1001, "Overloaded") \
             XX(CASS_ERROR_SOURCE_SERVER, CASS_ERROR_SERVER_IS_BOOTSTRAPPING, 0x1002, "Is bootstrapping") \
             XX(CASS_ERROR_SOURCE_SERVER, CASS_ERROR_SERVER_TRUNCATE_ERROR, 0x1003, "Truncate error") \
             XX(CASS_ERROR_SOURCE_SERVER, CASS_ERROR_SERVER_WRITE_TIMEOUT, 0x1100, "Write timeout") \
             XX(CASS_ERROR_SOURCE_SERVER, CASS_ERROR_SERVER_READ_TIMEOUT, 0x1200, "Read timeout") \
             XX(CASS_ERROR_SOURCE_SERVER, CASS_ERROR_SERVER_READ_FAILURE, 0x1300, "Read failure") \
             XX(CASS_ERROR_SOURCE_SERVER, CASS_ERROR_SERVER_FUNCTION_FAILURE, 0x1400, "Function failure") \
             XX(CASS_ERROR_SOURCE_SERVER, CASS_ERROR_SERVER_WRITE_FAILURE, 0x1500, "Write failure") \
             XX(CASS_ERROR_SOURCE_SERVER, CASS_ERROR_SERVER_SYNTAX_ERROR, 0x2000, "Syntax error") \
             XX(CASS_ERROR_SOURCE_SERVER, CASS_ERROR_SERVER_UNAUTHORIZED, 0x2100, "Unauthorized") \
             XX(CASS_ERROR_SOURCE_SERVER, CASS_ERROR_SERVER_INVALID_QUERY, 0x2200, "Invalid query") \
             XX(CASS_ERROR_SOURCE_SERVER, CASS_ERROR_SERVER_CONFIG_ERROR, 0x2300, "Configuration error") \
             XX(CASS_ERROR_SOURCE_SERVER, CASS_ERROR_SERVER_ALREADY_EXISTS, 0x2400, "Already exists") \
             XX(CASS_ERROR_SOURCE_SERVER, CASS_ERROR_SERVER_UNPREPARED, 0x2500, "Unprepared") \

             XX(CASS_ERROR_SOURCE_SSL, CASS_ERROR_SSL_INVALID_CERT, 1, "Unable to load certificate") \
             XX(CASS_ERROR_SOURCE_SSL, CASS_ERROR_SSL_INVALID_PRIVATE_KEY, 2, "Unable to load private key") \
             XX(CASS_ERROR_SOURCE_SSL, CASS_ERROR_SSL_NO_PEER_CERT, 3, "No peer certificate")  \
             XX(CASS_ERROR_SOURCE_SSL, CASS_ERROR_SSL_INVALID_PEER_CERT, 4, "Invalid peer certificate") \
             XX(CASS_ERROR_SOURCE_SSL, CASS_ERROR_SSL_IDENTITY_MISMATCH, 5, "Certificate does not match host or IP address") \
             XX(CASS_ERROR_SOURCE_SSL, CASS_ERROR_SSL_PROTOCOL_ERROR, 6, "Protocol error")
             */
        }

        public var description: String {
            switch self.code {
            case .rowsExhausted:
                return "Rows exhausted"
            case .disconnected:
                return "Disconnected"
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
            case .other(let code, _):
                return "Other (\(code))"
            }
        }

        /// All rows for a query result have been consumed.
        public static let rowsExhausted = Error(code: .rowsExhausted)

        /// Unexpected client connection state.
        public static let disconnected = Error(code: .disconnected)

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

        public static func other(code: UInt32, description: String?) -> Error {
            .init(code: .other(code: code, description: description))
        }
    }
}

public extension CassandraClient {
    struct ConfigurationError: Swift.Error, CustomStringConvertible {
        public let message: String

        public var description: String {
            "Configuration error: \(self.message)"
        }
    }
}
