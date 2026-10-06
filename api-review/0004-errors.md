# Errors

## Overview

- **Area:** Errors — 83 of the module's public symbols.
- **Note:** This PR exists to collect API review feedback ahead of 1.0. It will be closed rather than merged; anything it settles lands as its own change.
- **Generated from:** `Sources/CassandraClient` at `e60f45a`, with Apple Swift version 6.3.1 (swiftlang-6.3.1.1.2 clang-2100.0.123.102). Derived, not maintained by hand — see `api-review/README.md` to regenerate.

Inherited protocol members are listed without their standard-library documentation, grouped under the protocol that declared each. Members the symbol graph does not emit are marked `[reconstructed]`.

## Public API

````swift
extension CassandraClient {
    public struct ConfigurationError : CustomStringConvertible, Error, LocalizedError, Sendable {
        public var description: String { get }
        /// Backs `localizedDescription`, which otherwise reports a generic Foundation message.
        public var errorDescription: String? { get }
        public let message: String

        // [inherited from Error] 1 member
        @available(macOS 10.10, watchOS 2.0, iOS 8.0, tvOS 9.0, *)
        var localizedDescription: String { get }

        // [inherited from LocalizedError] 3 members
        @available(macOS 10.10, watchOS 2.0, iOS 8.0, tvOS 9.0, *)
        var failureReason: String? { get }
        @available(macOS 10.10, watchOS 2.0, iOS 8.0, tvOS 9.0, *)
        var recoverySuggestion: String? { get }
        @available(macOS 10.10, watchOS 2.0, iOS 8.0, tvOS 9.0, *)
        var helpAnchor: String? { get }
    }

    /// Possible ``CassandraClient`` errors.
    public struct Error : CustomStringConvertible, Equatable, Error, LocalizedError, Sendable {
        /// A `nextPage()` call was made while another was still in flight on the same
        /// `PaginatedRows`. Pagination must be driven sequentially (one page at a time).
        public static let concurrentPaginationUnsupported: CassandraClient.Error
        public var description: String { get }
        /// Unexpected client connection state.
        public static let disconnected: CassandraClient.Error
        /// Backs `localizedDescription`, which otherwise reports a generic Foundation message.
        public var errorDescription: String? { get }
        /// All rows for a query result have been consumed.
        public static let rowsExhausted: CassandraClient.Error
        public var shortDescription: String { get }

        public static func alreadyExists(_ description: String) -> Error
        public static func badCredentials(_ description: String) -> Error
        public static func badParams(_ description: String) -> Error
        public static func callbackAlreadySet(_ description: String) -> Error
        public static func decryptionError(_ description: String) -> Error
        public static func encryptionConfigError(_ description: String) -> Error
        public static func encryptionError(_ description: String) -> Error
        public static func functionFailure(_ description: String) -> Error
        public static func hostResolution(_ description: String) -> Error
        public static func indexOutOfBounds(_ description: String) -> Error
        public static func internalError(_ description: String) -> Error
        public static func invalidCustomType(_ description: String) -> Error
        public static func invalidData(_ description: String) -> Error
        public static func invalidErrorResultType(_ description: String) -> Error
        public static func invalidFutureType(_ description: String) -> Error
        public static func invalidItemCount(_ description: String) -> Error
        public static func invalidQuery(_ description: String) -> Error
        public static func invalidState(_ description: String) -> Error
        public static func invalidStatementType(_ description: String) -> Error
        public static func invalidValueType(_ description: String) -> Error
        public static func keyNotFound(_ description: String) -> Error
        public static func messageEncode(_ description: String) -> Error
        public static func nameDoesNotExist(_ description: String) -> Error
        public static func noAvailableIOThread(_ description: String) -> Error
        public static func noCustomPayload(_ description: String) -> Error
        public static func noHostsAvailable(_ description: String) -> Error
        public static func noPagingState(_ description: String) -> Error
        public static func noStreams(_ description: String) -> Error
        public static func notEnoughData(_ description: String) -> Error
        public static func notImplemented(_ description: String) -> Error
        public static func nullValue(_ description: String) -> Error
        public static func other(code: UInt32, description: String?) -> Error
        public static func parameterUnset(_ description: String) -> Error
        public static func protocolError(_ description: String) -> Error
        public static func readFailure(_ description: String) -> Error
        public static func readTimeout(_ description: String) -> Error
        public static func requestQueueFull(_ description: String) -> Error
        public static func requestTimedOut(_ description: String) -> Error
        public static func serverBootstrapping(_ description: String) -> Error
        public static func serverConfigError(_ description: String) -> Error
        public static func serverError(_ description: String) -> Error
        public static func serverOverloaded(_ description: String) -> Error
        public static func serverUnavailable(_ description: String) -> Error
        public static func sslClosed(_ description: String) -> Error
        public static func sslIdentityMismatch(_ description: String) -> Error
        public static func sslInvalidCert(_ description: String) -> Error
        public static func sslInvalidPeerCert(_ description: String) -> Error
        public static func sslInvalidPrivateKey(_ description: String) -> Error
        public static func sslNoPeerCert(_ description: String) -> Error
        public static func sslProtocolError(_ description: String) -> Error
        public static func syntaxError(_ description: String) -> Error
        public static func truncateError(_ description: String) -> Error
        public static func unableToClose(_ description: String) -> Error
        public static func unableToConnect(_ description: String) -> Error
        public static func unableToDetermineProtocol(_ description: String) -> Error
        public static func unableToInit(_ description: String) -> Error
        public static func unableToSetKeyspace(_ description: String) -> Error
        public static func unauthorized(_ description: String) -> Error
        public static func unexpectedResponse(_ description: String) -> Error
        public static func unprepared(_ description: String) -> Error
        public static func writeError(_ description: String) -> Error
        public static func writeFailure(_ description: String) -> Error
        public static func writeTimeout(_ description: String) -> Error

        // [inherited from Equatable] 1 member
        static func != (lhs: Self, rhs: Self) -> Bool

        // [inherited from Error] 1 member
        @available(macOS 10.10, watchOS 2.0, iOS 8.0, tvOS 9.0, *)
        var localizedDescription: String { get }

        // [inherited from LocalizedError] 3 members
        @available(macOS 10.10, watchOS 2.0, iOS 8.0, tvOS 9.0, *)
        var failureReason: String? { get }
        @available(macOS 10.10, watchOS 2.0, iOS 8.0, tvOS 9.0, *)
        var recoverySuggestion: String? { get }
        @available(macOS 10.10, watchOS 2.0, iOS 8.0, tvOS 9.0, *)
        var helpAnchor: String? { get }

        // [reconstructed] 1 member guaranteed by a conformance above that the symbol graph does not emit
        public static func == (lhs: Self, rhs: Self) -> Bool
    }
}
````
