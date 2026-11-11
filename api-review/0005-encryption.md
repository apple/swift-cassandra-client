# Client-side encryption

## Overview

- **Area:** Client-side encryption — 56 of the module's public symbols.
- **Note:** This PR exists to collect API review feedback ahead of 1.0. It will be closed rather than merged; anything it settles lands as its own change.
- **Generated from:** `Sources/CassandraClient` at `e60f45a`, with Apple Swift version 6.3.1 (swiftlang-6.3.1.1.2 clang-2100.0.123.102). Derived, not maintained by hand — see `api-review/README.md` to regenerate.

Inherited protocol members are listed without their standard-library documentation, grouped under the protocol that declared each. Members the symbol graph does not emit are marked `[reconstructed]`.

## Public API

````swift
extension CassandraClient {
    /// Wrapper that marks a value for transparent encryption on the write path
    /// and decryption on the read path via Codable.
    public struct Encrypted<T> where T : Decodable, T : Encodable {
        public init(_ value: T)
        /// Always throws — decoding is intercepted by `RowDecodingContainer` before this is called.
        /// This conformance exists so `Encrypted<T>` satisfies the `Codable` requirement on struct fields.
        /// Using `Encrypted<T>` with a standard `Decoder` (e.g. `JSONDecoder`) is not supported.
        public init(from decoder: Decoder) throws

        public var customMirror: Mirror { get }
        public var debugDescription: String { get }
        public var description: String { get }
        public let value: T

        public func encode(to encoder: Encoder) throws
    }

    /// Context identifying the encrypted column and row for key derivation.
    ///
    /// Renaming keyspace, table, or column after data has been encrypted
    /// will make that data permanently unreadable.
    public struct EncryptionContext : Hashable, Sendable {
        public let column: String
        public let keyspace: String
        /// Full primary key (partition key + clustering columns), built via ``PrimaryKey/init(from:)``.
        public let primaryKey: PrimaryKey
        public let table: String

        /// Base context without a column name. Use ``forColumn(_:)`` to produce a full ``EncryptionContext``.
        public struct Base : Hashable, Sendable {
            public init(keyspace: String, table: String, primaryKey: PrimaryKey)

            public let keyspace: String
            public let primaryKey: PrimaryKey
            public let table: String

            public func forColumn(_ column: String) -> EncryptionContext

            // [inherited from Equatable] 1 member
            static func != (lhs: Self, rhs: Self) -> Bool

            // [reconstructed] 1 member guaranteed by a conformance above that the symbol graph does not emit
            public static func == (lhs: Self, rhs: Self) -> Bool
        }

        // [inherited from Equatable] 1 member
        static func != (lhs: Self, rhs: Self) -> Bool

        // [reconstructed] 1 member guaranteed by a conformance above that the symbol graph does not emit
        public static func == (lhs: Self, rhs: Self) -> Bool
    }

    /// Describes one table's encrypted column layout for automatic context building.
    ///
    /// Register a schema via ``Configuration/registerEncryptionSchema(_:)`` so the decoder
    /// can build ``EncryptionContext/Base`` per row without an `encryptionContextBuilder` closure.
    public struct EncryptionSchema : Hashable, Sendable {
        public init(keyspace: String, table: String, keyColumns: [KeyColumn], encryptedColumns: Set<String>) throws

        /// Names of all encrypted regular columns in the table.
        ///
        /// Used only by write-path binding validation; see ``Statement/Options/encryptionTable``
        /// for what each execution path enforces. Decryption is driven by the declared property
        /// type on the decoded model, so a schema registered only to build row contexts for reads
        /// can pass an empty set.
        public let encryptedColumns: Set<String>
        public let keyColumns: [KeyColumn]
        public let keyspace: String
        public let table: String

        /// A column in the table's primary key, used to build the PrimaryKey for DEK derivation.
        public struct KeyColumn : Hashable, Sendable {
            public init(name: String, type: KeyColumnType)

            public let name: String
            public let type: KeyColumnType

            // [inherited from Equatable] 1 member
            static func != (lhs: Self, rhs: Self) -> Bool

            // [reconstructed] 1 member guaranteed by a conformance above that the symbol graph does not emit
            public static func == (lhs: Self, rhs: Self) -> Bool
        }

        // [inherited from Equatable] 1 member
        static func != (lhs: Self, rhs: Self) -> Bool

        // [reconstructed] 1 member guaranteed by a conformance above that the symbol graph does not emit
        public static func == (lhs: Self, rhs: Self) -> Bool
    }

    /// Handles column-level encryption and decryption using AES-GCM with HKDF-derived keys.
    @available(macOS 15.0, iOS 18.0, visionOS 2.0, *)
    public final class Encryptor : Sendable {
        /// Create an encryptor with the given key map and current key name.
        ///
        /// - Parameters:
        ///   - keyMap: Dictionary mapping key names (e.g. "key-2025") to 32-byte raw key material.
        ///     Multiple keys can be provided to support key rotation — old keys decrypt existing data,
        ///     while `currentKeyName` selects which key is used for new encryptions.
        ///   - currentKeyName: The key name to use for new encryptions. Must exist in `keyMap`.
        ///   - salt: Salt for HKDF key derivation. Suffixed with `-KEK` and `-DEK` internally for domain separation.
        ///   - logger: Logger for encryption audit trail.
        /// - Throws: `CassandraClient.Error.encryptionConfigError` if the key map is empty,
        ///   a key name is invalid, a key is not 32 bytes, the salt is empty, or `currentKeyName` is not in the map.
        @available(macOS 15.0, iOS 18.0, visionOS 2.0, *)
        public init(keyMap: [String : Data], currentKeyName: String, salt: Data, logger: Logger) throws

        @available(macOS 15.0, iOS 18.0, visionOS 2.0, *)
        public func addKey(name: String, secret: Data) throws
        /// Replace the entire key map. Existing keys cannot be removed or changed.
        /// The current key name must exist in the new map.
        @available(macOS 15.0, iOS 18.0, visionOS 2.0, *)
        public func loadKeys(from newKeyMap: [String : Data]) throws
        /// Set the key name used for new encryptions. The key must already exist in the key map.
        @available(macOS 15.0, iOS 18.0, visionOS 2.0, *)
        public func setCurrentKeyName(_ name: String) throws
    }

    /// Type tag for key columns used in column registration.
    ///
    /// New column types can be added without breaking existing consumers.
    public struct KeyColumnType : Hashable, Sendable {
        public static let data: CassandraClient.KeyColumnType
        public static let date: CassandraClient.KeyColumnType
        public static let int32: CassandraClient.KeyColumnType
        public static let int64: CassandraClient.KeyColumnType
        public static let string: CassandraClient.KeyColumnType
        public static let uuid: CassandraClient.KeyColumnType

        // [inherited from Equatable] 1 member
        static func != (lhs: Self, rhs: Self) -> Bool

        // [reconstructed] 1 member guaranteed by a conformance above that the symbol graph does not emit
        public static func == (lhs: Self, rhs: Self) -> Bool
    }

    /// A typed key component for use with ``PrimaryKey/init(from:)``.
    ///
    /// New component types can be added without breaking existing consumers.
    public struct KeyComponent : Sendable {
        public static func data(_ value: Data) -> KeyComponent
        public static func date(_ value: Date) -> KeyComponent
        public static func int32(_ value: Int32) -> KeyComponent
        public static func int64(_ value: Int64) -> KeyComponent
        public static func string(_ value: String) -> KeyComponent
        public static func uuid(_ value: Foundation.UUID) -> KeyComponent
    }

    /// Opaque primary key with length-prefixed key components.
    ///
    /// Each component is serialized as `[4-byte big-endian length][value bytes]`.
    /// This ensures composite keys are unambiguous — for example,
    /// `PrimaryKey(from: .string("ab"), .string("c"))` produces different bytes
    /// from `PrimaryKey(from: .string("a"), .string("bc"))`.
    public struct PrimaryKey : Hashable, Sendable {
        public init(from components: CassandraClient.KeyComponent...)

        // [inherited from Equatable] 1 member
        static func != (lhs: Self, rhs: Self) -> Bool

        // [reconstructed] 1 member guaranteed by a conformance above that the symbol graph does not emit
        public static func == (lhs: Self, rhs: Self) -> Bool
    }
}
````
