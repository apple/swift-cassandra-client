//===----------------------------------------------------------------------===//
//
// This source file is part of the Swift Cassandra Client open source project
//
// Copyright (c) 2022-2025 Apple Inc. and the Swift Cassandra Client project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of Swift Cassandra Client project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import Crypto
import Dispatch
import Foundation
import Logging
import Metrics
import Synchronization

// MARK: - EncryptionContext

extension CassandraClient {
    /// Context identifying the encrypted column and row for key derivation.
    ///
    /// Renaming keyspace, table, or column after data has been encrypted
    /// will make that data permanently unreadable.
    public struct EncryptionContext {
        public let keyspace: String
        public let table: String
        public let column: String
        /// Full primary key (partition key + clustering columns), built via ``PrimaryKey/init(from:)``.
        public let primaryKey: PrimaryKey

        internal init(keyspace: String, table: String, column: String, primaryKey: PrimaryKey) {
            self.keyspace = keyspace
            self.table = table
            self.column = column
            self.primaryKey = primaryKey
        }

        internal var contextString: String { "\(self.keyspace).\(self.table).\(self.column)" }

        /// Base context without a column name. Use ``forColumn(_:)`` to produce a full ``EncryptionContext``.
        public struct Base {
            public let keyspace: String
            public let table: String
            public let primaryKey: PrimaryKey

            public init(keyspace: String, table: String, primaryKey: PrimaryKey) {
                self.keyspace = keyspace
                self.table = table
                self.primaryKey = primaryKey
            }

            public func forColumn(_ column: String) -> EncryptionContext {
                EncryptionContext(
                    keyspace: self.keyspace,
                    table: self.table,
                    column: column,
                    primaryKey: self.primaryKey
                )
            }
        }
    }

}

extension CassandraClient.EncryptionContext: Sendable {}

// MARK: - PrimaryKey

extension CassandraClient {
    /// Opaque primary key with length-prefixed key components.
    ///
    /// Each component is serialized as `[4-byte big-endian length][value bytes]`.
    /// This ensures composite keys are unambiguous — for example,
    /// `PrimaryKey(from: .string("ab"), .string("c"))` produces different bytes
    /// from `PrimaryKey(from: .string("a"), .string("bc"))`.
    public struct PrimaryKey: Sendable, Equatable {
        internal let data: Data

        public init(from components: CassandraClient.KeyComponent...) {
            self.init(from: components)
        }

        /// Internal init from an array of key components (variadic can't be forwarded).
        internal init(from components: [CassandraClient.KeyComponent]) {
            var result = Data()
            for component in components {
                var length = UInt32(component.bytes.count).bigEndian
                result.append(Data(bytes: &length, count: 4))
                result.append(component.bytes)
            }
            self.data = result
        }
    }
}

// MARK: - KeyComponent

extension CassandraClient {
    /// A typed key component for use with ``PrimaryKey/init(from:)``.
    ///
    /// New component types can be added without breaking existing consumers.
    public struct KeyComponent: Sendable {
        /// The serialized bytes for this component.
        internal let bytes: Data

        public static func string(_ value: String) -> KeyComponent {
            KeyComponent(bytes: Data(value.utf8))
        }

        public static func uuid(_ value: Foundation.UUID) -> KeyComponent {
            let t = value.uuid
            return KeyComponent(
                bytes: Data([
                    t.0, t.1, t.2, t.3, t.4, t.5, t.6, t.7,
                    t.8, t.9, t.10, t.11, t.12, t.13, t.14, t.15,
                ])
            )
        }

        public static func int32(_ value: Int32) -> KeyComponent {
            var be = value.bigEndian
            return KeyComponent(bytes: Data(bytes: &be, count: 4))
        }

        public static func int64(_ value: Int64) -> KeyComponent {
            var be = value.bigEndian
            return KeyComponent(bytes: Data(bytes: &be, count: 8))
        }

        public static func data(_ value: Data) -> KeyComponent {
            KeyComponent(bytes: value)
        }

        public static func date(_ value: Date) -> KeyComponent {
            var be = Int64(value.timeIntervalSince1970 * 1000).bigEndian
            return KeyComponent(bytes: Data(bytes: &be, count: 8))
        }
    }
}

// MARK: - Encrypted wrapper

extension CassandraClient {
    /// Wrapper that marks a value for transparent encryption on the write path
    /// and decryption on the read path via Codable.
    public struct Encrypted<T>: Codable where T: Codable {
        public let value: T
        public init(_ value: T) { self.value = value }

        /// Always throws — decoding is intercepted by `RowDecodingContainer` before this is called.
        /// This conformance exists so `Encrypted<T>` satisfies the `Codable` requirement on struct fields.
        /// Using `Encrypted<T>` with a standard `Decoder` (e.g. `JSONDecoder`) is not supported.
        public init(from decoder: Decoder) throws {
            throw CassandraClient.Error.decryptionError(
                "Encrypted<T> should be decoded via RowDecoder, not directly"
            )
        }

        public func encode(to encoder: Encoder) throws {
            throw CassandraClient.Error.encryptionError(
                "Encrypted<T> cannot be encoded directly. Use Statement.Value.encryptedXxx() on the write path."
            )
        }
    }
}

// Only Sendable when T is Sendable
extension CassandraClient.Encrypted: Sendable where T: Sendable {}

// MARK: - KeysHolder

extension CassandraClient {
    /// Internal cache for derived column keys and storage for root key material.
    /// Not thread-safe on its own — the `Encryptor` that owns it must hold a lock.
    @available(macOS 15.0, iOS 18.0, visionOS 2.0, *)
    struct KeysHolder {
        private var rootKeys: [String: Data]
        let salt: Data
        private let kekSalt: Data
        private let dekSalt: Data
        private var kekCache: [String: SymmetricKey] = [:]

        init(rootKeys: [String: Data], salt: Data) {
            self.rootKeys = rootKeys
            self.salt = salt
            self.kekSalt = salt + Data("-KEK".utf8)
            self.dekSalt = salt + Data("-DEK".utf8)
        }

        func hasKey(_ name: String) -> Bool {
            self.rootKeys[name] != nil
        }

        var allKeys: [String: Data] {
            self.rootKeys
        }

        mutating func addKey(name: String, secret: Data) {
            self.rootKeys[name] = secret
        }

        /// Returns cached column key (KEK) or derives and caches a new one.
        /// Cache key is "keyName:contextString" (e.g. "key-2025:prod.users.ssn").
        mutating func deriveKEK(keyName: String, context: String) throws -> SymmetricKey {
            let cacheKey = "\(keyName):\(context)"
            if let cached = self.kekCache[cacheKey] {
                return cached
            }
            guard let rootKeyData = self.rootKeys[keyName] else {
                throw CassandraClient.Error.keyNotFound("Key '\(keyName)' not found in keyMap")
            }
            let rootKey = SymmetricKey(data: rootKeyData)
            let kek = HKDF<SHA512>.deriveKey(
                inputKeyMaterial: rootKey,
                salt: self.kekSalt,
                info: Data(context.utf8),
                outputByteCount: 32
            )
            self.kekCache[cacheKey] = kek
            return kek
        }

        /// Derive a per-row data encryption key (DEK) from the column key and primary key bytes.
        /// Not cached — each row has a unique primary key, so caching would grow unbounded.
        mutating func deriveDEK(
            keyName: String,
            context: String,
            primaryKey: CassandraClient.PrimaryKey
        ) throws -> SymmetricKey {
            let kek = try self.deriveKEK(keyName: keyName, context: context)
            return HKDF<SHA512>.deriveKey(
                inputKeyMaterial: kek,
                salt: self.dekSalt,
                info: primaryKey.data,
                outputByteCount: 32
            )
        }
    }
}

// MARK: - Encryptor

extension CassandraClient {
    /// Handles column-level encryption and decryption using AES-GCM with HKDF-derived keys.
    @available(macOS 15.0, iOS 18.0, visionOS 2.0, *)
    public final class Encryptor {
        static let envelopeVersion: UInt8 = 0x02
        /// AES-GCM with a per-row DEK derived deterministically via HKDF from the column KEK and primary key.
        /// Encryption is non-deterministic (random nonce per call); only the DEK derivation is deterministic.
        static let algorithmHKDFDerivedAESGCM: UInt8 = 0x02
        static let nonceSize = 12
        static let keySize = 32
        static let maxKeyNameLength = 255

        private struct State {
            var currentKeyName: String
            var keysHolder: KeysHolder
            var metricsCache: [String: EncryptorMetrics] = [:]

            mutating func metrics(forKey keyName: String) -> EncryptorMetrics {
                if let cached = metricsCache[keyName] {
                    return cached
                }
                let m = EncryptorMetrics(keyName: keyName)
                metricsCache[keyName] = m
                return m
            }
        }

        /// Cached metric handles for a single key name, avoiding per-call allocation.
        private struct EncryptorMetrics {
            let encryptTotal: Counter
            let encryptDuration: Timer
            let decryptTotal: Counter
            let decryptDuration: Timer
            let decryptFailures: Counter

            init(keyName: String) {
                let dims = [(EncryptionMetric.dimensionKeyName, keyName)]
                self.encryptTotal = Counter(label: EncryptionMetric.encryptTotal, dimensions: dims)
                self.encryptDuration = Timer(label: EncryptionMetric.encryptDuration, dimensions: dims)
                self.decryptTotal = Counter(label: EncryptionMetric.decryptTotal, dimensions: dims)
                self.decryptDuration = Timer(label: EncryptionMetric.decryptDuration, dimensions: dims)
                self.decryptFailures = Counter(label: EncryptionMetric.decryptFailures, dimensions: dims)
            }
        }

        private let state: Mutex<State>
        private let logger: Logger

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
        public init(
            keyMap: [String: Data],
            currentKeyName: String,
            salt: Data,
            logger: Logger
        ) throws {
            guard !salt.isEmpty else {
                throw CassandraClient.Error.encryptionConfigError("salt must not be empty")
            }
            guard !keyMap.isEmpty else {
                throw CassandraClient.Error.encryptionConfigError("keyMap must not be empty")
            }
            for (name, key) in keyMap {
                try Self.validateKey(name: name, secret: key)
            }
            guard keyMap[currentKeyName] != nil else {
                throw CassandraClient.Error.encryptionConfigError(
                    "currentKeyName '\(currentKeyName)' not found in keyMap"
                )
            }
            self.logger = logger
            self.state = Mutex(
                State(
                    currentKeyName: currentKeyName,
                    keysHolder: KeysHolder(rootKeys: keyMap, salt: salt)
                )
            )
        }

        public func addKey(name: String, secret: Data) throws {
            try Self.validateKey(name: name, secret: secret)
            self.state.withLock { $0.keysHolder.addKey(name: name, secret: secret) }
        }

        /// Set the key name used for new encryptions. The key must already exist in the key map.
        public func setCurrentKeyName(_ name: String) throws {
            try Self.validateKeyName(name)
            let oldName = try self.state.withLock {
                guard $0.keysHolder.hasKey(name) else {
                    throw CassandraClient.Error.keyNotFound("Key '\(name)' not found in keyMap")
                }
                let old = $0.currentKeyName
                $0.currentKeyName = name
                return old
            }
            self.logger.info(
                "Cassandra client key rotated",
                metadata: [
                    EncryptionLogKey.keyRotationFrom: "\(oldName)",
                    EncryptionLogKey.keyRotationTo: "\(name)",
                ]
            )
            Counter(label: EncryptionMetric.keyRotationTotal, dimensions: [(EncryptionMetric.dimensionKeyName, name)])
                .increment()
        }

        /// Replace the entire key map. Existing keys cannot be removed or changed.
        /// The current key name must exist in the new map.
        public func loadKeys(from newKeyMap: [String: Data]) throws {
            for (name, key) in newKeyMap {
                try Self.validateKey(name: name, secret: key)
            }
            let newKeyCount = try self.state.withLock {
                let existingKeys = $0.keysHolder.allKeys
                for (existingName, existingKey) in existingKeys {
                    guard let newKey = newKeyMap[existingName] else {
                        throw CassandraClient.Error.encryptionConfigError(
                            "Cannot remove existing key '\(existingName)'"
                        )
                    }
                    guard newKey == existingKey else {
                        throw CassandraClient.Error.encryptionConfigError(
                            "Cannot change existing key '\(existingName)'"
                        )
                    }
                }
                guard newKeyMap[$0.currentKeyName] != nil else {
                    throw CassandraClient.Error.encryptionConfigError(
                        "currentKeyName '\($0.currentKeyName)' not found in new keyMap"
                    )
                }
                let added = newKeyMap.count - existingKeys.count
                $0.keysHolder = KeysHolder(rootKeys: newKeyMap, salt: $0.keysHolder.salt)
                return added
            }
            self.logger.info(
                "Keys loaded",
                metadata: [
                    EncryptionLogKey.keysLoaded: "\(newKeyCount)"
                ]
            )
        }

        /// Encrypt plaintext data for the given context.
        ///
        /// Produces an envelope: `[version:1][algorithm:1][key-name-len:1][key-name:N][nonce:12][ciphertext+tag]`
        ///
        /// The envelope is self-describing — it contains the key name and algorithm used,
        /// so `decrypt` can process data encrypted with any key in the key map.
        internal func encrypt(_ data: Data, context: EncryptionContext) throws -> Data {
            let start = DispatchTime.now()
            let (keyName, dek, metrics) = try self.state.withLock {
                let name = $0.currentKeyName
                let key = try $0.keysHolder.deriveDEK(
                    keyName: name,
                    context: context.contextString,
                    primaryKey: context.primaryKey
                )
                let m = $0.metrics(forKey: name)
                return (name, key, m)
            }

            let keyNameBytes = Data(keyName.utf8)
            let nonce = AES.GCM.Nonce()
            let aad = Data(context.contextString.utf8)

            let sealed: AES.GCM.SealedBox
            do {
                sealed = try AES.GCM.seal(data, using: dek, nonce: nonce, authenticating: aad)
            } catch {
                throw CassandraClient.Error.encryptionError("AES-GCM seal failed: \(error)")
            }

            var envelope = Data()
            envelope.reserveCapacity(3 + keyNameBytes.count + Self.nonceSize + sealed.ciphertext.count + 16)
            envelope.append(Self.envelopeVersion)
            envelope.append(Self.algorithmHKDFDerivedAESGCM)
            envelope.append(UInt8(keyNameBytes.count))
            envelope.append(keyNameBytes)
            envelope.append(contentsOf: sealed.nonce)
            envelope.append(sealed.ciphertext)
            envelope.append(sealed.tag)

            let duration = DispatchTime.now().uptimeNanoseconds - start.uptimeNanoseconds
            self.logger.debug(
                "Encrypted column",
                metadata: [
                    EncryptionLogKey.keyName: "\(keyName)",
                    EncryptionLogKey.column: "\(context.column)",
                ]
            )
            metrics.encryptTotal.increment()
            metrics.encryptDuration.recordNanoseconds(Int64(duration))

            return envelope
        }

        /// Decrypt an envelope for the given context.
        ///
        /// Parses the envelope to extract the key name and algorithm, derives the DEK
        /// using the key from the envelope (not `currentKeyName`), and decrypts.
        /// This allows data encrypted with any key in the key map to be decrypted.
        internal func decrypt(_ envelope: Data, context: EncryptionContext) throws -> Data {
            let start = DispatchTime.now()
            // Minimum envelope: version(1) + algorithm(1) + keyNameLen(1) + keyName(1+) + nonce(12) + tag(16)
            let minSize = 1 + 1 + 1 + 1 + Self.nonceSize + 16
            guard envelope.count >= minSize else {
                self.onDecryptionFailure(
                    message: "Decryption failed — envelope too small",
                    keyName: "unavailable",
                    column: context.column
                )
                throw CassandraClient.Error.decryptionError(
                    "Envelope too small: \(envelope.count) bytes, minimum \(minSize)"
                )
            }

            var offset = 0

            let version = envelope[offset]
            offset += 1
            guard version == Self.envelopeVersion else {
                self.onDecryptionFailure(
                    message: "Decryption failed — unsupported envelope version",
                    keyName: "unavailable",
                    column: context.column
                )
                throw CassandraClient.Error.decryptionError("Unsupported envelope version: \(version)")
            }

            let algorithm = envelope[offset]
            offset += 1
            guard algorithm == Self.algorithmHKDFDerivedAESGCM else {
                self.onDecryptionFailure(
                    message: "Decryption failed — unsupported algorithm",
                    keyName: "unavailable",
                    column: context.column
                )
                throw CassandraClient.Error.decryptionError("Unsupported algorithm: \(algorithm)")
            }

            let keyNameLen = Int(envelope[offset])
            offset += 1
            guard keyNameLen > 0, offset + keyNameLen <= envelope.count else {
                self.onDecryptionFailure(
                    message: "Decryption failed — invalid key name length",
                    keyName: "unavailable",
                    column: context.column
                )
                throw CassandraClient.Error.decryptionError("Invalid key name length: \(keyNameLen)")
            }

            let keyNameBytes = envelope[offset..<offset + keyNameLen]
            offset += keyNameLen
            guard let keyName = String(data: Data(keyNameBytes), encoding: .utf8) else {
                self.onDecryptionFailure(
                    message: "Decryption failed — invalid key name encoding",
                    keyName: "unavailable",
                    column: context.column
                )
                throw CassandraClient.Error.decryptionError("Invalid key name encoding")
            }

            guard envelope.count - offset >= Self.nonceSize + 16 else {
                self.onDecryptionFailure(
                    message: "Decryption failed — envelope too small for nonce + ciphertext + tag",
                    keyName: keyName,
                    column: context.column
                )
                throw CassandraClient.Error.decryptionError("Envelope too small for nonce + ciphertext + tag")
            }

            let nonceData = envelope[offset..<offset + Self.nonceSize]
            offset += Self.nonceSize

            let ciphertextAndTag = envelope[offset...]

            let dek: SymmetricKey
            let metrics: EncryptorMetrics
            do {
                (dek, metrics) = try self.state.withLock {
                    let key = try $0.keysHolder.deriveDEK(
                        keyName: keyName,
                        context: context.contextString,
                        primaryKey: context.primaryKey
                    )
                    let m = $0.metrics(forKey: keyName)
                    return (key, m)
                }
            } catch {
                self.onDecryptionFailure(
                    message: "Decryption failed — key derivation error",
                    keyName: keyName,
                    column: context.column
                )
                throw error
            }

            let nonce: AES.GCM.Nonce
            do {
                nonce = try AES.GCM.Nonce(data: nonceData)
            } catch {
                self.onDecryptionFailure(
                    message: "Decryption failed — invalid nonce",
                    keyName: keyName,
                    column: context.column,
                    metrics: metrics
                )
                throw CassandraClient.Error.decryptionError("Invalid nonce: \(error)")
            }

            let sealedBox: AES.GCM.SealedBox
            do {
                sealedBox = try AES.GCM.SealedBox(
                    nonce: nonce,
                    ciphertext: ciphertextAndTag.dropLast(16),
                    tag: ciphertextAndTag.suffix(16)
                )
            } catch {
                self.onDecryptionFailure(
                    message: "Decryption failed — invalid sealed box",
                    keyName: keyName,
                    column: context.column,
                    metrics: metrics
                )
                throw CassandraClient.Error.decryptionError("Invalid sealed box: \(error)")
            }

            let aad = Data(context.contextString.utf8)
            do {
                let plaintext = try AES.GCM.open(sealedBox, using: dek, authenticating: aad)
                let duration = DispatchTime.now().uptimeNanoseconds - start.uptimeNanoseconds
                self.logger.debug(
                    "Decrypted column",
                    metadata: [
                        EncryptionLogKey.keyName: "\(keyName)",
                        EncryptionLogKey.column: "\(context.column)",
                    ]
                )
                metrics.decryptTotal.increment()
                metrics.decryptDuration.recordNanoseconds(Int64(duration))
                return plaintext
            } catch {
                self.onDecryptionFailure(
                    message: "Decryption failed — possible data corruption or tampering",
                    keyName: keyName,
                    column: context.column,
                    metrics: metrics
                )
                throw CassandraClient.Error.decryptionError("AES-GCM authentication failed: \(error)")
            }
        }

        private func onDecryptionFailure(
            message: String,
            keyName: String,
            column: String,
            metrics: EncryptorMetrics? = nil
        ) {
            self.logger.warning(
                "\(message)",
                metadata: [
                    EncryptionLogKey.keyName: "\(keyName)",
                    EncryptionLogKey.column: "\(column)",
                ]
            )
            if let metrics = metrics {
                metrics.decryptFailures.increment()
            } else {
                Counter(
                    label: EncryptionMetric.decryptFailures,
                    dimensions: [(EncryptionMetric.dimensionKeyName, keyName)]
                ).increment()
            }
        }

        private static func validateKey(name: String, secret: Data) throws {
            try validateKeyName(name)
            guard secret.count == keySize else {
                throw CassandraClient.Error.encryptionConfigError(
                    "Key '\(name)' must be \(keySize) bytes, got \(secret.count)"
                )
            }
        }

        /// Validate a key name: must be 1-255 characters, alphanumeric plus '-' and '_'.
        private static func validateKeyName(_ name: String) throws {
            guard !name.isEmpty, name.count <= maxKeyNameLength else {
                throw CassandraClient.Error.encryptionConfigError(
                    "Key name must be 1-\(maxKeyNameLength) characters, got \(name.count)"
                )
            }
            let allowed = CharacterSet.alphanumerics.union(CharacterSet(charactersIn: "-_"))
            guard name.unicodeScalars.allSatisfy({ allowed.contains($0) }) else {
                throw CassandraClient.Error.encryptionConfigError(
                    "Key name '\(name)' contains invalid characters (allowed: alphanumeric, '-', '_')"
                )
            }
        }
    }
}

@available(macOS 15.0, iOS 18.0, visionOS 2.0, *)
extension CassandraClient.Encryptor: Sendable {}

// MARK: - KeyColumnType

extension CassandraClient {
    /// Type tag for key columns used in column registration.
    ///
    /// New column types can be added without breaking existing consumers.
    public struct KeyColumnType: Sendable, Equatable {
        internal let rawValue: String

        public static let string = KeyColumnType(rawValue: "string")
        public static let uuid = KeyColumnType(rawValue: "uuid")
        public static let int32 = KeyColumnType(rawValue: "int32")
        public static let int64 = KeyColumnType(rawValue: "int64")
        public static let data = KeyColumnType(rawValue: "data")
        public static let date = KeyColumnType(rawValue: "date")
    }
}

// MARK: - EncryptionSchema

extension CassandraClient {
    /// Describes one table's encrypted column layout for automatic context building.
    ///
    /// Register a schema via ``Configuration/registerEncryptionSchema(_:)`` so the decoder
    /// can build ``EncryptionContext/Base`` per row without an `encryptionContextBuilder` closure.
    public struct EncryptionSchema: Sendable {
        /// A column in the table's primary key, used to build the PrimaryKey for DEK derivation.
        public struct KeyColumn: Sendable {
            public let name: String
            public let type: KeyColumnType

            public init(name: String, type: KeyColumnType) {
                self.name = name
                self.type = type
            }
        }

        public let keyspace: String
        public let table: String
        public let keyColumns: [KeyColumn]
        /// Names of all encrypted regular columns in the table.
        public let encryptedColumns: Set<String>

        public init(
            keyspace: String,
            table: String,
            keyColumns: [KeyColumn],
            encryptedColumns: Set<String> = []
        ) throws {
            let keyColumnNames = Set(keyColumns.map(\.name))
            let overlap = keyColumnNames.intersection(encryptedColumns)
            guard overlap.isEmpty else {
                throw CassandraClient.Error.encryptionConfigError(
                    "Key columns cannot also be encrypted columns: \(overlap.sorted())"
                )
            }
            self.keyspace = keyspace
            self.table = table
            self.keyColumns = keyColumns
            self.encryptedColumns = encryptedColumns
        }

        /// Registry lookup key: "keyspace.table".
        internal var registryKey: String { "\(self.keyspace).\(self.table)" }

    }
}

// MARK: - CodingUserInfoKey extensions for encryption

extension CodingUserInfoKey {
    static let cassandraEncryptor = CodingUserInfoKey(rawValue: "cassandraEncryptor")!
    static let cassandraRowContext = CodingUserInfoKey(rawValue: "cassandraRowContext")!
}
