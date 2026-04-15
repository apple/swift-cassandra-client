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
import Foundation
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
        /// Full primary key bytes (partition key + clustering columns).
        public let primaryKey: Data

        public init(keyspace: String, table: String, column: String, primaryKey: Data) {
            self.keyspace = keyspace
            self.table = table
            self.column = column
            self.primaryKey = primaryKey
        }

        internal var contextString: String { "\(self.keyspace).\(self.table).\(self.column)" }

        public func withColumn(_ column: String) -> EncryptionContext {
            EncryptionContext(keyspace: self.keyspace, table: self.table, column: column, primaryKey: self.primaryKey)
        }

        public func withPrimaryKey(_ primaryKey: Data) -> EncryptionContext {
            EncryptionContext(keyspace: self.keyspace, table: self.table, column: self.column, primaryKey: primaryKey)
        }
    }

    /// Partial encryption context supplied per-row during Codable decoding.
    /// The `column` field is derived automatically from the struct's property name.
    public struct RowEncryptionContext {
        public let keyspace: String
        public let table: String
        public let primaryKey: Data

        public init(keyspace: String, table: String, primaryKey: Data) {
            self.keyspace = keyspace
            self.table = table
            self.primaryKey = primaryKey
        }

        internal func forColumn(_ column: String) -> EncryptionContext {
            EncryptionContext(keyspace: self.keyspace, table: self.table, column: column, primaryKey: self.primaryKey)
        }
    }
}

extension CassandraClient.EncryptionContext: Sendable {}

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
    @available(macOS 15.0, iOS 18.0, *)
    struct KeysHolder {
        private var rootKeys: [String: Data]
        private var kekCache: [String: SymmetricKey] = [:]
        private let salt = Data("swift-cassandra-client-v1".utf8)

        init(rootKeys: [String: Data]) {
            self.rootKeys = rootKeys
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
            let kek = HKDF<SHA256>.deriveKey(
                inputKeyMaterial: rootKey,
                salt: self.salt,
                info: Data(context.utf8),
                outputByteCount: 32
            )
            self.kekCache[cacheKey] = kek
            return kek
        }

        /// Derive a per-row data encryption key (DEK) from the column key and primary key bytes.
        /// Not cached — each row has a unique primary key, so caching would grow unbounded.
        mutating func deriveDEK(keyName: String, context: String, primaryKey: Data) throws -> SymmetricKey {
            let kek = try self.deriveKEK(keyName: keyName, context: context)
            return HKDF<SHA256>.deriveKey(
                inputKeyMaterial: kek,
                salt: Data(),
                info: primaryKey,
                outputByteCount: 32
            )
        }
    }
}

// MARK: - Encryptor

extension CassandraClient {
    /// Handles column-level encryption and decryption using AES-GCM with HKDF-derived keys.
    @available(macOS 15.0, iOS 18.0, *)
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
        }

        private let state: Mutex<State>

        /// Create an encryptor with the given key map and current key name.
        ///
        /// - Parameters:
        ///   - keyMap: Dictionary mapping key names (e.g. "key-2025") to 32-byte raw key material.
        ///     Multiple keys can be provided to support key rotation — old keys decrypt existing data,
        ///     while `currentKeyName` selects which key is used for new encryptions.
        ///   - currentKeyName: The key name to use for new encryptions. Must exist in `keyMap`.
        /// - Throws: `CassandraClient.Error.encryptionConfigError` if the key map is empty,
        ///   a key name is invalid, a key is not 32 bytes, or `currentKeyName` is not in the map.
        public init(keyMap: [String: Data], currentKeyName: String) throws {
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
            self.state = Mutex(State(
                currentKeyName: currentKeyName,
                keysHolder: KeysHolder(rootKeys: keyMap)
            ))
        }

        public func addKey(name: String, secret: Data) throws {
            try Self.validateKey(name: name, secret: secret)
            self.state.withLock { $0.keysHolder.addKey(name: name, secret: secret) }
        }

        /// Set the key name used for new encryptions. The key must already exist in the key map.
        public func setCurrentKeyName(_ name: String) throws {
            try Self.validateKeyName(name)
            try self.state.withLock {
                guard $0.keysHolder.hasKey(name) else {
                    throw CassandraClient.Error.keyNotFound("Key '\(name)' not found in keyMap")
                }
                $0.currentKeyName = name
            }
        }

        /// Replace the entire key map. Existing keys cannot be removed or changed.
        /// The current key name must exist in the new map.
        public func loadKeys(from newKeyMap: [String: Data]) throws {
            for (name, key) in newKeyMap {
                try Self.validateKey(name: name, secret: key)
            }
            try self.state.withLock {
                for (existingName, existingKey) in $0.keysHolder.allKeys {
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
                $0.keysHolder = KeysHolder(rootKeys: newKeyMap)
            }
        }

        /// Encrypt plaintext data for the given context.
        ///
        /// Produces an envelope: `[version:1][algorithm:1][key-name-len:1][key-name:N][nonce:12][ciphertext+tag]`
        ///
        /// The envelope is self-describing — it contains the key name and algorithm used,
        /// so `decrypt` can process data encrypted with any key in the key map.
        internal func encrypt(_ data: Data, context: EncryptionContext) throws -> Data {
            let (keyName, dek) = try self.state.withLock {
                let name = $0.currentKeyName
                let key = try $0.keysHolder.deriveDEK(
                    keyName: name,
                    context: context.contextString,
                    primaryKey: context.primaryKey
                )
                return (name, key)
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
            return envelope
        }

        /// Decrypt an envelope for the given context.
        ///
        /// Parses the envelope to extract the key name and algorithm, derives the DEK
        /// using the key from the envelope (not `currentKeyName`), and decrypts.
        /// This allows data encrypted with any key in the key map to be decrypted.
        internal func decrypt(_ envelope: Data, context: EncryptionContext) throws -> Data {
            // Minimum envelope: version(1) + algorithm(1) + keyNameLen(1) + keyName(1+) + nonce(12) + tag(16)
            let minSize = 1 + 1 + 1 + 1 + Self.nonceSize + 16
            guard envelope.count >= minSize else {
                throw CassandraClient.Error.decryptionError(
                    "Envelope too small: \(envelope.count) bytes, minimum \(minSize)"
                )
            }

            var offset = 0

            let version = envelope[offset]
            offset += 1
            guard version == Self.envelopeVersion else {
                throw CassandraClient.Error.decryptionError("Unsupported envelope version: \(version)")
            }

            let algorithm = envelope[offset]
            offset += 1
            guard algorithm == Self.algorithmHKDFDerivedAESGCM else {
                throw CassandraClient.Error.decryptionError("Unsupported algorithm: \(algorithm)")
            }

            let keyNameLen = Int(envelope[offset])
            offset += 1
            guard keyNameLen > 0, offset + keyNameLen <= envelope.count else {
                throw CassandraClient.Error.decryptionError("Invalid key name length: \(keyNameLen)")
            }

            let keyNameBytes = envelope[offset ..< offset + keyNameLen]
            offset += keyNameLen
            guard let keyName = String(data: Data(keyNameBytes), encoding: .utf8) else {
                throw CassandraClient.Error.decryptionError("Invalid key name encoding")
            }

            guard envelope.count - offset >= Self.nonceSize + 16 else {
                throw CassandraClient.Error.decryptionError("Envelope too small for nonce + ciphertext + tag")
            }

            let nonceData = envelope[offset ..< offset + Self.nonceSize]
            offset += Self.nonceSize

            let ciphertextAndTag = envelope[offset...]

            let dek = try self.state.withLock {
                return try $0.keysHolder.deriveDEK(
                    keyName: keyName,
                    context: context.contextString,
                    primaryKey: context.primaryKey
                )
            }

            let nonce: AES.GCM.Nonce
            do {
                nonce = try AES.GCM.Nonce(data: nonceData)
            } catch {
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
                throw CassandraClient.Error.decryptionError("Invalid sealed box: \(error)")
            }

            let aad = Data(context.contextString.utf8)
            do {
                return try AES.GCM.open(sealedBox, using: dek, authenticating: aad)
            } catch {
                throw CassandraClient.Error.decryptionError("AES-GCM authentication failed: \(error)")
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

@available(macOS 15.0, iOS 18.0, *)
extension CassandraClient.Encryptor: Sendable {}

// MARK: - CodingUserInfoKey extensions for encryption

extension CodingUserInfoKey {
    static let cassandraEncryptor = CodingUserInfoKey(rawValue: "cassandraEncryptor")!
    static let cassandraRowContext = CodingUserInfoKey(rawValue: "cassandraRowContext")!
}
