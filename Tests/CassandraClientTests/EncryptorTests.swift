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

import Foundation
import Logging
import XCTest

@testable import CassandraClient

/// Generate a random 32-byte key for testing.
func randomKey() -> Data {
    var bytes = [UInt8](repeating: 0, count: 32)
    for i in 0..<bytes.count {
        bytes[i] = UInt8.random(in: 0...255)
    }
    return Data(bytes)
}

@available(macOS 15.0, iOS 18.0, visionOS 2.0, *)
final class EncryptorTests: XCTestCase {

    // Helper: create a simple context for testing
    private func testContext(
        column: String = "ssn",
        primaryKey: CassandraClient.PrimaryKey? = nil
    ) -> CassandraClient.EncryptionContext {
        CassandraClient.EncryptionContext(
            keyspace: "test_keyspace",
            table: "users",
            column: column,
            primaryKey: primaryKey ?? .init(from: .string("row-1"))
        )
    }

    // Helper: create an encryptor with one key
    private func makeEncryptor(keyName: String = "key-1", key: Data? = nil) throws -> (CassandraClient.Encryptor, Data)
    {
        let keyData = key ?? randomKey()
        let encryptor = try CassandraClient.Encryptor(
            keyMap: [keyName: keyData],
            currentKeyName: keyName,
            logger: Logger(label: "test.encryptor")
        )
        return (encryptor, keyData)
    }

    // MARK: - Basic encrypt / decrypt

    /// Encrypt then decrypt should return the original plaintext.
    func testEncryptDecrypt() throws {
        let (encryptor, _) = try makeEncryptor()
        let context = testContext()
        let plaintext = Data("hello-world!".utf8)

        let encrypted = try encryptor.encrypt(plaintext, context: context)
        let decrypted = try encryptor.decrypt(encrypted, context: context)

        XCTAssertEqual(decrypted, plaintext)
        XCTAssertNotEqual(encrypted, plaintext, "Encrypted data should differ from plaintext")
    }

    /// Encrypt and decrypt empty data.
    func testEmptyPlaintext() throws {
        let (encryptor, _) = try makeEncryptor()
        let context = testContext()
        let encrypted = try encryptor.encrypt(Data(), context: context)
        let decrypted = try encryptor.decrypt(encrypted, context: context)
        XCTAssertEqual(decrypted, Data())
    }

    // MARK: - Context binding

    /// Same plaintext encrypted for different columns should produce different ciphertext.
    func testContextBinding() throws {
        let (encryptor, _) = try makeEncryptor()
        let plaintext = Data("secret-value".utf8)

        let rowContext = CassandraClient.EncryptionContext.Base(
            keyspace: "test_keyspace",
            table: "users",
            primaryKey: .init(from: .string("row-1"))
        )
        let ssnContext = rowContext.forColumn("ssn")
        let ccContext = rowContext.forColumn("credit_card")

        let encryptedSSN = try encryptor.encrypt(plaintext, context: ssnContext)
        let encryptedCC = try encryptor.encrypt(plaintext, context: ccContext)

        XCTAssertNotEqual(encryptedSSN, encryptedCC, "Different columns should produce different ciphertext")
    }

    /// Same plaintext encrypted for different rows should produce different ciphertext.
    func testRowBinding() throws {
        let (encryptor, _) = try makeEncryptor()
        let plaintext = Data("secret-value".utf8)

        let context1 = testContext(primaryKey: .init(from: .string("row-1")))
        let context2 = CassandraClient.EncryptionContext(
            keyspace: "test_keyspace",
            table: "users",
            column: "ssn",
            primaryKey: .init(from: .string("row-2"))
        )

        let encrypted1 = try encryptor.encrypt(plaintext, context: context1)
        let encrypted2 = try encryptor.encrypt(plaintext, context: context2)

        XCTAssertNotEqual(encrypted1, encrypted2, "Different primary keys should produce different ciphertext")
    }

    /// Decrypt with a different primaryKey should fail.
    func testWrongContext() throws {
        let (encryptor, _) = try makeEncryptor()
        let contextA = testContext(primaryKey: .init(from: .string("row-1")))
        let contextB = testContext(primaryKey: .init(from: .string("row-2")))
        let encrypted = try encryptor.encrypt(Data("secret".utf8), context: contextA)
        XCTAssertThrowsError(try encryptor.decrypt(encrypted, context: contextB))
    }

    // MARK: - Type-specific roundtrips

    /// Encrypt and decrypt Int32.
    func testInt32Roundtrip() throws {
        let (encryptor, _) = try makeEncryptor()
        let context = testContext(column: "age")
        let value: Int32 = -42
        var bigEndian = value.bigEndian
        let data = Data(bytes: &bigEndian, count: 4)
        let encrypted = try encryptor.encrypt(data, context: context)
        let decrypted = try encryptor.decrypt(encrypted, context: context)
        XCTAssertEqual(decrypted.count, 4)
        let result = decrypted.withUnsafeBytes { $0.loadUnaligned(as: Int32.self).bigEndian }
        XCTAssertEqual(result, value)
    }

    /// Encrypt and decrypt Int64.
    func testInt64Roundtrip() throws {
        let (encryptor, _) = try makeEncryptor()
        let context = testContext(column: "age64")
        let value: Int64 = 9_876_543_210
        var bigEndian = value.bigEndian
        let data = Data(bytes: &bigEndian, count: 8)
        let encrypted = try encryptor.encrypt(data, context: context)
        let decrypted = try encryptor.decrypt(encrypted, context: context)
        XCTAssertEqual(decrypted.count, 8)
        let result = decrypted.withUnsafeBytes { $0.loadUnaligned(as: Int64.self).bigEndian }
        XCTAssertEqual(result, value)
    }

    /// Encrypt and decrypt Double.
    func testDoubleRoundtrip() throws {
        let (encryptor, _) = try makeEncryptor()
        let context = testContext(column: "score")
        let value: Double = 3.14159
        var bits = value.bitPattern.bigEndian
        let data = Data(bytes: &bits, count: 8)
        let encrypted = try encryptor.encrypt(data, context: context)
        let decrypted = try encryptor.decrypt(encrypted, context: context)
        XCTAssertEqual(decrypted.count, 8)
        let result = Double(bitPattern: decrypted.withUnsafeBytes { $0.loadUnaligned(as: UInt64.self).bigEndian })
        XCTAssertEqual(result, value)
    }

    /// Encrypt and decrypt UUID.
    func testUUIDRoundtrip() throws {
        let (encryptor, _) = try makeEncryptor()
        let context = testContext(column: "uid")
        let value = Foundation.UUID()
        let u = value.uuid
        let data = Data([
            u.0, u.1, u.2, u.3, u.4, u.5, u.6, u.7,
            u.8, u.9, u.10, u.11, u.12, u.13, u.14, u.15,
        ])
        let encrypted = try encryptor.encrypt(data, context: context)
        let decrypted = try encryptor.decrypt(encrypted, context: context)
        XCTAssertEqual(decrypted.count, 16)
        let t: uuid_t = (
            decrypted[0], decrypted[1], decrypted[2], decrypted[3],
            decrypted[4], decrypted[5], decrypted[6], decrypted[7],
            decrypted[8], decrypted[9], decrypted[10], decrypted[11],
            decrypted[12], decrypted[13], decrypted[14], decrypted[15]
        )
        XCTAssertEqual(Foundation.UUID(uuid: t), value)
    }

    /// Encrypt and decrypt raw bytes.
    func testBytesRoundtrip() throws {
        let (encryptor, _) = try makeEncryptor()
        let context = testContext(column: "blob")
        let value: [UInt8] = [0x00, 0xFF, 0x42, 0xAB, 0x01]
        let encrypted = try encryptor.encrypt(Data(value), context: context)
        let decrypted = try encryptor.decrypt(encrypted, context: context)
        XCTAssertEqual(Array(decrypted), value)
    }

    // MARK: - Key management

    /// Encrypt with key-1, add key-2, switch to key-2, old data still decrypts.
    func testKeyRotation() throws {
        let (encryptor, _) = try makeEncryptor(keyName: "key-1")
        let context = testContext()
        let plaintext = Data("secret-data".utf8)

        let encryptedWithKey1 = try encryptor.encrypt(plaintext, context: context)

        try encryptor.addKey(name: "key-2", secret: randomKey())
        try encryptor.setCurrentKeyName("key-2")

        let encryptedWithKey2 = try encryptor.encrypt(plaintext, context: context)

        let decrypted1 = try encryptor.decrypt(encryptedWithKey1, context: context)
        let decrypted2 = try encryptor.decrypt(encryptedWithKey2, context: context)

        XCTAssertEqual(decrypted1, plaintext)
        XCTAssertEqual(decrypted2, plaintext)
    }

    /// Decrypt with an encryptor that doesn't have the key used to encrypt.
    func testMissingKey() throws {
        let (encryptor1, _) = try makeEncryptor(keyName: "key-1")
        let context = testContext()
        let encrypted = try encryptor1.encrypt(Data("secret".utf8), context: context)

        let (encryptor2, _) = try makeEncryptor(keyName: "key-2")
        XCTAssertThrowsError(try encryptor2.decrypt(encrypted, context: context))
    }

    /// Empty key name should be rejected.
    func testEmptyKeyName() {
        XCTAssertThrowsError(
            try CassandraClient.Encryptor(
                keyMap: ["": randomKey()],
                currentKeyName: "",
                logger: Logger(label: "test.encryptor")
            )
        )
    }

    /// Key name with invalid characters should be rejected.
    func testInvalidKeyNameCharacters() {
        XCTAssertThrowsError(
            try CassandraClient.Encryptor(
                keyMap: ["key with spaces": randomKey()],
                currentKeyName: "key with spaces",
                logger: Logger(label: "test.encryptor")
            )
        )
    }

    /// Key that is not 32 bytes should be rejected.
    func testWrongKeySize() {
        XCTAssertThrowsError(
            try CassandraClient.Encryptor(
                keyMap: ["key-1": Data([0x01, 0x02, 0x03])],
                currentKeyName: "key-1",
                logger: Logger(label: "test.encryptor")
            )
        )
    }

    /// Cannot remove an existing key from the map.
    func testLoadKeysCannotRemoveKey() throws {
        let key1 = randomKey()
        let key2 = randomKey()
        let encryptor = try CassandraClient.Encryptor(
            keyMap: ["key-1": key1, "key-2": key2],
            currentKeyName: "key-1",
            logger: Logger(label: "test.encryptor")
        )
        // New map missing "key-2" — should throw
        XCTAssertThrowsError(try encryptor.loadKeys(from: ["key-1": key1]))
    }

    /// Cannot change an existing key's bytes.
    func testLoadKeysCannotChangeKey() throws {
        let key1 = randomKey()
        let encryptor = try CassandraClient.Encryptor(
            keyMap: ["key-1": key1],
            currentKeyName: "key-1",
            logger: Logger(label: "test.encryptor")
        )
        // Same name, different bytes — should throw
        XCTAssertThrowsError(try encryptor.loadKeys(from: ["key-1": randomKey()]))
    }

    /// Can add a new key via loadKeys while keeping existing ones.
    func testLoadKeysCanAddKey() throws {
        let key1 = randomKey()
        let encryptor = try CassandraClient.Encryptor(
            keyMap: ["key-1": key1],
            currentKeyName: "key-1",
            logger: Logger(label: "test.encryptor")
        )
        let key2 = randomKey()
        XCTAssertNoThrow(try encryptor.loadKeys(from: ["key-1": key1, "key-2": key2]))
    }

    // MARK: - Envelope validation

    /// Envelope too short to contain even the header fields.
    func testEnvelopeTooShort() throws {
        let (encryptor, _) = try makeEncryptor()
        let context = testContext()
        let tooShort = Data([0x01, 0x02, 0x03])
        XCTAssertThrowsError(try encryptor.decrypt(tooShort, context: context))
    }

    /// Envelope with wrong version byte.
    func testEnvelopeWrongVersion() throws {
        let (encryptor, _) = try makeEncryptor()
        let context = testContext()
        var envelope = try encryptor.encrypt(Data("hello".utf8), context: context)
        envelope[0] = 0xFF  // corrupt version byte
        XCTAssertThrowsError(try encryptor.decrypt(envelope, context: context))
    }

    /// Envelope with wrong algorithm byte.
    func testEnvelopeWrongAlgorithm() throws {
        let (encryptor, _) = try makeEncryptor()
        let context = testContext()
        var envelope = try encryptor.encrypt(Data("hello".utf8), context: context)
        envelope[1] = 0xFF  // corrupt algorithm byte
        XCTAssertThrowsError(try encryptor.decrypt(envelope, context: context))
    }

    /// Flipping a byte in the ciphertext should cause decryption to fail.
    func testTamperDetection() throws {
        let (encryptor, _) = try makeEncryptor()
        let context = testContext()
        let plaintext = Data("hello-world!".utf8)

        var encrypted = try encryptor.encrypt(plaintext, context: context)

        // Flip a byte near the end (in the ciphertext region)
        let tamperIndex = encrypted.count - 20
        encrypted[tamperIndex] ^= 0xFF

        XCTAssertThrowsError(try encryptor.decrypt(encrypted, context: context))
    }

    // MARK: - PrimaryKey

    /// Single string component produces 4-byte length prefix followed by UTF-8 bytes.
    func testEncodeKeyComponentsSingleString() {
        let encoded = CassandraClient.PrimaryKey(from: .string("hello"))
        let utf8 = Data("hello".utf8)
        var expectedLength = UInt32(utf8.count).bigEndian
        var expected = Data(bytes: &expectedLength, count: 4)
        expected.append(utf8)
        XCTAssertEqual(encoded.data, expected)
    }

    /// Composite key (string + UUID) is deterministic and produces correct bytes.
    func testEncodeKeyComponentsComposite() {
        let uuid = Foundation.UUID(uuidString: "12345678-1234-1234-1234-123456789ABC")!
        let encoded1 = CassandraClient.PrimaryKey(from: .string("user"), .uuid(uuid))
        let encoded2 = CassandraClient.PrimaryKey(from: .string("user"), .uuid(uuid))
        XCTAssertEqual(encoded1, encoded2, "Same inputs should produce identical output")
        // String "user" = 4 bytes → length(4) + value(4) = 8
        // UUID = 16 bytes → length(4) + value(16) = 20
        XCTAssertEqual(encoded1.data.count, 8 + 20)
    }

    /// Length-prefixing prevents ambiguity: ("ab", "c") != ("a", "bc").
    func testEncodeKeyComponentsAmbiguity() {
        let abC = CassandraClient.PrimaryKey(from: .string("ab"), .string("c"))
        let aBc = CassandraClient.PrimaryKey(from: .string("a"), .string("bc"))
        XCTAssertNotEqual(abC, aBc, "Length-prefixing should prevent ambiguity from naive concatenation")
    }

    // MARK: - Concurrent access

    /// Multiple threads encrypting and decrypting simultaneously should not crash or corrupt data.
    func testConcurrentEncryptDecrypt() throws {
        let (encryptor, _) = try makeEncryptor()
        let plaintext = Data("concurrent-test".utf8)
        let iterations = 100

        let group = DispatchGroup()
        var errors = [Error]()
        let errorLock = NSLock()

        for i in 0..<iterations {
            group.enter()
            DispatchQueue.global().async {
                defer { group.leave() }
                do {
                    let context = CassandraClient.EncryptionContext(
                        keyspace: "test",
                        table: "users",
                        column: "col_\(i % 5)",
                        primaryKey: .init(from: .string("row-\(i)"))
                    )
                    let encrypted = try encryptor.encrypt(plaintext, context: context)
                    let decrypted = try encryptor.decrypt(encrypted, context: context)
                    if decrypted != plaintext {
                        errorLock.lock()
                        errors.append(CassandraClient.Error.decryptionError("Data mismatch on iteration \(i)"))
                        errorLock.unlock()
                    }
                } catch {
                    errorLock.lock()
                    errors.append(error)
                    errorLock.unlock()
                }
            }
        }

        group.wait()
        XCTAssertEqual(errors.count, 0, "Concurrent errors: \(errors)")
    }

    // MARK: - Salt

    /// Encrypt then decrypt with a custom salt should round-trip correctly.
    func testCustomSaltRoundtrip() throws {
        let keyData = randomKey()
        let salt = Data("my-application-salt".utf8)
        let encryptor = try CassandraClient.Encryptor(
            keyMap: ["key-1": keyData],
            currentKeyName: "key-1",
            salt: salt,
            logger: Logger(label: "test.encryptor")
        )
        let context = testContext()
        let plaintext = Data("secret-value".utf8)

        let encrypted = try encryptor.encrypt(plaintext, context: context)
        let decrypted = try encryptor.decrypt(encrypted, context: context)

        XCTAssertEqual(decrypted, plaintext)
    }

    /// Different salts with the same key should produce different ciphertext envelopes
    /// (different KEK → different DEK → different ciphertext).
    func testDifferentSaltProducesDifferentKeys() throws {
        let keyData = randomKey()
        let encryptorA = try CassandraClient.Encryptor(
            keyMap: ["key-1": keyData],
            currentKeyName: "key-1",
            salt: Data("salt-a".utf8),
            logger: Logger(label: "test.encryptor")
        )
        let encryptorB = try CassandraClient.Encryptor(
            keyMap: ["key-1": keyData],
            currentKeyName: "key-1",
            salt: Data("salt-b".utf8),
            logger: Logger(label: "test.encryptor")
        )
        let context = testContext()
        let plaintext = Data("hello".utf8)

        let encryptedA = try encryptorA.encrypt(plaintext, context: context)
        let encryptedB = try encryptorB.encrypt(plaintext, context: context)

        // Both should decrypt with their own encryptor
        XCTAssertEqual(try encryptorA.decrypt(encryptedA, context: context), plaintext)
        XCTAssertEqual(try encryptorB.decrypt(encryptedB, context: context), plaintext)

        // Cross-decryption should fail (different salt → different derived keys)
        XCTAssertThrowsError(try encryptorA.decrypt(encryptedB, context: context))
        XCTAssertThrowsError(try encryptorB.decrypt(encryptedA, context: context))
    }

    // MARK: - Array-based PrimaryKey.init

    /// Array-based PrimaryKey.init produces identical bytes to the variadic init.
    func testArrayBasedPrimaryKeyInit() {
        let variadic = CassandraClient.PrimaryKey(
            from:
                .string("user"),
            .int32(42),
            .uuid(Foundation.UUID(uuidString: "12345678-1234-1234-1234-123456789ABC")!)
        )
        let arrayBased = CassandraClient.PrimaryKey(from: [
            .string("user"),
            .int32(42),
            .uuid(Foundation.UUID(uuidString: "12345678-1234-1234-1234-123456789ABC")!),
        ])
        XCTAssertEqual(variadic, arrayBased)
    }

    // MARK: - EncryptionSchema

    /// Basic construction and registryKey.
    func testEncryptionSchemaConstruction() throws {
        let schema = try CassandraClient.EncryptionSchema(
            keyspace: "prod",
            table: "users",
            keyColumns: [
                .init(name: "user_id", type: .string),
                .init(name: "created_at", type: .int64),
            ],
            encryptedColumns: ["phone_enc", "secret", "ssn"]
        )
        XCTAssertEqual(schema.registryKey, "prod.users")
        XCTAssertEqual(schema.keyColumns.count, 2)
        XCTAssertTrue(schema.encryptedColumns.contains("phone_enc"))
        XCTAssertEqual(schema.encryptedColumns, ["phone_enc", "secret", "ssn"])
    }

    /// Register a schema on Configuration and verify it round-trips through the AnyObject? backing store.
    func testRegisterAndRetrieveSchema() throws {
        var config = CassandraClient.Configuration(
            contactPointsProvider: { callback in callback(.success(["localhost"])) },
            port: 9042,
            protocolVersion: .v3
        )
        let schema = try CassandraClient.EncryptionSchema(
            keyspace: "prod",
            table: "users",
            keyColumns: [
                .init(name: "user_id", type: .string)
            ],
            encryptedColumns: ["ssn"]
        )
        config.registerEncryptionSchema(schema)

        let retrieved = config.encryptionSchemas["prod.users"]
        XCTAssertNotNil(retrieved)
        XCTAssertEqual(retrieved?.keyspace, "prod")
        XCTAssertEqual(retrieved?.table, "users")
        XCTAssertEqual(retrieved?.keyColumns.count, 1)
        XCTAssertEqual(retrieved?.encryptedColumns, ["ssn"])
    }

    /// Key column names must not overlap with encrypted column names.
    func testEncryptionSchemaRejectsKeyColumnOverlap() {
        XCTAssertThrowsError(
            try CassandraClient.EncryptionSchema(
                keyspace: "prod",
                table: "users",
                keyColumns: [.init(name: "user_id", type: .string)],
                encryptedColumns: ["user_id", "ssn"]
            )
        ) { error in
            let description = String(describing: error)
            XCTAssertTrue(description.contains("user_id"), "Error should mention the overlapping column")
        }
    }
}
