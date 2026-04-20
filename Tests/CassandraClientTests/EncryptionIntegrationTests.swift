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
import NIO
import XCTest

@testable import CassandraClient

@available(macOS 15.0, iOS 18.0, visionOS 2.0, *)
final class EncryptionIntegrationTests: XCTestCase {
    var cassandraClient: CassandraClient!
    var configuration: CassandraClient.Configuration!
    var encryptor: CassandraClient.Encryptor!

    private let keyName = "test-key"
    private lazy var keyData: Data = randomKey()

    override func setUp() {
        super.setUp()

        let env = ProcessInfo.processInfo.environment
        let keyspace = env["CASSANDRA_KEYSPACE"] ?? "test"
        self.configuration = CassandraClient.Configuration(
            contactPointsProvider: { callback in
                callback(.success([env["CASSANDRA_HOST"] ?? "127.0.0.1"]))
            },
            port: env["CASSANDRA_CQL_PORT"].flatMap(Int32.init) ?? 9042,
            protocolVersion: .v3
        )
        self.configuration.username = env["CASSANDRA_USER"]
        self.configuration.password = env["CASSANDRA_PASSWORD"]
        self.configuration.keyspace = keyspace
        self.configuration.requestTimeoutMillis = UInt32(24_000)
        self.configuration.connectTimeoutMillis = UInt32(10_000)

        XCTAssertNoThrow(
            self.encryptor = try CassandraClient.Encryptor(
                keyMap: [keyName: keyData],
                currentKeyName: keyName
            )
        )

        self.configuration.encryptor = self.encryptor

        var logger = Logger(label: "test")
        logger.logLevel = .debug

        self.cassandraClient = CassandraClient(configuration: self.configuration, logger: logger)
        XCTAssertNoThrow(
            try self.cassandraClient.withSession(keyspace: .none) { session in
                try session
                    .run(
                        "create keyspace if not exists \(keyspace) with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"
                    )
                    .wait()
            }
        )
    }

    override func tearDown() {
        super.tearDown()
        XCTAssertNoThrow(try self.cassandraClient.shutdown())
        self.cassandraClient = nil
    }

    // MARK: - Write path + manual read path

    /// Insert an encrypted string via Statement, read it back using Column.decryptedString.
    func testWriteAndManualRead() throws {
        let session = self.cassandraClient.makeSession(keyspace: self.configuration.keyspace)
        defer { XCTAssertNoThrow(try session.shutdown()) }

        let tableName = "test_enc_\(DispatchTime.now().uptimeNanoseconds)"

        try session.run("create table \(tableName) (user_id text primary key, secret blob)").wait()

        let userId = "user-1"
        let secretValue = "my-ssn-number"

        let context = CassandraClient.EncryptionContext(
            keyspace: self.configuration.keyspace!,
            table: tableName,
            column: "secret",
            primaryKey: CassandraClient.EncryptionContext.encodeKeyComponents(.string(userId))
        )

        let options = CassandraClient.Statement.Options()

        try session.run(
            "insert into \(tableName) (user_id, secret) values (?, ?)",
            parameters: [
                .string(userId),
                .encryptedString(CassandraClient.Encrypted(secretValue), context: context),
            ],
            options: options
        ).wait()

        // Read back using manual decryption
        let rows = try session.query("select * from \(tableName) where user_id = ?", parameters: [.string(userId)])
            .wait()
        let rowArray = Array(rows)
        XCTAssertEqual(rowArray.count, 1)

        let row = rowArray[0]

        let rawBytes: [UInt8]? = row.column("secret")
        XCTAssertNotNil(rawBytes)
        XCTAssertNotEqual(rawBytes.map { Data($0) }, Data(secretValue.utf8))

        let decrypted = try row.column("secret")?.decryptedString(
            encryptor: self.encryptor,
            context: context
        )
        XCTAssertEqual(decrypted, secretValue)
    }

    // MARK: - All types

    /// Write and read back all encrypted types: String, Int32, Int64, Double, UUID, [UInt8].
    func testAllEncryptedTypes() throws {
        let session = self.cassandraClient.makeSession(keyspace: self.configuration.keyspace)
        defer { XCTAssertNoThrow(try session.shutdown()) }

        let tableName = "test_enc_all_\(DispatchTime.now().uptimeNanoseconds)"

        try session.run(
            "create table \(tableName) (user_id text primary key, enc_name blob, enc_age blob, enc_count blob, enc_score blob, enc_id blob, enc_data blob)"
        ).wait()

        let userId = "user-all"
        let name = "Alice"
        let age: Int32 = 30
        let count: Int64 = 9_876_543_210
        let score: Double = 99.5
        let id = Foundation.UUID()
        let data: [UInt8] = [0xDE, 0xAD, 0xBE, 0xEF]
        let keyspace = self.configuration.keyspace!

        let baseContext = CassandraClient.EncryptionContext.Base(
            keyspace: keyspace,
            table: tableName,
            primaryKey: CassandraClient.EncryptionContext.encodeKeyComponents(.string(userId))
        )

        let options = CassandraClient.Statement.Options()

        try session.run(
            "insert into \(tableName) (user_id, enc_name, enc_age, enc_count, enc_score, enc_id, enc_data) values (?, ?, ?, ?, ?, ?, ?)",
            parameters: [
                .string(userId),
                .encryptedString(CassandraClient.Encrypted(name), context: baseContext.forColumn("enc_name")),
                .encryptedInt32(CassandraClient.Encrypted(age), context: baseContext.forColumn("enc_age")),
                .encryptedInt64(CassandraClient.Encrypted(count), context: baseContext.forColumn("enc_count")),
                .encryptedDouble(CassandraClient.Encrypted(score), context: baseContext.forColumn("enc_score")),
                .encryptedUUID(CassandraClient.Encrypted(id), context: baseContext.forColumn("enc_id")),
                .encryptedBytes(CassandraClient.Encrypted(data), context: baseContext.forColumn("enc_data")),
            ],
            options: options
        ).wait()

        // Read back manually
        let rows = try session.query(
            "select * from \(tableName) where user_id = ?",
            parameters: [.string(userId)]
        ).wait()
        let row = Array(rows)[0]

        XCTAssertEqual(
            try row.column("enc_name")?.decryptedString(
                encryptor: self.encryptor,
                context: baseContext.forColumn("enc_name")
            ),
            name
        )
        XCTAssertEqual(
            try row.column("enc_age")?.decryptedInt32(
                encryptor: self.encryptor,
                context: baseContext.forColumn("enc_age")
            ),
            age
        )
        XCTAssertEqual(
            try row.column("enc_count")?.decryptedInt64(
                encryptor: self.encryptor,
                context: baseContext.forColumn("enc_count")
            ),
            count
        )
        XCTAssertEqual(
            try row.column("enc_score")?.decryptedDouble(
                encryptor: self.encryptor,
                context: baseContext.forColumn("enc_score")
            ),
            score
        )
        XCTAssertEqual(
            try row.column("enc_id")?.decryptedUUID(
                encryptor: self.encryptor,
                context: baseContext.forColumn("enc_id")
            ),
            id
        )
        XCTAssertEqual(
            try row.column("enc_data")?.decryptedBytes(
                encryptor: self.encryptor,
                context: baseContext.forColumn("enc_data")
            ),
            data
        )
    }

    // MARK: - Key rotation + re-encryption

    /// Full lifecycle: write with key-1, rotate to key-2, old data still reads,
    /// re-encrypt old data with key-2, verify it now uses key-2.
    func testKeyRotationAndReEncryption() throws {
        let session = self.cassandraClient.makeSession(keyspace: self.configuration.keyspace)
        defer { XCTAssertNoThrow(try session.shutdown()) }

        let tableName = "test_enc_rotate_\(DispatchTime.now().uptimeNanoseconds)"
        let keyspace = self.configuration.keyspace!

        try session.run("create table \(tableName) (user_id text primary key, secret blob)").wait()

        let userId = "user-rotate"
        let secretValue = "rotate-me"
        let context = CassandraClient.EncryptionContext(
            keyspace: keyspace,
            table: tableName,
            column: "secret",
            primaryKey: CassandraClient.EncryptionContext.encodeKeyComponents(.string(userId))
        )

        // Step 1: Write with key-1 (the encryptor from setUp uses "test-key")
        let options = CassandraClient.Statement.Options()

        try session.run(
            "insert into \(tableName) (user_id, secret) values (?, ?)",
            parameters: [
                .string(userId),
                .encryptedString(CassandraClient.Encrypted(secretValue), context: context),
            ],
            options: options
        ).wait()

        // Step 2: Rotate — add key-2, switch to it
        try self.encryptor.addKey(name: "key-2", secret: randomKey())
        try self.encryptor.setCurrentKeyName("key-2")

        // Step 3: Old data still decrypts (key-1 is still in the map)
        let rows = try session.query(
            "select * from \(tableName) where user_id = ?",
            parameters: [.string(userId)]
        ).wait()
        let row = Array(rows)[0]
        let decrypted = try row.column("secret")?.decryptedString(
            encryptor: self.encryptor,
            context: context
        )
        XCTAssertEqual(decrypted, secretValue)

        // Step 4: Re-encrypt with key-2 (read plaintext, write back encrypted with new key)
        try session.run(
            "insert into \(tableName) (user_id, secret) values (?, ?)",
            parameters: [
                .string(userId),
                .encryptedString(CassandraClient.Encrypted(secretValue), context: context),
            ],
            options: options
        ).wait()

        // Step 5: Verify re-encrypted data still decrypts
        let rows2 = try session.query(
            "select * from \(tableName) where user_id = ?",
            parameters: [.string(userId)]
        ).wait()
        let row2 = Array(rows2)[0]
        let decrypted2 = try row2.column("secret")?.decryptedString(
            encryptor: self.encryptor,
            context: context
        )
        XCTAssertEqual(decrypted2, secretValue)

        // Step 6: Verify the envelope now contains "key-2"
        let rawBytes: [UInt8]? = row2.column("secret")
        let envelope = Data(rawBytes!)
        // Envelope format: [version:1][algorithm:1][key-name-len:1][key-name:N]...
        let keyNameLen = Int(envelope[2])
        let keyName = String(data: envelope[3..<3 + keyNameLen], encoding: .utf8)
        XCTAssertEqual(keyName, "key-2")
    }

    // MARK: - Null encrypted column

    /// A null encrypted column should return nil, not crash.
    func testNullEncryptedColumn() throws {
        let session = self.cassandraClient.makeSession(keyspace: self.configuration.keyspace)
        defer { XCTAssertNoThrow(try session.shutdown()) }

        let tableName = "test_enc_null_\(DispatchTime.now().uptimeNanoseconds)"
        let keyspace = self.configuration.keyspace!

        try session.run("create table \(tableName) (user_id text primary key, secret blob)").wait()

        try session.run(
            "insert into \(tableName) (user_id) values (?)",
            parameters: [.string("user-null")]
        ).wait()

        let rows = try session.query(
            "select * from \(tableName) where user_id = ?",
            parameters: [.string("user-null")]
        ).wait()
        let row = Array(rows)[0]

        let context = CassandraClient.EncryptionContext(
            keyspace: keyspace,
            table: tableName,
            column: "secret",
            primaryKey: CassandraClient.EncryptionContext.encodeKeyComponents(.string("user-null"))
        )

        let decrypted = try row.column("secret")?.decryptedString(
            encryptor: self.encryptor,
            context: context
        )
        XCTAssertNil(decrypted)
    }

    // MARK: - Codable read path

    /// Insert encrypted data, read it back using the Codable path with Encrypted<String>.
    func testCodableDecrypt() throws {
        let session = self.cassandraClient.makeSession(keyspace: self.configuration.keyspace)
        defer { XCTAssertNoThrow(try session.shutdown()) }

        let tableName = "test_enc_codable_\(DispatchTime.now().uptimeNanoseconds)"

        try session.run("create table \(tableName) (user_id text primary key, secret blob)").wait()

        let userId = "user-codable"
        let secretValue = "codable-secret-value"

        let writeContext = CassandraClient.EncryptionContext(
            keyspace: self.configuration.keyspace!,
            table: tableName,
            column: "secret",
            primaryKey: CassandraClient.EncryptionContext.encodeKeyComponents(.string(userId))
        )

        let writeOptions = CassandraClient.Statement.Options()

        try session.run(
            "insert into \(tableName) (user_id, secret) values (?, ?)",
            parameters: [
                .string(userId),
                .encryptedString(CassandraClient.Encrypted(secretValue), context: writeContext),
            ],
            options: writeOptions
        ).wait()

        // Read back using Codable path
        var readOptions = CassandraClient.Statement.Options()
        readOptions.encryptionContextBuilder = { row in
            guard let uid: String = row.column("user_id") else {
                throw CassandraClient.Error.badParams("user_id not found in row")
            }
            return CassandraClient.EncryptionContext.Base(
                keyspace: self.configuration.keyspace!,
                table: tableName,
                primaryKey: CassandraClient.EncryptionContext.encodeKeyComponents(.string(uid))
            )
        }

        let results: [UserWithSecret] = try session.query(
            "select user_id, secret from \(tableName) where user_id = ?",
            parameters: [.string(userId)],
            options: readOptions
        ).wait()

        XCTAssertEqual(results.count, 1)
        XCTAssertEqual(results[0].user_id, userId)
        XCTAssertEqual(results[0].secret.value, secretValue)
    }

    // MARK: - Multiple rows with Codable

    /// Insert 3 rows, read all back via Codable, verify each decrypts with its own primaryKey.
    func testCodableMultipleRows() throws {
        let session = self.cassandraClient.makeSession(keyspace: self.configuration.keyspace)
        defer { XCTAssertNoThrow(try session.shutdown()) }

        let tableName = "test_enc_multi_\(DispatchTime.now().uptimeNanoseconds)"
        let keyspace = self.configuration.keyspace!

        try session.run("create table \(tableName) (user_id text primary key, secret blob)").wait()

        let users = [
            ("alice", "alice-secret"),
            ("bob", "bob-secret"),
            ("charlie", "charlie-secret"),
        ]

        var options = CassandraClient.Statement.Options()

        options.encryptionContextBuilder = { row in
            guard let uid: String = row.column("user_id") else {
                throw CassandraClient.Error.badParams("user_id not found in row")
            }
            return CassandraClient.EncryptionContext.Base(
                keyspace: keyspace,
                table: tableName,
                primaryKey: CassandraClient.EncryptionContext.encodeKeyComponents(.string(uid))
            )
        }

        for (userId, secretValue) in users {
            let context = CassandraClient.EncryptionContext(
                keyspace: keyspace,
                table: tableName,
                column: "secret",
                primaryKey: CassandraClient.EncryptionContext.encodeKeyComponents(.string(userId))
            )
            try session.run(
                "insert into \(tableName) (user_id, secret) values (?, ?)",
                parameters: [
                    .string(userId),
                    .encryptedString(CassandraClient.Encrypted(secretValue), context: context),
                ],
                options: options
            ).wait()
        }

        // Read all back via Codable
        let results: [UserWithSecret] = try session.query(
            "select user_id, secret from \(tableName)",
            options: options
        ).wait()

        XCTAssertEqual(results.count, 3)

        // Sort by user_id since Cassandra doesn't guarantee order
        let sorted = results.sorted { $0.user_id < $1.user_id }
        for (i, (expectedId, expectedSecret)) in users.sorted(by: { $0.0 < $1.0 }).enumerated() {
            XCTAssertEqual(sorted[i].user_id, expectedId)
            XCTAssertEqual(sorted[i].secret.value, expectedSecret)
        }
    }

    // MARK: - Codable with multiple encrypted columns

    /// Struct with two Encrypted fields of different types, decoded via Codable.
    func testCodableMultipleEncryptedColumns() throws {
        let session = self.cassandraClient.makeSession(keyspace: self.configuration.keyspace)
        defer { XCTAssertNoThrow(try session.shutdown()) }

        let tableName = "test_enc_multi_col_\(DispatchTime.now().uptimeNanoseconds)"
        let keyspace = self.configuration.keyspace!

        try session.run(
            "create table \(tableName) (user_id text primary key, secret_name blob, secret_age blob)"
        ).wait()

        let userId = "user-multi-col"
        let name = "Alice"
        let age: Int32 = 30
        let primaryKeyData = CassandraClient.EncryptionContext.encodeKeyComponents(.string(userId))

        let baseContext = CassandraClient.EncryptionContext.Base(
            keyspace: keyspace,
            table: tableName,
            primaryKey: primaryKeyData
        )

        var options = CassandraClient.Statement.Options()

        options.encryptionContextBuilder = { row in
            guard let uid: String = row.column("user_id") else {
                throw CassandraClient.Error.badParams("user_id not found in row")
            }
            return CassandraClient.EncryptionContext.Base(
                keyspace: keyspace,
                table: tableName,
                primaryKey: CassandraClient.EncryptionContext.encodeKeyComponents(.string(uid))
            )
        }

        try session.run(
            "insert into \(tableName) (user_id, secret_name, secret_age) values (?, ?, ?)",
            parameters: [
                .string(userId),
                .encryptedString(CassandraClient.Encrypted(name), context: baseContext.forColumn("secret_name")),
                .encryptedInt32(CassandraClient.Encrypted(age), context: baseContext.forColumn("secret_age")),
            ],
            options: options
        ).wait()

        // Read back via Codable
        let results: [UserWithMultipleSecrets] = try session.query(
            "select user_id, secret_name, secret_age from \(tableName) where user_id = ?",
            parameters: [.string(userId)],
            options: options
        ).wait()

        XCTAssertEqual(results.count, 1)
        XCTAssertEqual(results[0].user_id, userId)
        XCTAssertEqual(results[0].secret_name.value, name)
        XCTAssertEqual(results[0].secret_age.value, age)
    }
}

// MARK: - Test model

struct UserWithSecret: Decodable {
    let user_id: String
    let secret: CassandraClient.Encrypted<String>
}

struct UserWithMultipleSecrets: Decodable {
    let user_id: String
    let secret_name: CassandraClient.Encrypted<String>
    let secret_age: CassandraClient.Encrypted<Int32>
}
