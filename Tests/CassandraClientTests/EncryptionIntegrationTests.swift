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
                currentKeyName: keyName,
                salt: Data("integration-test-salt".utf8),
                logger: Logger(label: "test.encryptor")
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

    /// Shutdown and recreate the client after configuration changes (e.g. registering schemas).
    private func recreateClient() {
        XCTAssertNoThrow(try self.cassandraClient.shutdown())
        var logger = Logger(label: "test")
        logger.logLevel = .debug
        self.cassandraClient = CassandraClient(configuration: self.configuration, logger: logger)
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
            primaryKey: .init(from: .string(userId))
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
            primaryKey: .init(from: .string(userId))
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
            primaryKey: .init(from: .string(userId))
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
            primaryKey: .init(from: .string("user-null"))
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
            primaryKey: .init(from: .string(userId))
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
                primaryKey: .init(from: .string(uid))
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
                primaryKey: .init(from: .string(uid))
            )
        }

        for (userId, secretValue) in users {
            let context = CassandraClient.EncryptionContext(
                keyspace: keyspace,
                table: tableName,
                column: "secret",
                primaryKey: .init(from: .string(userId))
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
        let primaryKeyData = CassandraClient.PrimaryKey(from: .string(userId))

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
                primaryKey: .init(from: .string(uid))
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

    // MARK: - Column registration

    /// No clustering key, one encrypted regular column.
    /// Verify decryption works without an encryptionContextBuilder — uses column registration instead.
    func testColumnRegistrationSimple() throws {
        let tableName = "test_colreg_simple_\(DispatchTime.now().uptimeNanoseconds)"
        let keyspace = self.configuration.keyspace!

        // Create table using a session from the current client
        do {
            let session = self.cassandraClient.makeSession(keyspace: self.configuration.keyspace)
            defer { XCTAssertNoThrow(try session.shutdown()) }
            try session.run("create table \(tableName) (user_id text primary key, secret blob)").wait()
        }

        // Register the schema
        let schema = try CassandraClient.EncryptionSchema(
            keyspace: keyspace,
            table: tableName,
            keyColumns: [.init(name: "user_id", type: .string)],
            encryptedColumns: ["secret"]
        )
        self.configuration.registerEncryptionSchema(schema)

        // Recreate client with updated configuration
        self.recreateClient()

        let userId = "user-reg"
        let secretValue = "registered-secret"
        let baseContext = CassandraClient.EncryptionContext.Base(
            keyspace: keyspace,
            table: tableName,
            primaryKey: .init(from: .string(userId))
        )

        // Write with explicit context using a new session
        let session = self.cassandraClient.makeSession(keyspace: self.configuration.keyspace)
        defer { XCTAssertNoThrow(try session.shutdown()) }

        try session.run(
            "insert into \(tableName) (user_id, secret) values (?, ?)",
            parameters: [
                .string(userId),
                .encryptedString(CassandraClient.Encrypted(secretValue), context: baseContext.forColumn("secret")),
            ]
        ).wait()

        // Read via column registration — no encryptionContextBuilder needed
        var readOptions = CassandraClient.Statement.Options()
        readOptions.encryptionTable = tableName

        let results: [UserWithSecret] = try self.cassandraClient.query(
            "select user_id, secret from \(tableName) where user_id = ?",
            parameters: [.string(userId)],
            options: readOptions
        ).wait()

        XCTAssertEqual(results.count, 1)
        XCTAssertEqual(results[0].user_id, userId)
        XCTAssertEqual(results[0].secret.value, secretValue)
    }

    /// Prepared statement read path with column registration — execute<T>(prepared:) decodes via makeDecoder.
    func testColumnRegistrationWithPreparedStatement() async throws {
        let tableName = "test_colreg_prep_\(DispatchTime.now().uptimeNanoseconds)"
        let keyspace = self.configuration.keyspace!

        // Create table using a session from the current client
        do {
            let session = self.cassandraClient.makeSession(keyspace: self.configuration.keyspace)
            defer { XCTAssertNoThrow(try session.shutdown()) }
            try session.run("create table \(tableName) (user_id text primary key, secret blob)").wait()
        }

        let schema = try CassandraClient.EncryptionSchema(
            keyspace: keyspace,
            table: tableName,
            keyColumns: [.init(name: "user_id", type: .string)],
            encryptedColumns: ["secret"]
        )
        self.configuration.registerEncryptionSchema(schema)

        self.recreateClient()

        let userId = "user-prep"
        let secretValue = "prepared-secret"
        let baseContext = CassandraClient.EncryptionContext.Base(
            keyspace: keyspace,
            table: tableName,
            primaryKey: .init(from: .string(userId))
        )

        // Write using a prepared statement with explicit context
        let insertStmt = try await self.cassandraClient.prepare(
            "insert into \(tableName) (user_id, secret) values (?, ?)"
        )
        _ = try await self.cassandraClient.execute(
            prepared: insertStmt,
            parameters: [
                .string(userId),
                .encryptedString(CassandraClient.Encrypted(secretValue), context: baseContext.forColumn("secret")),
            ]
        )

        // Read using a prepared statement with column registration
        let selectStmt = try await self.cassandraClient.prepare(
            "select user_id, secret from \(tableName) where user_id = ?"
        )
        var readOptions = CassandraClient.Statement.Options()
        readOptions.encryptionTable = tableName

        let results: [UserWithSecret] = try await self.cassandraClient.execute(
            prepared: selectStmt,
            parameters: [.string(userId)],
            options: readOptions
        )

        XCTAssertEqual(results.count, 1)
        XCTAssertEqual(results[0].user_id, userId)
        XCTAssertEqual(results[0].secret.value, secretValue)
    }

    /// Both encryptionContextBuilder and encryptionTable are set — builder wins.
    func testColumnRegistrationBuilderTakesPrecedence() throws {
        let tableName = "test_colreg_prec_\(DispatchTime.now().uptimeNanoseconds)"
        let keyspace = self.configuration.keyspace!

        // Create table using a session from the current client
        do {
            let session = self.cassandraClient.makeSession(keyspace: self.configuration.keyspace)
            defer { XCTAssertNoThrow(try session.shutdown()) }
            try session.run("create table \(tableName) (user_id text primary key, secret blob)").wait()
        }

        // Register schema and recreate client
        let schema = try CassandraClient.EncryptionSchema(
            keyspace: keyspace,
            table: tableName,
            keyColumns: [.init(name: "user_id", type: .string)],
            encryptedColumns: ["secret"]
        )
        self.configuration.registerEncryptionSchema(schema)

        self.recreateClient()

        let userId = "user-prec"
        let secretValue = "precedence-secret"
        let baseContext = CassandraClient.EncryptionContext.Base(
            keyspace: keyspace,
            table: tableName,
            primaryKey: .init(from: .string(userId))
        )

        // Insert using the new client's session
        let session = self.cassandraClient.makeSession(keyspace: self.configuration.keyspace)
        defer { XCTAssertNoThrow(try session.shutdown()) }

        try session.run(
            "insert into \(tableName) (user_id, secret) values (?, ?)",
            parameters: [
                .string(userId),
                .encryptedString(CassandraClient.Encrypted(secretValue), context: baseContext.forColumn("secret")),
            ]
        ).wait()

        // Set both builder and encryptionTable — builder should win
        var readOptions = CassandraClient.Statement.Options()
        readOptions.encryptionTable = tableName
        readOptions.encryptionContextBuilder = { row in
            guard let uid: String = row.column("user_id") else {
                throw CassandraClient.Error.badParams("user_id not found in row")
            }
            return CassandraClient.EncryptionContext.Base(
                keyspace: keyspace,
                table: tableName,
                primaryKey: .init(from: .string(uid))
            )
        }

        let results: [UserWithSecret] = try self.cassandraClient.query(
            "select user_id, secret from \(tableName) where user_id = ?",
            parameters: [.string(userId)],
            options: readOptions
        ).wait()

        XCTAssertEqual(results.count, 1)
        XCTAssertEqual(results[0].secret.value, secretValue)
    }

    /// encryptionTable set but no matching schema registered → throws encryptionConfigError.
    func testColumnRegistrationMissingSchema() throws {
        let tableName = "test_colreg_missing_\(DispatchTime.now().uptimeNanoseconds)"

        // Create the table and insert a row so the decoder is invoked
        do {
            let session = self.cassandraClient.makeSession(keyspace: self.configuration.keyspace)
            defer { XCTAssertNoThrow(try session.shutdown()) }
            try session.run("create table \(tableName) (user_id text primary key, secret blob)").wait()
            try session.run(
                "insert into \(tableName) (user_id, secret) values (?, ?)",
                parameters: [.string("user-1"), .bytes([0x01, 0x02])]
            ).wait()
        }

        // Do NOT register a schema — that's what we're testing
        var readOptions = CassandraClient.Statement.Options()
        readOptions.encryptionTable = tableName

        do {
            let _: [UserWithSecret] = try self.cassandraClient.query(
                "select user_id, secret from \(tableName)",
                options: readOptions
            ).wait()
            XCTFail("Expected encryptionConfigError to be thrown")
        } catch let error as CassandraClient.Error {
            let msg = error.description
            XCTAssertTrue(
                msg.contains("No EncryptionSchema registered"),
                "Unexpected message: \(msg)"
            )
        }
    }

    // MARK: - Write-path enforcement

    /// Binding a plaintext value to a column registered as encrypted should throw.
    func testWritePathRejectsPlaintextForEncryptedColumn() async throws {
        let tableName = "test_wp_reject_pt_\(DispatchTime.now().uptimeNanoseconds)"
        let keyspace = self.configuration.keyspace!

        do {
            let session = self.cassandraClient.makeSession(keyspace: self.configuration.keyspace)
            defer { XCTAssertNoThrow(try session.shutdown()) }
            try session.run("create table \(tableName) (user_id text primary key, secret blob)").wait()
        }

        let schema = try CassandraClient.EncryptionSchema(
            keyspace: keyspace,
            table: tableName,
            keyColumns: [.init(name: "user_id", type: .string)],
            encryptedColumns: ["secret"]
        )
        self.configuration.registerEncryptionSchema(schema)

        self.recreateClient()

        let prepared = try await self.cassandraClient.prepare(
            "insert into \(tableName) (user_id, secret) values (?, ?)"
        )

        var options = CassandraClient.Statement.Options()
        options.encryptionTable = tableName

        do {
            // Bind plaintext bytes to an encrypted column — should be rejected
            _ = try await self.cassandraClient.execute(
                prepared: prepared,
                parameters: [.string("user-1"), .bytes([0x01, 0x02])],
                options: options
            )
            XCTFail("Expected encryptionConfigError for plaintext value on encrypted column")
        } catch let error as CassandraClient.Error {
            let msg = error.description
            XCTAssertTrue(msg.contains("secret"), "Error should mention column name: \(msg)")
        }
    }

    /// Binding an encrypted value to a column NOT registered as encrypted should throw.
    func testWritePathRejectsEncryptedForPlaintextColumn() async throws {
        let tableName = "test_wp_reject_enc_\(DispatchTime.now().uptimeNanoseconds)"
        let keyspace = self.configuration.keyspace!

        do {
            let session = self.cassandraClient.makeSession(keyspace: self.configuration.keyspace)
            defer { XCTAssertNoThrow(try session.shutdown()) }
            try session.run("create table \(tableName) (user_id text primary key, name text)").wait()
        }

        // Schema has no encrypted columns — only PK
        let schema = try CassandraClient.EncryptionSchema(
            keyspace: keyspace,
            table: tableName,
            keyColumns: [.init(name: "user_id", type: .string)]
        )
        self.configuration.registerEncryptionSchema(schema)

        self.recreateClient()

        let prepared = try await self.cassandraClient.prepare(
            "insert into \(tableName) (user_id, name) values (?, ?)"
        )

        let baseContext = CassandraClient.EncryptionContext.Base(
            keyspace: keyspace,
            table: tableName,
            primaryKey: .init(from: .string("user-1"))
        )

        var options = CassandraClient.Statement.Options()
        options.encryptionTable = tableName

        do {
            // Bind encrypted value to a plaintext column — should be rejected
            _ = try await self.cassandraClient.execute(
                prepared: prepared,
                parameters: [
                    .string("user-1"),
                    .encryptedString(CassandraClient.Encrypted("oops"), context: baseContext.forColumn("name")),
                ],
                options: options
            )
            XCTFail("Expected encryptionConfigError for encrypted value on plaintext column")
        } catch let error as CassandraClient.Error {
            let msg = error.description
            XCTAssertTrue(msg.contains("name"), "Error should mention column name: \(msg)")
        }
    }

    // MARK: - Auto context inference tests (metadata-based)

    /// Auto-inferred context: write with .encryptedString (no context) and read back successfully.
    func testAutoContextInferenceBasic() async throws {
        let tableName = "test_auto_ctx_\(DispatchTime.now().uptimeNanoseconds)"
        let keyspace = self.configuration.keyspace!

        do {
            let session = self.cassandraClient.makeSession(keyspace: self.configuration.keyspace)
            defer { XCTAssertNoThrow(try session.shutdown()) }
            try session.run("create table \(tableName) (user_id text primary key, secret blob)").wait()
        }

        let schema = try CassandraClient.EncryptionSchema(
            keyspace: keyspace,
            table: tableName,
            keyColumns: [.init(name: "user_id", type: .string)],
            encryptedColumns: ["secret"]
        )
        self.configuration.registerEncryptionSchema(schema)
        self.recreateClient()

        let userId = "user-auto"
        let secretValue = "auto-secret"

        let insertStmt = try await self.cassandraClient.prepare(
            "insert into \(tableName) (user_id, secret) values (?, ?)"
        )

        var options = CassandraClient.Statement.Options()
        options.encryptionTable = tableName

        _ = try await self.cassandraClient.execute(
            prepared: insertStmt,
            parameters: [
                .string(userId),
                .encryptedString(CassandraClient.Encrypted(secretValue)),
            ],
            options: options
        )

        let selectStmt = try await self.cassandraClient.prepare(
            "select user_id, secret from \(tableName) where user_id = ?"
        )

        let results: [UserWithSecret] = try await self.cassandraClient.execute(
            prepared: selectStmt,
            parameters: [.string(userId)],
            options: options
        )

        XCTAssertEqual(results.count, 1)
        XCTAssertEqual(results[0].user_id, userId)
        XCTAssertEqual(results[0].secret.value, secretValue)
    }

    /// Auto-inferred context with composite primary key (partition + clustering).
    func testAutoContextInferenceCompositeKey() async throws {
        let tableName = "test_auto_ctx_comp_\(DispatchTime.now().uptimeNanoseconds)"
        let keyspace = self.configuration.keyspace!

        do {
            let session = self.cassandraClient.makeSession(keyspace: self.configuration.keyspace)
            defer { XCTAssertNoThrow(try session.shutdown()) }
            try session.run(
                "create table \(tableName) (partition_id text, cluster_id int, secret blob, PRIMARY KEY (partition_id, cluster_id))"
            ).wait()
        }

        let schema = try CassandraClient.EncryptionSchema(
            keyspace: keyspace,
            table: tableName,
            keyColumns: [
                .init(name: "partition_id", type: .string),
                .init(name: "cluster_id", type: .int32),
            ],
            encryptedColumns: ["secret"]
        )
        self.configuration.registerEncryptionSchema(schema)
        self.recreateClient()

        let partitionId = "part-1"
        let clusterId: Int32 = 42
        let secretValue = "composite-secret"

        let insertStmt = try await self.cassandraClient.prepare(
            "insert into \(tableName) (partition_id, cluster_id, secret) values (?, ?, ?)"
        )

        var options = CassandraClient.Statement.Options()
        options.encryptionTable = tableName

        _ = try await self.cassandraClient.execute(
            prepared: insertStmt,
            parameters: [
                .string(partitionId),
                .int32(clusterId),
                .encryptedString(CassandraClient.Encrypted(secretValue)),
            ],
            options: options
        )

        let selectStmt = try await self.cassandraClient.prepare(
            "select partition_id, cluster_id, secret from \(tableName) where partition_id = ? and cluster_id = ?"
        )

        let results: [CompositeKeyUser] = try await self.cassandraClient.execute(
            prepared: selectStmt,
            parameters: [.string(partitionId), .int32(clusterId)],
            options: options
        )

        XCTAssertEqual(results.count, 1)
        XCTAssertEqual(results[0].secret.value, secretValue)
    }

    /// Auto-inferred context with multiple encrypted columns.
    func testAutoContextInferenceMultipleEncryptedColumns() async throws {
        let tableName = "test_auto_ctx_multi_\(DispatchTime.now().uptimeNanoseconds)"
        let keyspace = self.configuration.keyspace!

        do {
            let session = self.cassandraClient.makeSession(keyspace: self.configuration.keyspace)
            defer { XCTAssertNoThrow(try session.shutdown()) }
            try session.run(
                "create table \(tableName) (user_id text primary key, secret_name blob, secret_age blob)"
            ).wait()
        }

        let schema = try CassandraClient.EncryptionSchema(
            keyspace: keyspace,
            table: tableName,
            keyColumns: [.init(name: "user_id", type: .string)],
            encryptedColumns: ["secret_name", "secret_age"]
        )
        self.configuration.registerEncryptionSchema(schema)
        self.recreateClient()

        let userId = "user-multi"
        let nameValue = "Alice"
        let ageValue: Int32 = 30

        let insertStmt = try await self.cassandraClient.prepare(
            "insert into \(tableName) (user_id, secret_name, secret_age) values (?, ?, ?)"
        )

        var options = CassandraClient.Statement.Options()
        options.encryptionTable = tableName

        _ = try await self.cassandraClient.execute(
            prepared: insertStmt,
            parameters: [
                .string(userId),
                .encryptedString(CassandraClient.Encrypted(nameValue)),
                .encryptedInt32(CassandraClient.Encrypted(ageValue)),
            ],
            options: options
        )

        let selectStmt = try await self.cassandraClient.prepare(
            "select user_id, secret_name, secret_age from \(tableName) where user_id = ?"
        )

        let results: [UserWithMultipleSecrets] = try await self.cassandraClient.execute(
            prepared: selectStmt,
            parameters: [.string(userId)],
            options: options
        )

        XCTAssertEqual(results.count, 1)
        XCTAssertEqual(results[0].secret_name.value, nameValue)
        XCTAssertEqual(results[0].secret_age.value, ageValue)
    }

    /// Mixed: one encrypted column with explicit context, another auto-inferred.
    func testAutoContextInferenceMixedExplicitAndAuto() async throws {
        let tableName = "test_auto_ctx_mixed_\(DispatchTime.now().uptimeNanoseconds)"
        let keyspace = self.configuration.keyspace!

        do {
            let session = self.cassandraClient.makeSession(keyspace: self.configuration.keyspace)
            defer { XCTAssertNoThrow(try session.shutdown()) }
            try session.run(
                "create table \(tableName) (user_id text primary key, secret_name blob, secret_age blob)"
            ).wait()
        }

        let schema = try CassandraClient.EncryptionSchema(
            keyspace: keyspace,
            table: tableName,
            keyColumns: [.init(name: "user_id", type: .string)],
            encryptedColumns: ["secret_name", "secret_age"]
        )
        self.configuration.registerEncryptionSchema(schema)
        self.recreateClient()

        let userId = "user-mixed"
        let nameValue = "Alice"
        let ageValue: Int32 = 30
        let baseContext = CassandraClient.EncryptionContext.Base(
            keyspace: keyspace,
            table: tableName,
            primaryKey: .init(from: .string(userId))
        )

        let insertStmt = try await self.cassandraClient.prepare(
            "insert into \(tableName) (user_id, secret_name, secret_age) values (?, ?, ?)"
        )

        var options = CassandraClient.Statement.Options()
        options.encryptionTable = tableName

        _ = try await self.cassandraClient.execute(
            prepared: insertStmt,
            parameters: [
                .string(userId),
                .encryptedString(CassandraClient.Encrypted(nameValue), context: baseContext.forColumn("secret_name")),
                .encryptedInt32(CassandraClient.Encrypted(ageValue)),
            ],
            options: options
        )

        let selectStmt = try await self.cassandraClient.prepare(
            "select user_id, secret_name, secret_age from \(tableName) where user_id = ?"
        )

        let results: [UserWithMultipleSecrets] = try await self.cassandraClient.execute(
            prepared: selectStmt,
            parameters: [.string(userId)],
            options: options
        )

        XCTAssertEqual(results.count, 1)
        XCTAssertEqual(results[0].secret_name.value, nameValue)
        XCTAssertEqual(results[0].secret_age.value, ageValue)
    }

    /// Missing PK column in parameters should produce a clear error.
    func testAutoContextInferenceErrorsOnMissingPKColumn() async throws {
        let tableName = "test_auto_ctx_missing_pk_\(DispatchTime.now().uptimeNanoseconds)"
        let keyspace = self.configuration.keyspace!

        do {
            let session = self.cassandraClient.makeSession(keyspace: self.configuration.keyspace)
            defer { XCTAssertNoThrow(try session.shutdown()) }
            try session.run(
                "create table \(tableName) (part_id text, clust_id int, secret blob, PRIMARY KEY (part_id, clust_id))"
            ).wait()
        }

        let schema = try CassandraClient.EncryptionSchema(
            keyspace: keyspace,
            table: tableName,
            keyColumns: [
                .init(name: "part_id", type: .string),
                .init(name: "clust_id", type: .int32),
            ],
            encryptedColumns: ["secret"]
        )
        self.configuration.registerEncryptionSchema(schema)
        self.recreateClient()

        // part_id is hardcoded in the query — not a bind parameter
        let updateStmt = try await self.cassandraClient.prepare(
            "update \(tableName) set secret = ? where part_id = 'fixed' and clust_id = ?"
        )

        var options = CassandraClient.Statement.Options()
        options.encryptionTable = tableName

        do {
            _ = try await self.cassandraClient.execute(
                prepared: updateStmt,
                parameters: [
                    .encryptedString(CassandraClient.Encrypted("value")),
                    .int32(1),
                ],
                options: options
            )
            XCTFail("Expected error for missing PK column")
        } catch let error as CassandraClient.Error {
            let msg = error.description
            XCTAssertTrue(msg.contains("part_id"), "Error should mention missing key column: \(msg)")
        }
    }

    /// NULL PK value should produce a clear error.
    func testAutoContextInferenceErrorsOnNullPKValue() async throws {
        let tableName = "test_auto_ctx_null_pk_\(DispatchTime.now().uptimeNanoseconds)"
        let keyspace = self.configuration.keyspace!

        do {
            let session = self.cassandraClient.makeSession(keyspace: self.configuration.keyspace)
            defer { XCTAssertNoThrow(try session.shutdown()) }
            try session.run("create table \(tableName) (user_id text primary key, secret blob)").wait()
        }

        let schema = try CassandraClient.EncryptionSchema(
            keyspace: keyspace,
            table: tableName,
            keyColumns: [.init(name: "user_id", type: .string)],
            encryptedColumns: ["secret"]
        )
        self.configuration.registerEncryptionSchema(schema)
        self.recreateClient()

        let insertStmt = try await self.cassandraClient.prepare(
            "insert into \(tableName) (user_id, secret) values (?, ?)"
        )

        var options = CassandraClient.Statement.Options()
        options.encryptionTable = tableName

        do {
            _ = try await self.cassandraClient.execute(
                prepared: insertStmt,
                parameters: [
                    .null,
                    .encryptedString(CassandraClient.Encrypted("value")),
                ],
                options: options
            )
            XCTFail("Expected error for null PK value")
        } catch let error as CassandraClient.Error {
            let msg = error.description
            XCTAssertTrue(msg.contains("user_id"), "Error should mention the key column: \(msg)")
        }
    }

    /// Two rows with different PKs: each gets its own context and decrypts independently.
    func testAutoContextInferenceDistinctKeysPerRow() async throws {
        let tableName = "test_auto_ctx_distinct_\(DispatchTime.now().uptimeNanoseconds)"
        let keyspace = self.configuration.keyspace!

        do {
            let session = self.cassandraClient.makeSession(keyspace: self.configuration.keyspace)
            defer { XCTAssertNoThrow(try session.shutdown()) }
            try session.run("create table \(tableName) (user_id text primary key, secret blob)").wait()
        }

        let schema = try CassandraClient.EncryptionSchema(
            keyspace: keyspace,
            table: tableName,
            keyColumns: [.init(name: "user_id", type: .string)],
            encryptedColumns: ["secret"]
        )
        self.configuration.registerEncryptionSchema(schema)
        self.recreateClient()

        let insertStmt = try await self.cassandraClient.prepare(
            "insert into \(tableName) (user_id, secret) values (?, ?)"
        )

        var options = CassandraClient.Statement.Options()
        options.encryptionTable = tableName

        _ = try await self.cassandraClient.execute(
            prepared: insertStmt,
            parameters: [
                .string("alice"),
                .encryptedString(CassandraClient.Encrypted("alice-secret")),
            ],
            options: options
        )

        _ = try await self.cassandraClient.execute(
            prepared: insertStmt,
            parameters: [
                .string("bob"),
                .encryptedString(CassandraClient.Encrypted("bob-secret")),
            ],
            options: options
        )

        let selectStmt = try await self.cassandraClient.prepare(
            "select user_id, secret from \(tableName) where user_id = ?"
        )

        let aliceResults: [UserWithSecret] = try await self.cassandraClient.execute(
            prepared: selectStmt,
            parameters: [.string("alice")],
            options: options
        )
        XCTAssertEqual(aliceResults.count, 1)
        XCTAssertEqual(aliceResults[0].secret.value, "alice-secret")

        let bobResults: [UserWithSecret] = try await self.cassandraClient.execute(
            prepared: selectStmt,
            parameters: [.string("bob")],
            options: options
        )
        XCTAssertEqual(bobResults.count, 1)
        XCTAssertEqual(bobResults[0].secret.value, "bob-secret")
    }

    /// No encryptionTable in options — context-less encrypted value should error at bind time.
    func testAutoContextInferenceErrorsWithoutEncryptionTable() async throws {
        let tableName = "test_auto_ctx_no_table_\(DispatchTime.now().uptimeNanoseconds)"

        do {
            let session = self.cassandraClient.makeSession(keyspace: self.configuration.keyspace)
            defer { XCTAssertNoThrow(try session.shutdown()) }
            try session.run("create table \(tableName) (user_id text primary key, secret blob)").wait()
        }

        let insertStmt = try await self.cassandraClient.prepare(
            "insert into \(tableName) (user_id, secret) values (?, ?)"
        )

        do {
            _ = try await self.cassandraClient.execute(
                prepared: insertStmt,
                parameters: [
                    .string("user-1"),
                    .encryptedString(CassandraClient.Encrypted("value")),
                ]
            )
            XCTFail("Expected error for context-less encrypted value without encryptionTable")
        } catch let error as CassandraClient.Error {
            let msg = error.description
            XCTAssertTrue(msg.contains("no encryption context"), "Error should explain missing context: \(msg)")
        }
    }

    // MARK: - Batch + encryption tests

    /// Batch insert multiple rows with encrypted values, read back and verify each decrypts correctly.
    func testBatchEncryptedPreparedStatements() async throws {
        let tableName = "test_batch_enc_\(DispatchTime.now().uptimeNanoseconds)"
        let keyspace = self.configuration.keyspace!

        do {
            let session = self.cassandraClient.makeSession(keyspace: self.configuration.keyspace)
            defer { XCTAssertNoThrow(try session.shutdown()) }
            try session.run("create table \(tableName) (user_id text primary key, secret blob)").wait()
        }

        let schema = try CassandraClient.EncryptionSchema(
            keyspace: keyspace,
            table: tableName,
            keyColumns: [.init(name: "user_id", type: .string)],
            encryptedColumns: ["secret"]
        )
        self.configuration.registerEncryptionSchema(schema)
        self.recreateClient()

        let insertStmt = try await self.cassandraClient.prepare(
            "insert into \(tableName) (user_id, secret) values (?, ?)",
            encryptionTable: tableName
        )

        try await self.cassandraClient.batch { batch in
            try batch.add(
                prepared: insertStmt,
                parameters: [
                    .string("alice"),
                    .encryptedString(CassandraClient.Encrypted("alice-secret")),
                ]
            )
            try batch.add(
                prepared: insertStmt,
                parameters: [
                    .string("bob"),
                    .encryptedString(CassandraClient.Encrypted("bob-secret")),
                ]
            )
            try batch.add(
                prepared: insertStmt,
                parameters: [
                    .string("carol"),
                    .encryptedString(CassandraClient.Encrypted("carol-secret")),
                ]
            )
        }

        let selectStmt = try await self.cassandraClient.prepare(
            "select user_id, secret from \(tableName) where user_id = ?",
            encryptionTable: tableName
        )

        for (userId, expected) in [("alice", "alice-secret"), ("bob", "bob-secret"), ("carol", "carol-secret")] {
            let results: [UserWithSecret] = try await self.cassandraClient.execute(
                prepared: selectStmt,
                parameters: [.string(userId)]
            )
            XCTAssertEqual(results.count, 1)
            XCTAssertEqual(results[0].secret.value, expected)
        }
    }

    /// Batch with composite key — each row gets its own encryption context based on its PK values.
    func testBatchEncryptedCompositeKey() async throws {
        let tableName = "test_batch_enc_comp_\(DispatchTime.now().uptimeNanoseconds)"
        let keyspace = self.configuration.keyspace!

        do {
            let session = self.cassandraClient.makeSession(keyspace: self.configuration.keyspace)
            defer { XCTAssertNoThrow(try session.shutdown()) }
            try session.run(
                "create table \(tableName) (partition_id text, cluster_id int, secret blob, PRIMARY KEY (partition_id, cluster_id))"
            ).wait()
        }

        let schema = try CassandraClient.EncryptionSchema(
            keyspace: keyspace,
            table: tableName,
            keyColumns: [
                .init(name: "partition_id", type: .string),
                .init(name: "cluster_id", type: .int32),
            ],
            encryptedColumns: ["secret"]
        )
        self.configuration.registerEncryptionSchema(schema)
        self.recreateClient()

        let insertStmt = try await self.cassandraClient.prepare(
            "insert into \(tableName) (partition_id, cluster_id, secret) values (?, ?, ?)"
        )

        var options = CassandraClient.Statement.Options()
        options.encryptionTable = tableName

        try await self.cassandraClient.batch { batch in
            try batch.add(
                prepared: insertStmt,
                parameters: [
                    .string("part-1"), .int32(1),
                    .encryptedString(CassandraClient.Encrypted("secret-1-1")),
                ],
                options: options
            )
            try batch.add(
                prepared: insertStmt,
                parameters: [
                    .string("part-1"), .int32(2),
                    .encryptedString(CassandraClient.Encrypted("secret-1-2")),
                ],
                options: options
            )
            try batch.add(
                prepared: insertStmt,
                parameters: [
                    .string("part-2"), .int32(1),
                    .encryptedString(CassandraClient.Encrypted("secret-2-1")),
                ],
                options: options
            )
        }

        let selectStmt = try await self.cassandraClient.prepare(
            "select partition_id, cluster_id, secret from \(tableName) where partition_id = ? and cluster_id = ?"
        )

        let r1: [CompositeKeyUser] = try await self.cassandraClient.execute(
            prepared: selectStmt,
            parameters: [.string("part-1"), .int32(1)],
            options: options
        )
        XCTAssertEqual(r1.count, 1)
        XCTAssertEqual(r1[0].secret.value, "secret-1-1")

        let r2: [CompositeKeyUser] = try await self.cassandraClient.execute(
            prepared: selectStmt,
            parameters: [.string("part-1"), .int32(2)],
            options: options
        )
        XCTAssertEqual(r2.count, 1)
        XCTAssertEqual(r2[0].secret.value, "secret-1-2")

        let r3: [CompositeKeyUser] = try await self.cassandraClient.execute(
            prepared: selectStmt,
            parameters: [.string("part-2"), .int32(1)],
            options: options
        )
        XCTAssertEqual(r3.count, 1)
        XCTAssertEqual(r3[0].secret.value, "secret-2-1")
    }

    /// Batch with non-encrypted prepared statement — resolver skips resolution, works normally.
    func testBatchPreparedNoEncryption() async throws {
        let tableName = "test_batch_no_enc_\(DispatchTime.now().uptimeNanoseconds)"

        do {
            let session = self.cassandraClient.makeSession(keyspace: self.configuration.keyspace)
            defer { XCTAssertNoThrow(try session.shutdown()) }
            try session.run("create table \(tableName) (id text primary key, value text)").wait()
        }

        let insertStmt = try await self.cassandraClient.prepare(
            "insert into \(tableName) (id, value) values (?, ?)"
        )

        try await self.cassandraClient.batch { batch in
            try batch.add(prepared: insertStmt, parameters: [.string("a"), .string("val-a")])
            try batch.add(prepared: insertStmt, parameters: [.string("b"), .string("val-b")])
        }

        let selectStmt = try await self.cassandraClient.prepare(
            "select id, value from \(tableName) where id = ?"
        )

        let rows = try await self.cassandraClient.execute(
            prepared: selectStmt,
            parameters: [.string("a")]
        )
        let rowArray = Array(rows)
        XCTAssertEqual(rowArray.count, 1)
        let value: String? = rowArray[0].column("value")
        XCTAssertEqual(value, "val-a")
    }

    /// Batch mixing different prepared statements: two encrypted tables, one plaintext table.
    func testBatchMixedEncryptedAndPlaintextTables() async throws {
        let encTable1 = "test_batch_mix_enc1_\(DispatchTime.now().uptimeNanoseconds)"
        let encTable2 = "test_batch_mix_enc2_\(DispatchTime.now().uptimeNanoseconds)"
        let plainTable = "test_batch_mix_plain_\(DispatchTime.now().uptimeNanoseconds)"
        let keyspace = self.configuration.keyspace!

        do {
            let session = self.cassandraClient.makeSession(keyspace: self.configuration.keyspace)
            defer { XCTAssertNoThrow(try session.shutdown()) }
            try session.run("create table \(encTable1) (user_id text primary key, secret blob)").wait()
            try session.run("create table \(encTable2) (item_id text primary key, data blob)").wait()
            try session.run("create table \(plainTable) (id text primary key, value text)").wait()
        }

        let schema1 = try CassandraClient.EncryptionSchema(
            keyspace: keyspace,
            table: encTable1,
            keyColumns: [.init(name: "user_id", type: .string)],
            encryptedColumns: ["secret"]
        )
        let schema2 = try CassandraClient.EncryptionSchema(
            keyspace: keyspace,
            table: encTable2,
            keyColumns: [.init(name: "item_id", type: .string)],
            encryptedColumns: ["data"]
        )
        self.configuration.registerEncryptionSchema(schema1)
        self.configuration.registerEncryptionSchema(schema2)
        self.recreateClient()

        let encInsert1 = try await self.cassandraClient.prepare(
            "insert into \(encTable1) (user_id, secret) values (?, ?)",
            encryptionTable: encTable1
        )

        let encInsert2 = try await self.cassandraClient.prepare(
            "insert into \(encTable2) (item_id, data) values (?, ?)",
            encryptionTable: encTable2
        )

        let plainInsert = try await self.cassandraClient.prepare(
            "insert into \(plainTable) (id, value) values (?, ?)"
        )

        try await self.cassandraClient.batch { batch in
            try batch.add(
                prepared: encInsert1,
                parameters: [
                    .string("alice"),
                    .encryptedString(CassandraClient.Encrypted("alice-secret")),
                ]
            )
            try batch.add(
                prepared: encInsert2,
                parameters: [
                    .string("item-42"),
                    .encryptedBytes(CassandraClient.Encrypted([0xDE, 0xAD])),
                ]
            )
            try batch.add(
                prepared: plainInsert,
                parameters: [
                    .string("row-1"),
                    .string("plain-value"),
                ]
            )
        }

        let encSelect1 = try await self.cassandraClient.prepare(
            "select user_id, secret from \(encTable1) where user_id = ?",
            encryptionTable: encTable1
        )

        let aliceResults: [UserWithSecret] = try await self.cassandraClient.execute(
            prepared: encSelect1,
            parameters: [.string("alice")]
        )
        XCTAssertEqual(aliceResults.count, 1)
        XCTAssertEqual(aliceResults[0].secret.value, "alice-secret")

        let encSelect2 = try await self.cassandraClient.prepare(
            "select item_id, data from \(encTable2) where item_id = ?",
            encryptionTable: encTable2
        )

        let itemResults: [ItemWithData] = try await self.cassandraClient.execute(
            prepared: encSelect2,
            parameters: [.string("item-42")]
        )
        XCTAssertEqual(itemResults.count, 1)
        XCTAssertEqual(itemResults[0].data.value, [0xDE, 0xAD])

        let plainSelect = try await self.cassandraClient.prepare(
            "select id, value from \(plainTable) where id = ?"
        )
        let plainRows = try await self.cassandraClient.execute(
            prepared: plainSelect,
            parameters: [.string("row-1")]
        )
        let rowArray = Array(plainRows)
        XCTAssertEqual(rowArray.count, 1)
        let plainValue: String? = rowArray[0].column("value")
        XCTAssertEqual(plainValue, "plain-value")
    }

}

struct UserWithSecret: Decodable {
    let user_id: String
    let secret: CassandraClient.Encrypted<String>
}

struct UserWithMultipleSecrets: Decodable {
    let user_id: String
    let secret_name: CassandraClient.Encrypted<String>
    let secret_age: CassandraClient.Encrypted<Int32>
}

struct CompositeKeyUser: Decodable {
    let partition_id: String
    let cluster_id: Int32
    let secret: CassandraClient.Encrypted<String>
}

struct ItemWithData: Decodable {
    let item_id: String
    let data: CassandraClient.Encrypted<[UInt8]>
}
