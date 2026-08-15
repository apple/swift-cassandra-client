//===----------------------------------------------------------------------===//
//
// This source file is part of the Swift Cassandra Client open source project
//
// Copyright (c) 2022-2026 Apple Inc. and the Swift Cassandra Client project authors
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

/// Conversion logic between `Foundation.Decimal` and Cassandra's varint+scale wire form.
/// These run without a Cassandra connection.
final class DecimalConversionTests: XCTestCase {
    func testConversionRoundtrip() {
        let values: [Foundation.Decimal] = [
            Decimal(string: "0")!,
            Decimal(string: "1")!,
            Decimal(string: "123.45")!,
            Decimal(string: "-123.45")!,
            Decimal(string: "-0.001")!,
            Decimal(string: "128")!,
            Decimal(string: "-128")!,
            Decimal(string: "1000000")!,
            Decimal(string: "0.0000001")!,
            Decimal(string: "123456789012345678901234567890")!,
            Decimal(string: "-987654321.123456789")!,
        ]
        for value in values {
            let (varint, scale) = CassandraClient.decimalToVarint(value)
            let decoded = CassandraClient.varintToDecimal(varint: varint, scale: scale)
            XCTAssertEqual(decoded, value, "round-trip failed for \(value)")
        }
    }

    func testKnownWireVectors() {
        // 123.45 = unscaled 12345 (0x3039), scale 2
        let (positive, positiveScale) = CassandraClient.decimalToVarint(Decimal(string: "123.45")!)
        XCTAssertEqual(positive, [0x30, 0x39])
        XCTAssertEqual(positiveScale, 2)

        // 128 has its high bit set, so a leading 0x00 must be prepended to keep it positive.
        let (sign, signScale) = CassandraClient.decimalToVarint(Decimal(string: "128")!)
        XCTAssertEqual(sign, [0x00, 0x80])
        XCTAssertEqual(signScale, 0)

        // -1 with scale 3 is the two's complement of 1: 0xFF.
        let (negative, negativeScale) = CassandraClient.decimalToVarint(Decimal(string: "-0.001")!)
        XCTAssertEqual(negative, [0xFF])
        XCTAssertEqual(negativeScale, 3)

        XCTAssertEqual(CassandraClient.varintToDecimal(varint: [0x30, 0x39], scale: 2), Decimal(string: "123.45"))
        XCTAssertEqual(CassandraClient.varintToDecimal(varint: [0xFF], scale: 3), Decimal(string: "-0.001"))
    }

    func testOutOfRangeReturnsNil() {
        let tooWide = [UInt8](repeating: 0x7F, count: 24)
        XCTAssertNil(CassandraClient.varintToDecimal(varint: tooWide, scale: 0))
        XCTAssertNil(CassandraClient.varintToDecimal(varint: [], scale: 0))
    }
}

final class DecimalTests: XCTestCase {
    var cassandraClient: CassandraClient!
    var configuration: CassandraClient.Configuration!

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
        self.cassandraClient = nil  // FIXME: for tsan
    }

    // MARK: - Integration tests

    func testScalarRoundtrip() throws {
        let tableName = "test_\(DispatchTime.now().uptimeNanoseconds)"
        try self.cassandraClient.run(
            "create table \(tableName) (id int primary key, amount decimal)"
        ).wait()

        let values: [(Int32, Foundation.Decimal?)] = [
            (1, Decimal(string: "123.45")),
            (2, Decimal(string: "-987654321.123456789")),
            (3, nil),
        ]
        for (id, amount) in values {
            try self.cassandraClient.run(
                "insert into \(tableName) (id, amount) values (?, ?)",
                parameters: [.int32(id), amount.map { .decimal($0) } ?? .null]
            ).wait()
        }

        for (id, amount) in values {
            let rows = try self.cassandraClient.query(
                "select amount from \(tableName) where id = ?",
                parameters: [.int32(id)]
            ).wait()
            let read = try XCTUnwrap(rows.first).column("amount")?.decimal
            XCTAssertEqual(read, amount, "mismatch for id \(id)")
        }
    }

    func testListRoundtrip() throws {
        let tableName = "test_\(DispatchTime.now().uptimeNanoseconds)"
        try self.cassandraClient.run(
            "create table \(tableName) (id int primary key, amounts list<decimal>)"
        ).wait()

        let amounts = [Decimal(string: "1.1")!, Decimal(string: "-2.22")!, Decimal(string: "300")!]
        try self.cassandraClient.run(
            "insert into \(tableName) (id, amounts) values (?, ?)",
            parameters: [.int32(1), .decimalArray(amounts)]
        ).wait()

        let rows = try self.cassandraClient.query(
            "select amounts from \(tableName) where id = ?",
            parameters: [.int32(1)]
        ).wait()
        let read = try XCTUnwrap(try XCTUnwrap(rows.first).column("amounts")?.decimalArray)
        XCTAssertEqual(read, amounts)
    }

    func testCodableDecode() throws {
        struct Model: Codable, Equatable {
            let id: Int32
            let amount: Foundation.Decimal
            let amounts: [Foundation.Decimal]
        }

        let tableName = "test_\(DispatchTime.now().uptimeNanoseconds)"
        try self.cassandraClient.run(
            "create table \(tableName) (id int primary key, amount decimal, amounts list<decimal>)"
        ).wait()

        let expected = Model(
            id: 1,
            amount: Decimal(string: "42.42")!,
            amounts: [Decimal(string: "0.5")!, Decimal(string: "-1.25")!]
        )
        try self.cassandraClient.run(
            "insert into \(tableName) (id, amount, amounts) values (?, ?, ?)",
            parameters: [.int32(expected.id), .decimal(expected.amount), .decimalArray(expected.amounts)]
        ).wait()

        let result: [Model] = try self.cassandraClient.query(
            "select id, amount, amounts from \(tableName) where id = ?",
            parameters: [.int32(1)]
        ).wait()
        XCTAssertEqual(result, [expected])
    }
}
