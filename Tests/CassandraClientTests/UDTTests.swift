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

final class UDTTests: XCTestCase {
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

    private func uniqueSuffix() -> String {
        "\(DispatchTime.now().uptimeNanoseconds)"
    }

    private func makeType() throws -> String {
        let typeName = "udt_test_\(self.uniqueSuffix())"
        try self.cassandraClient.run(
            "create type \(typeName) (a text, b int, c double, d boolean)"
        ).wait()
        return typeName
    }

    private func value(a: String?, b: Int32, c: Double, d: Bool, typeName: String) -> CassandraClient.UDT {
        CassandraClient.UDT(
            typeName: typeName,
            fields: [
                ("a", a.map { .string($0) } ?? .null),
                ("b", .int32(b)),
                ("c", .double(c)),
                ("d", .bool(d)),
            ]
        )
    }

    func testListRoundtrip() throws {
        let typeName = try self.makeType()
        let tableName = "test_\(self.uniqueSuffix())"
        try self.cassandraClient.run(
            "create table \(tableName) (id uuid primary key, items list<frozen<\(typeName)>>)"
        ).wait()

        let id = UUID()
        let items = [
            self.value(a: "first", b: 1, c: 1.5, d: true, typeName: typeName),
            self.value(a: nil, b: 2, c: 2.5, d: false, typeName: typeName),
        ]
        try self.cassandraClient.run(
            "insert into \(tableName) (id, items) values (?, ?)",
            parameters: [.uuid(id), .udtArray(items)]
        ).wait()

        let rows = try self.cassandraClient.query(
            "select items from \(tableName) where id = ?",
            parameters: [.uuid(id)]
        ).wait()
        let row = try XCTUnwrap(rows.first)
        let read = try XCTUnwrap(row.column("items")?.udtArray)

        XCTAssertEqual(read.count, 2)
        XCTAssertEqual(read[0].field("a")?.string, "first")
        XCTAssertEqual(read[0].field("b")?.int32, 1)
        XCTAssertEqual(read[0].field("c")?.double, 1.5)
        XCTAssertEqual(read[0].field("d")?.bool, true)
        XCTAssertNil(read[1].field("a")?.string)
        XCTAssertEqual(read[1].field("b")?.int32, 2)
    }

    func testScalarRoundtrip() throws {
        let typeName = try self.makeType()
        let tableName = "test_\(self.uniqueSuffix())"
        try self.cassandraClient.run(
            "create table \(tableName) (id uuid primary key, item frozen<\(typeName)>)"
        ).wait()

        let id = UUID()
        try self.cassandraClient.run(
            "insert into \(tableName) (id, item) values (?, ?)",
            parameters: [.uuid(id), .udt(self.value(a: "solo", b: 7, c: 3.25, d: true, typeName: typeName))]
        ).wait()

        let rows = try self.cassandraClient.query("select item from \(tableName) where id = ?", parameters: [.uuid(id)])
            .wait()
        let row = try XCTUnwrap(rows.first)

        XCTAssertEqual(row.column("item")?.field("a")?.string, "solo")
        XCTAssertEqual(row.column("item")?.field("b")?.int32, 7)
        XCTAssertEqual(row.column("item")?.field("c")?.double, 3.25)
        XCTAssertEqual(row.column("item")?.field("d")?.bool, true)
    }

    func testNullHandling() throws {
        let typeName = try self.makeType()
        let tableName = "test_\(self.uniqueSuffix())"
        try self.cassandraClient.run(
            "create table \(tableName) (id uuid primary key, item frozen<\(typeName)>, items list<frozen<\(typeName)>>)"
        ).wait()

        let id = UUID()
        try self.cassandraClient.run(
            "insert into \(tableName) (id) values (?)",
            parameters: [.uuid(id)]
        ).wait()

        let rows = try self.cassandraClient.query(
            "select item, items from \(tableName) where id = ?",
            parameters: [.uuid(id)]
        ).wait()
        let row = try XCTUnwrap(rows.first)

        XCTAssertNil(row.column("item")?.field("a"))
        XCTAssertNil(row.column("items")?.udtArray)
    }

    func testMapRoundtrip() throws {
        let typeName = try self.makeType()
        let tableName = "test_\(self.uniqueSuffix())"
        try self.cassandraClient.run(
            "create table \(tableName) (id uuid primary key, items map<text, frozen<\(typeName)>>)"
        ).wait()

        let id = UUID()
        let items = [
            "one": self.value(a: "x", b: 10, c: 1.0, d: false, typeName: typeName)
        ]
        try self.cassandraClient.run(
            "insert into \(tableName) (id, items) values (?, ?)",
            parameters: [.uuid(id), .stringUDTMap(items)]
        ).wait()

        let rows = try self.cassandraClient.query(
            "select items from \(tableName) where id = ?",
            parameters: [.uuid(id)]
        ).wait()
        let row = try XCTUnwrap(rows.first)
        let read = try XCTUnwrap(row.column("items")?.stringUDTMap)

        XCTAssertEqual(read["one"]?.field("a")?.string, "x")
        XCTAssertEqual(read["one"]?.field("b")?.int32, 10)
    }

    func testUnknownTypeNameThrows() throws {
        let typeName = try self.makeType()
        let tableName = "test_\(self.uniqueSuffix())"
        try self.cassandraClient.run(
            "create table \(tableName) (id uuid primary key, items list<frozen<\(typeName)>>)"
        ).wait()

        let bad = CassandraClient.UDT(typeName: "does_not_exist", fields: [("a", .string("x"))])
        XCTAssertThrowsError(
            try self.cassandraClient.run(
                "insert into \(tableName) (id, items) values (?, ?)",
                parameters: [.uuid(UUID()), .udtArray([bad])]
            ).wait()
        )
    }

    func testCodableDecode() throws {
        struct Item: Codable, Equatable {
            let a: String?
            let b: Int32
            let c: Double
            let d: Bool
        }
        struct Model: Codable, Equatable {
            let id: UUID
            let items: [Item]
        }

        let typeName = try self.makeType()
        let tableName = "test_\(self.uniqueSuffix())"
        try self.cassandraClient.run(
            "create table \(tableName) (id uuid primary key, items list<frozen<\(typeName)>>)"
        ).wait()

        let id = UUID()
        try self.cassandraClient.run(
            "insert into \(tableName) (id, items) values (?, ?)",
            parameters: [
                .uuid(id),
                .udtArray([
                    self.value(a: "alpha", b: 1, c: 1.5, d: true, typeName: typeName),
                    self.value(a: nil, b: 2, c: 2.5, d: false, typeName: typeName),
                ]),
            ]
        ).wait()

        let result: [Model] = try self.cassandraClient.query(
            "select id, items from \(tableName) where id = ?",
            parameters: [.uuid(id)]
        ).wait()

        XCTAssertEqual(
            result,
            [
                Model(
                    id: id,
                    items: [
                        Item(a: "alpha", b: 1, c: 1.5, d: true),
                        Item(a: nil, b: 2, c: 2.5, d: false),
                    ]
                )
            ]
        )
    }

    func testDateFieldRoundtrip() throws {
        let typeName = "udt_date_\(self.uniqueSuffix())"
        try self.cassandraClient.run(
            "create type \(typeName) (a text, when timestamp)"
        ).wait()
        let tableName = "test_\(self.uniqueSuffix())"
        try self.cassandraClient.run(
            "create table \(tableName) (id uuid primary key, item frozen<\(typeName)>)"
        ).wait()

        let id = UUID()
        let millis: Int64 = 1_700_000_000_000
        let when = Date(timeIntervalSince1970: Double(millis) / 1000.0)
        let udt = CassandraClient.UDT(
            typeName: typeName,
            fields: [("a", .string("dated")), ("when", .date(when))]
        )
        try self.cassandraClient.run(
            "insert into \(tableName) (id, item) values (?, ?)",
            parameters: [.uuid(id), .udt(udt)]
        ).wait()

        let rows = try self.cassandraClient.query(
            "select item from \(tableName) where id = ?",
            parameters: [.uuid(id)]
        ).wait()
        let row = try XCTUnwrap(rows.first)

        XCTAssertEqual(row.column("item")?.field("a")?.string, "dated")
        XCTAssertEqual(row.column("item")?.field("when")?.timestamp, millis)
    }

    func testPreparedRoundtrip() throws {
        let typeName = try self.makeType()
        let tableName = "test_\(self.uniqueSuffix())"
        try self.cassandraClient.run(
            "create table \(tableName) (id uuid primary key, items list<frozen<\(typeName)>>)"
        ).wait()

        let id = UUID()
        let prepared = try self.cassandraClient.prepare(
            "insert into \(tableName) (id, items) values (?, ?)"
        ).wait()
        _ = try self.cassandraClient.execute(
            prepared: prepared,
            parameters: [.uuid(id), .udtArray([self.value(a: "p", b: 9, c: 9.5, d: true, typeName: typeName)])]
        ).wait()

        let rows = try self.cassandraClient.query(
            "select items from \(tableName) where id = ?",
            parameters: [.uuid(id)]
        ).wait()
        let read = try XCTUnwrap(try XCTUnwrap(rows.first).column("items")?.udtArray)
        XCTAssertEqual(read.first?.field("a")?.string, "p")
        XCTAssertEqual(read.first?.field("b")?.int32, 9)
    }
}
