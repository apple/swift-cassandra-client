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
import XCTest

@testable import CassandraClient

/// Verifies that Column.string only returns a value for text-typed columns (ASCII, TEXT, VARCHAR)
/// and returns nil for all other types (int, blob, uuid, boolean, etc.).
///
/// The DataStax C driver's cass_value_get_string has no type checking - it returns CASS_OK and
/// raw bytes for any non-null column. Without an explicit type guard, Column.string incorrectly
/// succeeds for non-string columns, causing callers to receive binary garbage for blobs, ints,
/// UDTs, and frozen collections.
final class DataTests: XCTestCase {
    var cassandraClient: CassandraClient!

    override func setUp() {
        super.setUp()
        let env = ProcessInfo.processInfo.environment
        var configuration = CassandraClient.Configuration(
            contactPointsProvider: { callback in
                callback(.success([env["CASSANDRA_HOST"] ?? "127.0.0.1"]))
            },
            port: env["CASSANDRA_CQL_PORT"].flatMap(Int32.init) ?? 9042,
            protocolVersion: .v3
        )
        configuration.username = env["CASSANDRA_USER"]
        configuration.password = env["CASSANDRA_PASSWORD"]
        configuration.keyspace = env["CASSANDRA_KEYSPACE"] ?? "test"
        self.cassandraClient = CassandraClient(configuration: configuration)
    }

    override func tearDown() {
        super.tearDown()
        XCTAssertNoThrow(try self.cassandraClient.shutdown())
        self.cassandraClient = nil
    }

    func testStringReturnsValueForTextColumns() {
        let tableName = "test_col_string_\(DispatchTime.now().uptimeNanoseconds)"
        XCTAssertNoThrow(
            try self.cassandraClient.run(
                "create table \(tableName) (id int primary key, a ascii, b text, c varchar);"
            ).wait()
        )
        XCTAssertNoThrow(
            try self.cassandraClient.run(
                "insert into \(tableName) (id, a, b, c) values (1, 'ascii', 'text', 'varchar');"
            ).wait()
        )

        let rows = try! self.cassandraClient.query("select a, b, c from \(tableName);").wait()
        let row = rows.first!

        XCTAssertEqual(row.column("a")?.string, "ascii")
        XCTAssertEqual(row.column("b")?.string, "text")
        XCTAssertEqual(row.column("c")?.string, "varchar")
    }

    func testStringReturnsNilForNonTextColumns() {
        let tableName = "test_col_string_nil_\(DispatchTime.now().uptimeNanoseconds)"
        XCTAssertNoThrow(
            try self.cassandraClient.run(
                """
                create table \(tableName) (
                    id int primary key,
                    col_int int,
                    col_bigint bigint,
                    col_blob blob,
                    col_uuid uuid,
                    col_bool boolean,
                    col_float float,
                    col_double double
                );
                """
            ).wait()
        )
        let uuid = UUID()
        XCTAssertNoThrow(
            try self.cassandraClient.run(
                "insert into \(tableName) (id, col_int, col_bigint, col_blob, col_uuid, col_bool, col_float, col_double) values (?, ?, ?, ?, ?, ?, ?, ?);",
                parameters: [
                    .int32(1),
                    .int32(42),
                    .int64(9_999_999_999),
                    .bytes([0xDE, 0xAD, 0xBE, 0xEF]),
                    .uuid(uuid),
                    .bool(true),
                    .float32(3.14),
                    .double(2.718),
                ]
            ).wait()
        )

        let rows = try! self.cassandraClient.query(
            "select col_int, col_bigint, col_blob, col_uuid, col_bool, col_float, col_double from \(tableName);"
        ).wait()
        let row = rows.first!

        XCTAssertNil(row.column("col_int")?.string, "int column must not decode as string")
        XCTAssertNil(row.column("col_bigint")?.string, "bigint column must not decode as string")
        XCTAssertNil(row.column("col_blob")?.string, "blob column must not decode as string")
        XCTAssertNil(row.column("col_uuid")?.string, "uuid column must not decode as string")
        XCTAssertNil(row.column("col_bool")?.string, "boolean column must not decode as string")
        XCTAssertNil(row.column("col_float")?.string, "float column must not decode as string")
        XCTAssertNil(row.column("col_double")?.string, "double column must not decode as string")

        XCTAssertEqual(row.column("col_int")?.int32, 42)
        XCTAssertEqual(row.column("col_bigint")?.int64, 9_999_999_999)
        XCTAssertEqual(row.column("col_blob")?.bytes, [0xDE, 0xAD, 0xBE, 0xEF])
        XCTAssertEqual(row.column("col_uuid")?.uuid, uuid)
        XCTAssertEqual(row.column("col_bool")?.bool, true)
    }
}
