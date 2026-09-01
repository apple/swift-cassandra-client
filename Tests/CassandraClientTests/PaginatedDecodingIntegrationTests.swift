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

/// Integration tests for the paginated decoding query — the `query(_:parameters:pageSize:options:logger:)`
/// overload returning a sequence of decoded `T`. Paged results need multi-page data, so these require a
/// live cluster:
///
///     CASSANDRA_HOST=<your-test-cluster-host> swift test --filter PaginatedDecodingIntegrationTests
///
/// The fixture puts every row in one partition behind a clustering key, so pages come back in `ck` order
/// and the ordering and mid-stream failure assertions are deterministic.
///
/// `selectAll` is `static` and each test binds the client to a local, so the async bodies never capture
/// `self` — a test case is not `Sendable`. The local is a snapshot taken before the handoff: a test that
/// reassigned `cassandraClient` mid-body would not see the new value there.
@available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
final class PaginatedDecodingIntegrationTests: XCTestCase {
    private static let partition: Int32 = 1
    private static let pageSize: Int32 = 10

    private var cassandraClient: CassandraClient!
    private var keyspace: String!

    /// A fixture row. Field names match the selected columns; `payload` is non-optional, so a null
    /// `payload` fails to decode.
    private struct Item: Decodable, Equatable, Sendable {
        let pk: Int32
        let ck: Int32
        let payload: String
    }

    override func setUp() {
        super.setUp()

        let env = ProcessInfo.processInfo.environment
        let keyspace = env["CASSANDRA_KEYSPACE"] ?? "test"
        var configuration = CassandraClient.Configuration(
            contactPointsProvider: { callback in
                callback(.success([env["CASSANDRA_HOST"] ?? "127.0.0.1"]))
            },
            port: env["CASSANDRA_CQL_PORT"].flatMap(Int32.init) ?? 9042,
            protocolVersion: .v3
        )
        configuration.username = env["CASSANDRA_USER"]
        configuration.password = env["CASSANDRA_PASSWORD"]
        configuration.keyspace = keyspace
        configuration.requestTimeoutMillis = UInt32(24_000)
        configuration.connectTimeoutMillis = UInt32(10_000)
        self.keyspace = keyspace

        var logger = Logger(label: "test")
        logger.logLevel = .debug

        self.cassandraClient = CassandraClient(configuration: configuration, logger: logger)
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

    /// The bound values for one fixture row, built fresh per row: `run` takes `parameters` as `sending`,
    /// and the futures below are collected and awaited together.
    private static func insertParameters(
        index: Int,
        nullPayloadAt: Int?
    ) -> [CassandraClient.Statement.Value] {
        [
            .int32(Self.partition),
            .int32(Int32(index)),
            index == nullPayloadAt ? .null : .string("payload-\(index)"),
        ]
    }

    /// Create the fixture table and insert `count` rows into one partition with `ck` 0..<count.
    /// `nullPayloadAt` writes a null `payload` at that clustering key.
    private func makeTable(rows count: Int, nullPayloadAt: Int? = nil) throws -> String {
        let table = "test_paged_decode_\(DispatchTime.now().uptimeNanoseconds)"
        try self.cassandraClient.run(
            "create table \(table) (pk int, ck int, payload text, primary key (pk, ck));"
        ).wait()

        let options = CassandraClient.Statement.Options(consistency: .localQuorum)
        var futures = [EventLoopFuture<Void>]()
        for index in 0..<count {
            futures.append(
                self.cassandraClient.run(
                    "insert into \(table) (pk, ck, payload) values (?, ?, ?);",
                    parameters: Self.insertParameters(index: index, nullPayloadAt: nullPayloadAt),
                    options: options
                )
            )
        }
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: System.coreCount)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
        try EventLoopFuture.andAllSucceed(futures, on: eventLoopGroup.next()).wait()
        return table
    }

    private static func selectAll(_ table: String) -> String {
        "select pk, ck, payload from \(table) where pk = \(Self.partition);"
    }

    /// Every row is decoded and yielded across page boundaries, in clustering order and with the same
    /// contents as the buffered decoding query over the same rows. Collecting the whole sequence before
    /// asserting also covers the documented difference from raw paging: a decoded value is an independent
    /// Swift value, so values taken earlier stay valid as the sequence advances.
    func testYieldsEveryRowInOrder() throws {
        let count = 25  // > pageSize: 3 pages
        let table = try self.makeTable(rows: count)

        let client = self.cassandraClient!
        runAsyncAndWaitFor(
            {
                let paged: AsyncThrowingMapSequence<CassandraClient.PaginatedRows, Item> =
                    try await client.query(Self.selectAll(table), pageSize: Self.pageSize)
                var decoded: [Item] = []
                for try await item in paged {
                    decoded.append(item)
                }

                let buffered: [Item] = try await client.query(Self.selectAll(table))
                XCTAssertEqual(decoded.count, count, "every row should be yielded")
                XCTAssertEqual(decoded, buffered, "paged decoding should match the buffered decoding query")
                XCTAssertEqual(
                    decoded.map(\.ck),
                    Array(Int32(0)..<Int32(count)),
                    "rows should arrive in clustering order"
                )
                XCTAssertEqual(
                    decoded.map(\.payload),
                    (0..<count).map { "payload-\($0)" },
                    "decoded values should carry the written contents"
                )
            },
            30.0
        )
    }

    /// The `withModelType:` paginated overload yields the same decoded values, in the same order, as its
    /// inference sibling. Here the element type is bound only by `withModelType:`, not a return-type
    /// annotation on `paged`.
    func testYieldsEveryRowInOrderWithModelType() throws {
        let count = 25  // > pageSize: 3 pages
        let table = try self.makeTable(rows: count)

        let client = self.cassandraClient!
        runAsyncAndWaitFor(
            {
                let paged = try await client.query(
                    Self.selectAll(table),
                    pageSize: Self.pageSize,
                    withModelType: Item.self
                )
                var decoded: [Item] = []
                for try await item in paged {
                    decoded.append(item)
                }

                let buffered = try await client.query(
                    Self.selectAll(table),
                    withModelType: Item.self
                )
                XCTAssertEqual(decoded.count, count, "every row should be yielded")
                XCTAssertEqual(decoded, buffered, "paged decoding should match the buffered decoding query")
                XCTAssertEqual(
                    decoded.map(\.ck),
                    Array(Int32(0)..<Int32(count)),
                    "rows should arrive in clustering order"
                )
            },
            30.0
        )
    }

    /// A row that cannot be decoded fails the iteration at that element: the rows before it — more than
    /// a full page of them — are still yielded, so rows are decoded one at a time as the sequence
    /// advances rather than up front.
    func testSurfacesDecodeFailureMidStream() throws {
        let count = 25
        let failingIndex = 15  // second page, so a full page is consumed before the failure
        let table = try self.makeTable(rows: count, nullPayloadAt: failingIndex)

        let client = self.cassandraClient!
        runAsyncAndWaitFor(
            {
                let paged: AsyncThrowingMapSequence<CassandraClient.PaginatedRows, Item> =
                    try await client.query(Self.selectAll(table), pageSize: Self.pageSize)
                var decoded: [Item] = []
                do {
                    for try await item in paged {
                        decoded.append(item)
                    }
                    XCTFail("expected the null payload row to fail decoding")
                } catch {
                    XCTAssertTrue(
                        "\(error)".contains("payload"),
                        "the failure should name the column that could not be decoded, got \(error)"
                    )
                }
                XCTAssertEqual(
                    decoded.map(\.ck),
                    Array(Int32(0)..<Int32(failingIndex)),
                    "every row before the failing one should have been yielded"
                )
            },
            30.0
        )
    }

    /// A first-row decode failure throws before anything is yielded.
    func testSurfacesDecodeFailureOnFirstRow() throws {
        let table = try self.makeTable(rows: 25, nullPayloadAt: 0)

        let client = self.cassandraClient!
        runAsyncAndWaitFor(
            {
                let paged: AsyncThrowingMapSequence<CassandraClient.PaginatedRows, Item> =
                    try await client.query(Self.selectAll(table), pageSize: Self.pageSize)
                var decoded: [Item] = []
                do {
                    for try await item in paged {
                        decoded.append(item)
                    }
                    XCTFail("expected the null payload row to fail decoding")
                } catch {
                    // expected
                }
                XCTAssertTrue(decoded.isEmpty, "nothing should be yielded when the first row fails to decode")
            },
            30.0
        )
    }

    /// An empty result set yields no elements and no error.
    func testEmptyResult() throws {
        let table = try self.makeTable(rows: 0)

        let client = self.cassandraClient!
        runAsyncAndWaitFor(
            {
                let paged: AsyncThrowingMapSequence<CassandraClient.PaginatedRows, Item> =
                    try await client.query(Self.selectAll(table), pageSize: Self.pageSize)
                var decoded: [Item] = []
                for try await item in paged {
                    decoded.append(item)
                }
                XCTAssertTrue(decoded.isEmpty, "an empty result set should yield no values")
            },
            30.0
        )
    }

    /// A result set smaller than `pageSize` (single page) still yields every row.
    func testSinglePageResult() throws {
        let count = 4  // < pageSize
        let table = try self.makeTable(rows: count)

        let client = self.cassandraClient!
        runAsyncAndWaitFor(
            {
                let paged: AsyncThrowingMapSequence<CassandraClient.PaginatedRows, Item> =
                    try await client.query(Self.selectAll(table), pageSize: Self.pageSize)
                var decoded: [Item] = []
                for try await item in paged {
                    decoded.append(item)
                }
                XCTAssertEqual(decoded.map(\.ck), Array(Int32(0)..<Int32(count)))
            },
            30.0
        )
    }

    /// A row count that is an exact multiple of `pageSize` yields every row — the last page is full, so
    /// this covers the boundary where the server may report a further (empty) page.
    func testRowCountExactMultipleOfPageSize() throws {
        let count = Int(Self.pageSize) * 2
        let table = try self.makeTable(rows: count)

        let client = self.cassandraClient!
        runAsyncAndWaitFor(
            {
                let paged: AsyncThrowingMapSequence<CassandraClient.PaginatedRows, Item> =
                    try await client.query(Self.selectAll(table), pageSize: Self.pageSize)
                var decoded: [Item] = []
                for try await item in paged {
                    decoded.append(item)
                }
                XCTAssertEqual(decoded.map(\.ck), Array(Int32(0)..<Int32(count)))
            },
            30.0
        )
    }

    /// Stopping the iteration part-way through a page ends it cleanly, having yielded exactly the rows
    /// consumed so far. (The assertion is on what the consumer sees; how far the producer had paged ahead
    /// is not observable here.)
    func testStoppingIterationEarly() throws {
        let table = try self.makeTable(rows: 25)
        let consumed = 12  // stops inside the second page

        let client = self.cassandraClient!
        runAsyncAndWaitFor(
            {
                let paged: AsyncThrowingMapSequence<CassandraClient.PaginatedRows, Item> =
                    try await client.query(Self.selectAll(table), pageSize: Self.pageSize)
                var decoded: [Item] = []
                for try await item in paged {
                    decoded.append(item)
                    if decoded.count == consumed { break }
                }
                XCTAssertEqual(decoded.map(\.ck), Array(Int32(0)..<Int32(consumed)))
            },
            30.0
        )
    }
}
