//===----------------------------------------------------------------------===//
//
// This source file is part of the Swift Cassandra Client open source project
//
// Copyright (c) 2026 Apple Inc. and the Swift Cassandra Client project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of Swift Cassandra Client project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import CassandraClient
import NIO
import XCTest

/// Unit tests for ``CassandraClient/Statement/setPagingSize(_:)``. Statement configuration needs no
/// cluster, and neither does a rejected page size on the paginated entry points — the page size is
/// applied before the request reaches a connection — so the accepted range is pinned here rather
/// than in the integration suite.
///
/// - Note: Imported without `@testable` (unlike the rest of the suite) so that the file stops
///   compiling if `setPagingSize` loses its `public` access level.
final class StatementPagingSizeTests: XCTestCase {
    private static func makeStatement() throws -> CassandraClient.Statement {
        try CassandraClient.Statement(query: "select id from test;")
    }

    /// A client that is never expected to reach a node: these calls fail on the page size first.
    private func makeClient() -> CassandraClient {
        CassandraClient(
            configuration: CassandraClient.Configuration(
                contactPointsProvider: { callback in callback(.success(["127.0.0.1"])) },
                port: 9042,
                protocolVersion: .v3
            )
        )
    }

    /// `Error.badParams` carries its message in the payload, so equality would pin the wording;
    /// `shortDescription` identifies the case without doing that.
    private func assertBadParams(
        _ error: Swift.Error,
        _ subject: String,
        file: StaticString = #filePath,
        line: UInt = #line
    ) {
        XCTAssertEqual(
            (error as? CassandraClient.Error)?.shortDescription,
            "Bad parameters",
            "\(subject) should be rejected as a bad parameter, got \(error)",
            file: file,
            line: line
        )
    }

    func testAcceptsPositiveSizes() throws {
        let statement = try Self.makeStatement()
        for size in [1, 10, 5000, Int(Int32.max)] {
            XCTAssertNoThrow(try statement.setPagingSize(size), "page size \(size) should be accepted")
        }
    }

    /// The driver treats a non-positive page size as "paging disabled" instead of clamping it, so the
    /// wrapper rejects it rather than returning an unpaginated result from a paginated call.
    func testRejectsNonPositiveSizes() throws {
        let statement = try Self.makeStatement()
        for size in [0, -1, Int(Int32.min)] {
            XCTAssertThrowsError(try statement.setPagingSize(size)) { error in
                self.assertBadParams(error, "page size \(size)")
            }
        }
    }

    /// The driver's page size is a C `int`, so sizes above `Int32.max` throw instead of trapping on
    /// the narrowing conversion. Only meaningful where `Int` is wider than `Int32`.
    func testRejectsSizesAboveInt32Max() throws {
        try XCTSkipUnless(Int.bitWidth > 32, "Int is no wider than Int32 on this platform")
        let statement = try Self.makeStatement()
        for size in [Int(Int32.max) + 1, Int.max] {
            XCTAssertThrowsError(try statement.setPagingSize(size)) { error in
                self.assertBadParams(error, "page size \(size)")
            }
        }
    }

    /// The public `pageSize:` entry points share this validation, so a size the driver would ignore
    /// fails the call rather than returning every row from a paginated one.
    func testPaginatedQueryRejectsNonPositivePageSize() throws {
        let client = self.makeClient()
        defer { XCTAssertNoThrow(try client.shutdown()) }

        XCTAssertThrowsError(
            try client.query("select id from test;", pageSize: Int32(0)).wait()
        ) { error in
            self.assertBadParams(error, "pageSize 0 on the EventLoopFuture query")
        }
    }

    /// The async paginated path applies the page size in separate code from the `EventLoopFuture`
    /// path, so it gets its own assertion.
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    func testPaginatedAsyncQueryRejectsNonPositivePageSize() async throws {
        let client = self.makeClient()

        do {
            let rows: CassandraClient.PaginatedRows = try await client.query(
                "select id from test;",
                pageSize: Int32(-1)
            )
            XCTFail("expected a bad parameter error, got \(rows)")
        } catch {
            self.assertBadParams(error, "pageSize -1 on the async query")
        }

        try await client.shutdownAsync()
    }

    /// ``CassandraClient/Statement/setPagingSize(_:)`` documents that the `pageSize`-taking variants
    /// overwrite a size already set on the statement. Applying a rejected size to a statement that
    /// carries a valid one pins that: the call fails, so the argument was applied unconditionally
    /// rather than deferring to what the statement already held.
    func testPaginatedExecuteOverwritesTheStatementPagingSize() throws {
        let client = self.makeClient()
        defer { XCTAssertNoThrow(try client.shutdown()) }

        let statement = try Self.makeStatement()
        try statement.setPagingSize(10)

        // `statement` is handed over `sending`. The call stays out of the `XCTAssertThrowsError`
        // autoclosure, and `makeStatement` is `static`, so the statement is not in the test case's
        // region — the error closure below reads `self`. `execute` does not throw; the page-size
        // failure arrives through the future, which `wait()` still surfaces.
        let paginated = client.execute(statement: statement, pageSize: Int32(0), on: nil)
        XCTAssertThrowsError(
            try paginated.wait()
        ) { error in
            self.assertBadParams(error, "pageSize 0 over a statement already set to 10")
        }
    }
}
