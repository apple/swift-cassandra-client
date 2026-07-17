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
import Tracing
import XCTest

@testable import CassandraClient

// Tracing tests: client-side span behaviour for the async execute / prepare / batch path.
//
// Prerequisites for a green run:
//  1. The failure attribute key is asserted as `db.cassandra.error.category` (the same "errorCategory"
//     concept logging emits as `request.errorCategory` and metrics tag as `errorCategory`; value is
//     `Error.category.rawValue`). The tracing helper must emit that key, not `error.type`.
//  2. `swift-distributed-tracing` must resolve so `RequestTrace` and the in-memory tracer
//     (`TracingSupport.swift`) compile; a few API spellings there are provisional.
//  3. Integration tests need a live cluster:  CASSANDRA_HOST=<host> swift test --filter TracingIntegrationTests
//
// The in-memory `TestTracer` is bootstrapped once (process-global, one-shot) and `reset()` per test; span
// captures therefore require serial in-process execution.

// MARK: - Unit (no cluster) — deterministic, load-bearing

@available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
final class TracingUnitTests: XCTestCase {
    override func setUp() {
        super.setUp()
        SharedTestTracer.bootstrap()
        SharedTestTracer.instance.reset()
    }

    /// V0 — the builder derives exactly the frozen attributes from its inputs (not a hardcoded dict).
    func testV0_builderDerivesFrozenAttributesFromInputs() {
        runAsyncAndWaitFor {
            try await CassandraClient.RequestTrace.traced(
                .execute,
                query: "SELECT id FROM t WHERE id = ?",
                consistency: .localOne,
                keyspace: "shop"
            ) {
                // no-op body — assertions below are on what the builder derived from the inputs above
            }

            let spans = SharedTestTracer.instance.recorded
            XCTAssertEqual(spans.count, 1)
            let span = try XCTUnwrap(spans.first)
            XCTAssertEqual(span.operationName, "Cassandra execute")
            XCTAssertEqual(span.kind, .client)
            XCTAssertEqual(span.attributes["db.system.name"], "cassandra")
            XCTAssertEqual(span.attributes["db.operation.name"], "execute")
            XCTAssertEqual(span.attributes["db.query.text"], "SELECT id FROM t WHERE id = ?")
            XCTAssertEqual(span.attributes["db.namespace"], "shop")
            XCTAssertEqual(span.attributes["db.cassandra.consistency_level"], "local_one")
            // Bound values are never attached; a success carries no error status/category.
            XCTAssertNil(span.attributes["db.query.parameters"])
            XCTAssertNil(span.statusCode)
            XCTAssertNil(span.attributes["db.cassandra.error.category"])
        }
    }

    /// V0b — an unresolved consistency (and unset keyspace) is omitted, not fabricated.
    func testV0b_consistencyAndNamespaceOmittedWhenNil() {
        runAsyncAndWaitFor {
            try await CassandraClient.RequestTrace.traced(
                .prepare,
                query: "SELECT 1",
                consistency: nil,
                keyspace: nil
            ) {}

            let span = try XCTUnwrap(SharedTestTracer.instance.recorded.first)
            XCTAssertEqual(span.attributes["db.operation.name"], "prepare")
            XCTAssertNil(span.attributes["db.cassandra.consistency_level"], "omit, do not fabricate 'default'")
            XCTAssertNil(span.attributes["db.namespace"])
        }
    }

    /// The consistency -> OpenTelemetry-token mapping is exhaustive and correct (guards the frozen contract).
    func testConsistencyTokenMappingIsExhaustiveAndCorrect() {
        let expected: [(CassandraClient.Consistency, String)] = [
            (.any, "any"), (.one, "one"), (.two, "two"), (.three, "three"),
            (.quorum, "quorum"), (.all, "all"), (.localQuorum, "local_quorum"),
            (.eachQuorum, "each_quorum"), (.serial, "serial"), (.localSerial, "local_serial"),
            (.localOne, "local_one"),
        ]
        for (consistency, token) in expected {
            XCTAssertEqual(CassandraClient.RequestTrace.otelToken(consistency), token)
        }
    }

    /// V2 (unit slice, + error-identity) — a `CassandraClient.Error` failure records `.error` status and the
    /// shared category, carries no server text, and rethrows the original error unchanged.
    func testV2_failureRecordsSanitizedCategoryAndRethrowsUnchanged() {
        runAsyncAndWaitFor {
            let thrown = CassandraClient.Error.serverError("secret keyspace 'pii' does not exist")
            var caught: Error?
            do {
                try await CassandraClient.RequestTrace.traced(
                    .execute,
                    query: "SELECT 1",
                    consistency: .one,
                    keyspace: "k"
                ) {
                    throw thrown
                }
                XCTFail("expected the body to throw")
            } catch {
                caught = error
            }

            // Observation-only: the original error propagates unchanged (`Error` is Equatable).
            XCTAssertEqual(caught as? CassandraClient.Error, thrown)

            let span = try XCTUnwrap(SharedTestTracer.instance.recorded.first)
            XCTAssertEqual(span.statusCode, .error)
            XCTAssertEqual(span.attributes["db.cassandra.error.category"], thrown.category.rawValue)
            // PII: neither the status message nor any recorded error may carry the server text.
            XCTAssertEqual(span.statusMessage, thrown.shortDescription)
            XCTAssertFalse((span.statusMessage ?? "").contains("secret keyspace"))
            XCTAssertFalse(span.recordedErrors.contains { $0.contains("secret keyspace") })
        }
    }

    /// A5 — a non-`CassandraClient.Error` failure sets `.error` status with no category and no message
    /// (no PII surface), and rethrows unchanged. Covers the helper's non-Cassandra error branch.
    func testA5_nonCassandraErrorSetsErrorStatusWithoutCategoryOrMessage() {
        runAsyncAndWaitFor {
            struct Boom: Error {}
            var caught: Error?
            do {
                try await CassandraClient.RequestTrace.traced(
                    .execute,
                    query: "SELECT 1",
                    consistency: .one,
                    keyspace: "k"
                ) {
                    throw Boom()
                }
                XCTFail("expected the body to throw")
            } catch {
                caught = error
            }

            XCTAssertTrue(caught is Boom, "the original non-Cassandra error is rethrown unchanged")
            let span = try XCTUnwrap(SharedTestTracer.instance.recorded.first)
            XCTAssertEqual(span.statusCode, .error)
            XCTAssertNil(span.attributes["db.cassandra.error.category"], "no category for a non-Cassandra error")
            XCTAssertNil(span.statusMessage, "no message set for a non-Cassandra error")
        }
    }
}

// MARK: - Integration (live cluster) — CASSANDRA_HOST=<host> swift test --filter TracingIntegrationTests

@available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
final class TracingIntegrationTests: XCTestCase {
    override func setUp() {
        super.setUp()
        SharedTestTracer.bootstrap()
        SharedTestTracer.instance.reset()
    }

    private func makeConfig() -> CassandraClient.Configuration {
        let env = ProcessInfo.processInfo.environment
        var config = CassandraClient.Configuration(
            contactPointsProvider: { callback in
                callback(.success([env["CASSANDRA_HOST"] ?? "127.0.0.1"]))
            },
            port: env["CASSANDRA_CQL_PORT"].flatMap(Int32.init) ?? 9042,
            protocolVersion: .v3
        )
        config.username = env["CASSANDRA_USER"]
        config.password = env["CASSANDRA_PASSWORD"]
        config.keyspace = env["CASSANDRA_KEYSPACE"] ?? "test"
        config.requestTimeoutMillis = 24_000
        config.connectTimeoutMillis = 10_000
        return config
    }

    private func createKeyspaceAndTable(
        _ client: CassandraClient,
        keyspace: String,
        table: String,
        schema: String
    ) async throws {
        try await client.withSession(keyspace: .none) { session in
            try await session.run(
                "create keyspace if not exists \(keyspace) with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"
            )
        }
        let session = client.makeSession(keyspace: keyspace)
        defer { try? session.shutdown() }
        try await session.run("create table \(keyspace).\(table) \(schema)")
    }

    private func spans(named name: String) -> [CapturedSpan] {
        SharedTestTracer.instance.recorded.filter { $0.operationName == name }
    }

    private var executeSpans: [CapturedSpan] { self.spans(named: "Cassandra execute") }

    /// V1 (+ positive control for V7) — a successful query emits exactly one `.client` "Cassandra execute"
    /// span with the frozen attributes and no error status.
    func testV1_successEmitsOneClientExecuteSpan() {
        runAsyncAndWaitFor(
            {
                let client = CassandraClient(configuration: self.makeConfig())
                defer { try? client.shutdown() }

                SharedTestTracer.instance.reset()
                try await client.withSession(keyspace: .none) { session in
                    _ = try await session.query("select release_version from system.local")
                }

                XCTAssertEqual(self.executeSpans.count, 1, "exactly one execute span (delta control for V7)")
                let span = try XCTUnwrap(self.executeSpans.first)
                XCTAssertEqual(span.kind, .client)
                XCTAssertEqual(span.attributes["db.system.name"], "cassandra")
                XCTAssertEqual(span.attributes["db.operation.name"], "execute")
                XCTAssertEqual(span.attributes["db.query.text"], "select release_version from system.local")
                XCTAssertNil(span.statusCode, "success -> no error status")
            },
            30.0
        )
    }

    /// V2b — a server-side syntax error records `.error` + the shared `callerFault` category, and the server
    /// text / offending query never lands on the span.
    func testV2b_serverErrorRecordsCategoryWithoutServerText() {
        runAsyncAndWaitFor(
            {
                let client = CassandraClient(configuration: self.makeConfig())
                defer { try? client.shutdown() }

                SharedTestTracer.instance.reset()
                do {
                    try await client.withSession(keyspace: .none) { session in
                        _ = try await session.query("SELCT bad syntax from system.local")  // deliberate syntax error
                    }
                    XCTFail("expected a syntax error")
                } catch {
                    // expected
                }

                let span = try XCTUnwrap(self.executeSpans.first)
                XCTAssertEqual(span.statusCode, .error)
                XCTAssertEqual(
                    span.attributes["db.cassandra.error.category"],
                    "callerFault",
                    "syntax error -> callerFault via the shared categoriser"
                )
                // PII: the offending query text must not be echoed onto the span.
                XCTAssertNotNil(span.statusMessage)
                XCTAssertFalse((span.statusMessage ?? "").contains("SELCT"))
                XCTAssertFalse(span.recordedErrors.contains { $0.contains("SELCT") })
            },
            30.0
        )
    }

    /// V3 — the request span is a child of the caller's span (task-local `ServiceContext` propagation).
    /// Depends on the helper opening the span with `context: .current`; if parenting fails here, the helper
    /// must pass `.current` explicitly.
    func testV3_executeSpanParentsToCallerSpan() {
        runAsyncAndWaitFor(
            {
                let client = CassandraClient(configuration: self.makeConfig())
                defer { try? client.shutdown() }

                SharedTestTracer.instance.reset()
                try await withSpan("caller") { _ in
                    try await client.withSession(keyspace: .none) { session in
                        _ = try await session.query("select release_version from system.local")
                    }
                }

                let caller = try XCTUnwrap(self.spans(named: "caller").first)
                let execute = try XCTUnwrap(self.executeSpans.first)
                XCTAssertEqual(execute.parentSpanID, caller.spanID, "execute span must parent to the caller span")
            },
            30.0
        )
    }

    // V4 (no-tracer / NoOp safety) is intentionally not an in-suite test: `InstrumentationSystem.bootstrap`
    // is process-global and one-shot, so once the capturing tracer is installed a genuine `NoOpTracer` default
    // is unreachable in this process. The baseline (non-tracing) suites already exercise every query path with
    // no tracer bootstrapped; a real NoOp assertion would need a dedicated non-bootstrapping test target.

    /// V5 — direct `nextPage()` x N emits one execute span per fetched page.
    func testV5_directNextPageEmitsOneSpanPerPage() {
        runAsyncAndWaitFor(
            {
                let client = CassandraClient(configuration: self.makeConfig())
                defer { try? client.shutdown() }
                let keyspace = self.makeConfig().keyspace!
                let table = "trace_v5_\(DispatchTime.now().uptimeNanoseconds)"
                try await self.createKeyspaceAndTable(
                    client,
                    keyspace: keyspace,
                    table: table,
                    schema: "(id int primary key)"
                )
                let session = client.makeSession(keyspace: keyspace)
                defer { try? session.shutdown() }
                for i in 0..<3 { try await session.run("insert into \(table) (id) values (\(i));") }

                SharedTestTracer.instance.reset()
                let paginated = try await session.query("select id from \(table);", pageSize: Int32(1))
                for _ in 0..<3 {
                    _ = try await paginated.nextPage()
                }

                XCTAssertEqual(self.executeSpans.count, 3, "one execute span per fetched page")
            },
            30.0
        )
    }

    /// V5b — pages fetched through the `for await` iterator (built inside an unstructured `Task {}`) parent to
    /// the caller's span.
    func testV5b_forAwaitPagesParentToCallerSpan() {
        runAsyncAndWaitFor(
            {
                let client = CassandraClient(configuration: self.makeConfig())
                defer { try? client.shutdown() }
                let keyspace = self.makeConfig().keyspace!
                let table = "trace_v5b_\(DispatchTime.now().uptimeNanoseconds)"
                try await self.createKeyspaceAndTable(
                    client,
                    keyspace: keyspace,
                    table: table,
                    schema: "(id int primary key)"
                )
                let session = client.makeSession(keyspace: keyspace)
                defer { try? session.shutdown() }
                for i in 0..<3 { try await session.run("insert into \(table) (id) values (\(i));") }

                SharedTestTracer.instance.reset()
                try await withSpan("caller") { _ in
                    // Iterate inline in the caller's span scope so the iterator's Task {} inherits `.current`.
                    let paginated = try await session.query("select id from \(table);", pageSize: Int32(1))
                    var seen = 0
                    for try await _ in paginated { seen += 1 }
                    XCTAssertEqual(seen, 3)
                }

                let caller = try XCTUnwrap(self.spans(named: "caller").first)
                let pages = self.executeSpans
                XCTAssertFalse(pages.isEmpty, "for-await should fetch pages via execute spans")
                for page in pages {
                    XCTAssertEqual(
                        page.parentSpanID,
                        caller.spanID,
                        "each page span must parent to the caller span through the Task {}"
                    )
                }
            },
            30.0
        )
    }

    /// V6 (+ prepare-span attributes, + no double-span) — a statement prepared by the real prepare path shows
    /// the real CQL on the EXECUTE span (not "(prepared)"), and `execute(prepared:)` yields exactly one span.
    func testV6_preparedExecuteShowsRealCQLAndYieldsOneSpan() {
        runAsyncAndWaitFor(
            {
                let client = CassandraClient(configuration: self.makeConfig())
                defer { try? client.shutdown() }
                let keyspace = self.makeConfig().keyspace!
                let table = "trace_v6_\(DispatchTime.now().uptimeNanoseconds)"
                try await self.createKeyspaceAndTable(
                    client,
                    keyspace: keyspace,
                    table: table,
                    schema: "(id bigint primary key, v text)"
                )
                let session = client.makeSession(keyspace: keyspace)
                defer { try? session.shutdown() }

                let cql = "insert into \(table) (id, v) values (?, ?)"

                // The prepare site opens a "Cassandra prepare" span carrying the real CQL, .client, no consistency.
                SharedTestTracer.instance.reset()
                let prepared = try await session.prepare(cql)
                let prepareSpan = try XCTUnwrap(self.spans(named: "Cassandra prepare").first)
                XCTAssertEqual(prepareSpan.kind, .client)
                XCTAssertEqual(prepareSpan.attributes["db.operation.name"], "prepare")
                XCTAssertEqual(prepareSpan.attributes["db.query.text"], cql)
                XCTAssertNil(prepareSpan.attributes["db.cassandra.consistency_level"])

                // Executing the prepared statement: the EXECUTE span shows real CQL, and there is exactly one.
                SharedTestTracer.instance.reset()
                _ = try await session.execute(prepared: prepared, parameters: [.int64(1), .string("x")])
                XCTAssertEqual(
                    self.executeSpans.count,
                    1,
                    "execute(prepared:) delegates to execute(statement:) — one span, not two"
                )
                let execute = try XCTUnwrap(self.executeSpans.first)
                XCTAssertEqual(execute.attributes["db.query.text"], cql)
                XCTAssertNotEqual(execute.attributes["db.query.text"], "(prepared)")
            },
            30.0
        )
    }

    /// V7 — connect fails => NO execute span (the span opens post-connect). Negative arm of the V1 delta.
    /// Uses an unroutable host; needs no live cluster.
    func testV7_noExecuteSpanWhenConnectFails() {
        runAsyncAndWaitFor(
            {
                var config = CassandraClient.Configuration(
                    contactPointsProvider: { callback in callback(.success(["240.0.0.1"])) },  // unroutable, reserved
                    port: 9042,
                    protocolVersion: .v3
                )
                config.keyspace = "test"
                config.connectTimeoutMillis = 2_000
                let client = CassandraClient(configuration: config)
                defer { try? client.shutdown() }

                SharedTestTracer.instance.reset()
                do {
                    try await client.withSession(keyspace: .none) { session in
                        _ = try await session.query("select release_version from system.local")
                    }
                    XCTFail("expected a connect failure")
                } catch {
                    // expected
                }

                XCTAssertEqual(self.executeSpans.count, 0, "span opens post-connect, so a failed connect emits none")
            },
            15.0
        )
    }

    /// A1 — a batch opens exactly one `.client` "Cassandra batch" span (operation `batch`, query text `batch`,
    /// no consistency).
    func testA1_batchOpensBatchSpan() {
        runAsyncAndWaitFor(
            {
                let client = CassandraClient(configuration: self.makeConfig())
                defer { try? client.shutdown() }
                let keyspace = self.makeConfig().keyspace!
                let table = "trace_batch_\(DispatchTime.now().uptimeNanoseconds)"
                try await self.createKeyspaceAndTable(
                    client,
                    keyspace: keyspace,
                    table: table,
                    schema: "(id int primary key, name text)"
                )

                SharedTestTracer.instance.reset()
                try await client.batch { batch in
                    try batch.add(
                        statement: CassandraClient.Statement(
                            query: "insert into \(table) (id, name) values (?, ?);",
                            parameters: [.int32(1), .string("a")]
                        )
                    )
                }

                let batchSpans = self.spans(named: "Cassandra batch")
                XCTAssertEqual(batchSpans.count, 1)
                let span = try XCTUnwrap(batchSpans.first)
                XCTAssertEqual(span.kind, .client)
                XCTAssertEqual(span.attributes["db.system.name"], "cassandra")
                XCTAssertEqual(span.attributes["db.operation.name"], "batch")
                XCTAssertEqual(span.attributes["db.query.text"], "batch")
                XCTAssertNil(span.attributes["db.cassandra.consistency_level"], "batch carries no consistency")
                XCTAssertNil(span.statusCode)
            },
            30.0
        )
    }

    /// A4 — constructing a `PaginatedRows` via `query(pageSize:)` opens NO span; the span appears only when a
    /// page is fetched.
    func testA4_pageSizeConstructionEmitsNoSpanUntilFetch() {
        runAsyncAndWaitFor(
            {
                let client = CassandraClient(configuration: self.makeConfig())
                defer { try? client.shutdown() }

                try await client.withSession(keyspace: .none) { session in
                    SharedTestTracer.instance.reset()
                    let paginated = try await session.query(
                        "select release_version from system.local",
                        pageSize: Int32(100)
                    )
                    XCTAssertEqual(self.executeSpans.count, 0, "constructing PaginatedRows opens no span")

                    _ = try await paginated.nextPage()
                    XCTAssertEqual(self.executeSpans.count, 1, "the span opens when a page is fetched")
                }
            },
            30.0
        )
    }

    /// A7 — the consistency attribute is the value the real code resolves: the statement's level wins over the
    /// session config's, and the config's is used when the statement sets none. Drives the real
    /// `statement.options.consistency ?? configuration.consistency` decision (not a hand-fed value).
    func testA7_consistencyResolvedFromStatementThenConfig() {
        runAsyncAndWaitFor(
            {
                var config = self.makeConfig()
                config.consistency = .quorum
                let client = CassandraClient(configuration: config)
                defer { try? client.shutdown() }

                // Statement-level consistency wins over the config default.
                SharedTestTracer.instance.reset()
                let explicit = try CassandraClient.Statement(
                    query: "select release_version from system.local",
                    options: .init(consistency: .one)
                )
                _ = try await client.execute(statement: explicit)
                XCTAssertEqual(self.executeSpans.first?.attributes["db.cassandra.consistency_level"], "one")

                // No statement-level consistency -> the config default is used.
                SharedTestTracer.instance.reset()
                let inherited = try CassandraClient.Statement(query: "select release_version from system.local")
                _ = try await client.execute(statement: inherited)
                XCTAssertEqual(self.executeSpans.first?.attributes["db.cassandra.consistency_level"], "quorum")
            },
            30.0
        )
    }

    /// A9 — a preflight (binding) failure throws before the span opens, so NO execute span is created
    /// (the boundary the "not traced" scope depends on).
    func testA9_preflightFailureNotTraced() {
        runAsyncAndWaitFor(
            {
                let client = CassandraClient(configuration: self.makeConfig())
                defer { try? client.shutdown() }
                let keyspace = self.makeConfig().keyspace!
                let table = "trace_preflight_\(DispatchTime.now().uptimeNanoseconds)"
                try await self.createKeyspaceAndTable(
                    client,
                    keyspace: keyspace,
                    table: table,
                    schema: "(id bigint primary key, v text)"
                )

                let session = client.makeSession(keyspace: keyspace)
                defer { try? session.shutdown() }
                let prepared = try await session.prepare("insert into \(table) (id, v) values (?, ?)")

                SharedTestTracer.instance.reset()  // ignore the prepare span
                do {
                    // Bind more parameters than there are placeholders -> preflight bind failure before the span.
                    _ = try await session.execute(
                        prepared: prepared,
                        parameters: [.int64(1), .string("x"), .string("extra")]
                    )
                    XCTFail("expected a preflight binding failure")
                } catch {
                    // expected
                }

                XCTAssertEqual(self.executeSpans.count, 0, "preflight failure throws before the span opens")
            },
            30.0
        )
    }
}
