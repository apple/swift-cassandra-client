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

/// Request-logging INTEGRATION tests. Most require a live Cassandra — run with:
///
///     CASSANDRA_HOST=<your-test-cluster-host> swift test --filter RequestLoggingIntegrationTests
///
/// The async API is used deliberately: the async logging helper emits the record *before* the `await`
/// returns, so assertions after the call are deterministic (no completion-callback race).
/// `testConnectFailureLoggedOnce` points at an unroutable host and does not need the cluster.
@available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
final class RequestLoggingIntegrationTests: XCTestCase {
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

    /// Create a client whose default logger captures, with slow-query logging on ("0" logs all successes).
    private func makeCapturingClient(
        logBoundValues: Bool = false
    ) -> (CassandraClient, TestLogCapture, String) {
        var config = self.makeConfig()
        config.slowQueryThresholdMillis = 0
        config.logBoundValues = logBoundValues
        let (logger, capture) = makeCapturingLogger()
        let client = CassandraClient(configuration: config, logger: logger)
        return (client, capture, config.keyspace!)
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

    /// A prepared statement's slow-query record shows the real CQL, not "(prepared)".
    func testPreparedStatementLogsRealCQL() throws {
        runAsyncAndWaitFor {
            let (client, capture, keyspace) = self.makeCapturingClient()
            defer { XCTAssertNoThrow(try client.shutdown()) }
            let table = "log_v6b_\(DispatchTime.now().uptimeNanoseconds)"
            try await self.createKeyspaceAndTable(
                client,
                keyspace: keyspace,
                table: table,
                schema: "(id bigint primary key, v text)"
            )

            let session = client.makeSession(keyspace: keyspace)
            defer { try? session.shutdown() }
            let cql = "insert into \(table) (id, v) values (?, ?)"
            let prepared = try await session.prepare(cql)
            capture.clear()  // ignore setup logs
            _ = try await session.execute(prepared: prepared, parameters: [.int64(1), .string("x")])

            let record = capture.all.first {
                $0.level == .debug && $0.metadata[CassandraClient.LogKey.query] != nil
            }
            XCTAssertNotNil(record, "expected a slow-query record for the prepared execute")
            XCTAssertEqual(record?.metadata[CassandraClient.LogKey.query], "\(cql)")
        }
    }

    /// A batch's record carries the "batch" operation label (batch has no single query text).
    func testBatchLogsOperationLabel() throws {
        runAsyncAndWaitFor {
            let (client, capture, keyspace) = self.makeCapturingClient()
            defer { XCTAssertNoThrow(try client.shutdown()) }
            let table = "log_v6c_\(DispatchTime.now().uptimeNanoseconds)"
            try await self.createKeyspaceAndTable(
                client,
                keyspace: keyspace,
                table: table,
                schema: "(id int primary key, name text)"
            )

            capture.clear()
            try await client.batch { batch in
                try batch.add(
                    statement: CassandraClient.Statement(
                        query: "insert into \(table) (id, name) values (?, ?);",
                        parameters: [.int32(1), .string("a")]
                    )
                )
            }

            let record = capture.all.first { $0.metadata[CassandraClient.LogKey.query] == "batch" }
            XCTAssertNotNil(record, "expected a batch record labelled \"batch\"")
        }
    }

    /// A connect failure (unroutable host) is logged exactly once. Does NOT need the cluster.
    func testConnectFailureLoggedOnce() throws {
        runAsyncAndWaitFor {
            var config = CassandraClient.Configuration(
                contactPointsProvider: { callback in callback(.success(["240.0.0.1"])) },  // unroutable
                port: 9042,
                protocolVersion: .v3
            )
            config.keyspace = "test"
            config.connectTimeoutMillis = 2_000
            let (logger, capture) = makeCapturingLogger()
            let client = CassandraClient(configuration: config, logger: logger)
            defer { XCTAssertNoThrow(try client.shutdown()) }
            let session = client.makeSession(keyspace: "test")
            defer { try? session.shutdown() }

            do {
                try await session.run("select release_version from system.local")
                XCTFail("expected a connect failure")
            } catch {
                // expected
            }

            let failures = capture.all.filter { $0.metadata[CassandraClient.LogKey.errorCategory] != nil }
            XCTAssertEqual(failures.count, 1, "connect failure should be logged exactly once, not per waiter")
        }
    }

    /// A preflight (binding) failure is logged even though it throws before the request is sent.
    func testPreflightFailureLogged() throws {
        runAsyncAndWaitFor {
            let (client, capture, keyspace) = self.makeCapturingClient()
            defer { XCTAssertNoThrow(try client.shutdown()) }
            let table = "log_v1c_\(DispatchTime.now().uptimeNanoseconds)"
            try await self.createKeyspaceAndTable(
                client,
                keyspace: keyspace,
                table: table,
                schema: "(id bigint primary key, v text)"
            )

            let session = client.makeSession(keyspace: keyspace)
            defer { try? session.shutdown() }
            let prepared = try await session.prepare("insert into \(table) (id, v) values (?, ?)")
            capture.clear()

            do {
                // Bind more parameters than the statement has placeholders -> preflight bind failure.
                _ = try await session.execute(
                    prepared: prepared,
                    parameters: [.int64(1), .string("x"), .string("extra")]
                )
                XCTFail("expected a preflight binding failure")
            } catch {
                // expected
            }

            let failures = capture.all.filter { $0.metadata[CassandraClient.LogKey.errorCategory] != nil }
            XCTAssertFalse(failures.isEmpty, "preflight binding failure should be logged even before a request is sent")
        }
    }

    /// PII: a bound value must not appear anywhere in captured logs (incl. the pre-existing `.trace` dump)
    /// when `logBoundValues` is off (default), and must appear (capped) when it's explicitly enabled.
    /// Exercises the real `Session` call site — not the `RequestLog` helper directly.
    func testBoundValuesRespectConfigFlag() throws {
        runAsyncAndWaitFor {
            let table = "log_pii_\(DispatchTime.now().uptimeNanoseconds)"

            func leaks(_ marker: String, in capture: TestLogCapture) -> Bool {
                capture.all.contains { entry in
                    entry.message.contains(marker) || entry.metadata.values.contains { "\($0)".contains(marker) }
                }
            }

            // Off (default): the bound value must not leak into any captured record.
            do {
                let (client, capture, keyspace) = self.makeCapturingClient(logBoundValues: false)
                defer { XCTAssertNoThrow(try client.shutdown()) }
                try await self.createKeyspaceAndTable(
                    client,
                    keyspace: keyspace,
                    table: table,
                    schema: "(id bigint primary key, v text)"
                )
                let marker = "PII_MARKER_OFF_\(DispatchTime.now().uptimeNanoseconds)"
                capture.clear()
                _ = try await client.execute(
                    statement: CassandraClient.Statement(
                        query: "insert into \(table) (id, v) values (?, ?)",
                        parameters: [.int64(1), .string(marker)]
                    )
                )
                XCTAssertFalse(capture.all.isEmpty, "expected at least a slow-query record (threshold=0)")
                XCTAssertFalse(leaks(marker, in: capture), "bound value must not appear when logBoundValues is off")
            }

            // On: the bound value should appear, capped, in the captured records.
            do {
                let (client, capture, _) = self.makeCapturingClient(logBoundValues: true)
                defer { XCTAssertNoThrow(try client.shutdown()) }
                let marker = "PII_MARKER_ON_\(DispatchTime.now().uptimeNanoseconds)"
                capture.clear()
                _ = try await client.execute(
                    statement: CassandraClient.Statement(
                        query: "insert into \(table) (id, v) values (?, ?)",
                        parameters: [.int64(2), .string(marker)]
                    )
                )
                XCTAssertTrue(leaks(marker, in: capture), "bound value should appear when logBoundValues is on")
            }
        }
    }

}
