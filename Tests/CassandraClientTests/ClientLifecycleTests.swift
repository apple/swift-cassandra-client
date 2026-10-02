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
import Logging
import ServiceLifecycle
import XCTest

/// Unit tests for ``CassandraClient/withClient(eventLoopGroup:configuration:_:)`` and ``CassandraClient/run()``,
/// including graceful shutdown through a `ServiceGroup`.
/// No cluster required: no test issues a request before the client is shut down, and a request after
/// shutdown fails with `.disconnected` without connecting.
final class ClientLifecycleTests: XCTestCase {
    private struct BodyError: Error, Equatable {}

    private static func makeConfiguration() -> CassandraClient.Configuration {
        CassandraClient.Configuration(
            contactPointsProvider: { callback in callback(.success(["127.0.0.1"])) },
            port: 9042,
            protocolVersion: .v3
        )
    }

    private static func assertDisconnected(
        _ client: CassandraClient,
        file: StaticString = #filePath,
        line: UInt = #line
    ) async {
        await assertThrowsErrorAsync(
            try await client.query("select release_version from system.local"),
            file: file,
            line: line
        ) { error in
            XCTAssertEqual(error as? CassandraClient.Error, .disconnected, file: file, line: line)
        }
    }

    func testWithClientReturnsBodyValue() async throws {
        let value = try await CassandraClient.withClient(configuration: Self.makeConfiguration()) { _ in 42 }
        XCTAssertEqual(value, 42)
    }

    func testWithClientRethrowsBodyError() async {
        await assertThrowsErrorAsync(
            try await CassandraClient.withClient(configuration: Self.makeConfiguration()) { _ in
                throw BodyError()
            }
        ) { error in
            XCTAssertEqual(error as? BodyError, BodyError())
        }
    }

    func testWithClientShutsClientDown() async throws {
        let client = try await CassandraClient.withClient(configuration: Self.makeConfiguration()) { $0 }
        await Self.assertDisconnected(client)
    }

    func testWithClientShutsClientDownAfterBodyThrows() async {
        var escaped: CassandraClient?
        await assertThrowsErrorAsync(
            try await CassandraClient.withClient(configuration: Self.makeConfiguration()) { client in
                escaped = client
                throw BodyError()
            }
        )
        guard let client = escaped else {
            return XCTFail("body did not run")
        }
        await Self.assertDisconnected(client)
    }

    func testRunReturnsOnCancellationAndShutsClientDown() async throws {
        let client = CassandraClient(configuration: Self.makeConfiguration())
        let running = Task { try await client.run() }
        running.cancel()
        try await running.value
        await Self.assertDisconnected(client)
    }

    func testRunReturnsOnGracefulShutdownAndShutsClientDown() async throws {
        let client = CassandraClient(configuration: Self.makeConfiguration())
        let serviceGroup = ServiceGroup(services: [client], logger: Logger(label: "test"))
        // A shutdown requested before the group runs is remembered: the group starts the client's `run()`
        // and then shuts it down gracefully.
        await serviceGroup.triggerGracefulShutdown()
        try await serviceGroup.run()
        await Self.assertDisconnected(client)
    }

    func testRunAfterShutdownThrowsDisconnected() async throws {
        let client = CassandraClient(configuration: Self.makeConfiguration())
        try await client.shutdownAsync()
        await assertThrowsErrorAsync(try await client.run()) { error in
            XCTAssertEqual(error as? CassandraClient.Error, .disconnected)
        }
    }

    // A second `run()` would otherwise wait for a cancellation that, inside `withClient`, never comes.
    func testSecondRunThrows() async throws {
        try await CassandraClient.withClient(configuration: Self.makeConfiguration()) { client in
            await assertThrowsErrorAsync(try await client.run()) { error in
                XCTAssertEqual(
                    error as? CassandraClient.Error,
                    .invalidState("run() has already been called on this client")
                )
            }
        }
    }

    // The metrics poller runs as a child of `run()`; cancelling `run()` must end it too.
    func testRunWithMetricsPollerReturnsOnCancellation() async throws {
        var configuration = Self.makeConfiguration()
        configuration.metricsEnabled = true
        configuration.metricsPollInterval = .milliseconds(10)
        let client = CassandraClient(configuration: configuration)
        let running = Task { try await client.run() }
        running.cancel()
        try await running.value
        await Self.assertDisconnected(client)
    }
}
