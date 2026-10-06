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
import Metrics
import MetricsTestKit
import NIOConcurrencyHelpers
import XCTest

@testable import CassandraClient

/// Integration tests for the metrics poller. Require a live cluster (`CASSANDRA_HOST`).
final class MetricsPollerIntegrationTests: XCTestCase {
    private var testMetrics: TestMetrics { MetricsTestSupport.testMetrics }
    private let connectionsTotal = "cassandra.connections.total"

    override func setUp() {
        super.setUp()
        MetricsTestSupport.bootstrap()
        self.testMetrics.reset()
    }

    /// Base configuration pointed at the test cluster; callers set the metrics knobs.
    private func makeConfiguration() -> CassandraClient.Configuration {
        let env = ProcessInfo.processInfo.environment
        var configuration = CassandraClient.Configuration(
            contactPointsProvider: { callback in
                callback(.success([env["CASSANDRA_HOST"] ?? "127.0.0.1"]))
            },
            port: env["CASSANDRA_CQL_PORT"].flatMap(Int32.init) ?? 9042,
            protocolVersion: .v3
        )
        if let username = env["CASSANDRA_USER"], let password = env["CASSANDRA_PASSWORD"] {
            configuration.authenticator = CassandraClient.PasswordAuthenticator(username: username, password: password)
        }
        configuration.connectTimeout = .milliseconds(10_000)
        configuration.requestTimeout = .milliseconds(24_000)
        return configuration
    }

    /// Poll `testMetrics` until the named meter has a recorded value (i.e. the poller ticked), or
    /// time out. Waiting for a value — not merely the handle — matters because `SnapshotGauges`
    /// pre-creates every `Meter` at poller start, so the handle exists before the first tick's `set`.
    private func waitForMeter(
        _ label: String,
        dimensions: [(String, String)],
        timeout: TimeInterval = 5
    ) async throws -> TestMeter? {
        let deadline = Date().addingTimeInterval(timeout)
        while Date() < deadline {
            if let meter = try? self.testMetrics.expectMeter(label, dimensions),
                meter.lastValue != nil
            {
                return meter
            }
            try await Task.sleep(for: .milliseconds(20))
        }
        return nil
    }

    // The poller ticks and bridges the driver snapshot onto the gauges.
    func testPollerRecordsSnapshotGauges() async throws {
        let session = "v2"
        let dims = [("session", session)]
        var configuration = self.makeConfiguration()
        configuration.metricsEnabled = true
        configuration.metricsPollInterval = .milliseconds(100)
        configuration.metricsSessionName = session

        try await CassandraClient.withClient(configuration: configuration) { client in
            // Force a connect + a real request so the driver has latency to report.
            _ = try await client.query("select release_version from system.local")

            // Wait until the poller has ticked at least once.
            let meter = try await self.waitForMeter(self.connectionsTotal, dimensions: dims)
            let total = try XCTUnwrap(meter)
            XCTAssertGreaterThan(try XCTUnwrap(total.lastValue), 0)

            // Every gauge must equal the corresponding field of a same-moment `getMetrics()` reading
            // (µs preserved). The driver's histogram is cumulative and frozen while idle, so once the
            // request's latency is folded in, one snapshot matches all 11 gauges exactly. Retry to skip
            // any tick that lands mid-drain; converges on the first consistent snapshot while idle.
            let deadline = Date().addingTimeInterval(5)
            var lastMismatch: String?
            repeat {
                let snapshot = client.getMetrics()
                let expected = CassandraClient.MetricsMapping.gaugeValues(from: snapshot)
                lastMismatch =
                    expected.first { pair in
                        let recorded = (try? self.testMetrics.expectMeter(pair.name, dims))?.lastValue
                        return recorded.map { UInt($0) } != pair.value
                    }?.name
                if lastMismatch == nil { return }
                try await Task.sleep(for: .milliseconds(20))
            } while Date() < deadline
            XCTFail(
                "gauges never matched a same-moment getMetrics() snapshot; last mismatch: \(lastMismatch ?? "?")"
            )
        }
    }

    // No gauge is recorded after shutdown, though `run()` and its poller are still running: a tick records
    // only while the session is connected and shutdown has not begun.
    func testShutdownStopsPoller() async throws {
        let session = "v3"
        let dims = [("session", session)]
        var configuration = self.makeConfiguration()
        configuration.metricsEnabled = true
        configuration.metricsPollInterval = .milliseconds(50)
        configuration.metricsSessionName = session

        try await CassandraClient.withClient(configuration: configuration) { client in
            _ = try await client.query("select release_version from system.local")
            let polled = try await self.waitForMeter(self.connectionsTotal, dimensions: dims)
            let meter = try XCTUnwrap(polled)

            try await client.shutdownAsync()

            // No tick after shutdown: the recorded-values count must stay put across several intervals.
            let countAfterShutdown = meter.values.count
            try await Task.sleep(for: .milliseconds(500))
            XCTAssertEqual(meter.values.count, countAfterShutdown)
        }
    }

    // With metrics disabled no gauge series are ever created.
    func testDisabledCreatesNoGauges() async throws {
        var configuration = self.makeConfiguration()
        configuration.metricsEnabled = false
        configuration.metricsPollInterval = .milliseconds(50)
        configuration.metricsSessionName = "v4"

        try await CassandraClient.withClient(configuration: configuration) { client in
            _ = try await client.query("select release_version from system.local")
            try await Task.sleep(for: .milliseconds(400))  // several would-be intervals
        }

        XCTAssertTrue(
            self.testMetrics.meters.allSatisfy { $0.label != self.connectionsTotal },
            "no gauges should exist when metrics are disabled"
        )
    }

    // metricsEnabled but interval nil or 0 => poller off, no gauges.
    func testNilAndZeroIntervalDisablePoller() async throws {
        try await self.assertIntervalDisablesPoller(nil)
        try await self.assertIntervalDisablesPoller(.zero)
    }

    private func assertIntervalDisablesPoller(_ interval: Duration?) async throws {
        self.testMetrics.reset()
        var configuration = self.makeConfiguration()
        configuration.metricsEnabled = true
        configuration.metricsPollInterval = interval
        configuration.metricsSessionName = "v5"

        try await CassandraClient.withClient(configuration: configuration) { client in
            _ = try await client.query("select release_version from system.local")
            try await Task.sleep(for: .milliseconds(300))
        }

        XCTAssertTrue(
            self.testMetrics.meters.allSatisfy { $0.label != self.connectionsTotal },
            "interval \(String(describing: interval)) should not schedule the poller"
        )
    }

    // Shutdown during an in-flight connect: the connect's compare-and-set loses, so the session never
    // connects and no tick records, though `run()` is running.
    func testShutdownDuringInFlightConnect() async throws {
        let session = "v6"
        let completionBox =
            NIOLockedValueBox<(@Sendable (Result<[String], Swift.Error>) -> Void)?>(nil)
        let providerInvoked = self.expectation(description: "connect started")
        let env = ProcessInfo.processInfo.environment
        let host = env["CASSANDRA_HOST"] ?? "127.0.0.1"

        var configuration = self.makeConfiguration()
        configuration.metricsEnabled = true
        configuration.metricsPollInterval = .milliseconds(50)
        configuration.metricsSessionName = session
        // Withhold the contact points: capture the completion and signal, but don't call it yet.
        configuration.contactPointsProvider = { completion in
            completionBox.withLockedValue { $0 = completion }
            providerInvoked.fulfill()
        }

        try await CassandraClient.withClient(configuration: configuration) { client in
            // Kick off a connect; it blocks in the withheld provider.
            let query = Task {
                _ = try? await client.query("select release_version from system.local")
            }
            await self.fulfillment(of: [providerInvoked], timeout: 5)

            // Shut down while the connect is still in flight.
            try await client.shutdownAsync()

            // Now let the connect finish; the CAS must lose, so no tick records.
            completionBox.withLockedValue { $0 }?(.success([host]))
            await query.value
            try await Task.sleep(for: .milliseconds(300))  // past several would-be ticks
        }

        XCTAssertTrue(
            self.testMetrics.meters.allSatisfy { $0.label != self.connectionsTotal },
            "no tick may record when the connect's CAS loses to shutdown"
        )
        // Reaching here without an assertion failure confirms deinit's invariant held.
    }
}
