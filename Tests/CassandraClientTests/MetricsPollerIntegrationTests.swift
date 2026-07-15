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

import Dispatch
import Foundation
import Logging
import Metrics
import MetricsTestKit
import NIO
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
        configuration.username = env["CASSANDRA_USER"]
        configuration.password = env["CASSANDRA_PASSWORD"]
        configuration.connectTimeoutMillis = UInt32(10_000)
        configuration.requestTimeoutMillis = UInt32(24_000)
        return configuration
    }

    private func makeLogger() -> Logger {
        var logger = Logger(label: "metrics-poller-test")
        logger.logLevel = .info
        return logger
    }

    /// Poll `testMetrics` until the named meter has a recorded value (i.e. the poller ticked), or
    /// time out. Waiting for a value — not merely the handle — matters because `SnapshotGauges`
    /// pre-creates every `Meter` at poller start, so the handle exists before the first tick's `set`.
    private func waitForMeter(
        _ label: String,
        dimensions: [(String, String)],
        timeout: TimeInterval = 5
    ) -> TestMeter? {
        let deadline = Date().addingTimeInterval(timeout)
        while Date() < deadline {
            if let meter = try? self.testMetrics.expectMeter(label, dimensions),
                meter.lastValue != nil
            {
                return meter
            }
            Thread.sleep(forTimeInterval: 0.02)
        }
        return nil
    }

    // The poller ticks and bridges the driver snapshot onto the gauges.
    func testPollerRecordsSnapshotGauges() throws {
        let session = "v2"
        let dims = [("session", session)]
        var configuration = self.makeConfiguration()
        configuration.metricsEnabled = true
        configuration.metricsPollIntervalMillis = 100
        configuration.metricsSessionName = session

        let client = CassandraClient(configuration: configuration, logger: self.makeLogger())
        defer { XCTAssertNoThrow(try client.shutdown()) }

        // Force a connect + a real request so the driver has latency to report.
        _ = try client.query("select release_version from system.local").wait()

        // Wait until the poller has ticked at least once.
        let total = try XCTUnwrap(self.waitForMeter(self.connectionsTotal, dimensions: dims))
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
            Thread.sleep(forTimeInterval: 0.02)
        } while Date() < deadline
        XCTFail(
            "gauges never matched a same-moment getMetrics() snapshot; last mismatch: \(lastMismatch ?? "?")"
        )
    }

    // Shutdown stops the poller: no gauge is recorded after shutdown returns. The poller reads the
    // snapshot only while `.connected` under the state lock, and shutdown flips the state before
    // closing, so a cancelled tick cannot record against a closing session.
    func testShutdownStopsPoller() throws {
        let session = "v3"
        let dims = [("session", session)]
        var configuration = self.makeConfiguration()
        configuration.metricsEnabled = true
        configuration.metricsPollIntervalMillis = 50
        configuration.metricsSessionName = session

        let client = CassandraClient(configuration: configuration, logger: self.makeLogger())
        // Defensive: guarantee shutdown even if the connect throws, so deinit's precondition holds.
        defer { try? client.shutdown() }
        _ = try client.query("select release_version from system.local").wait()
        let meter = try XCTUnwrap(self.waitForMeter(self.connectionsTotal, dimensions: dims))

        XCTAssertNoThrow(try client.shutdown())

        // No tick after shutdown: the recorded-values count must stay put across several intervals.
        let countAfterShutdown = meter.values.count
        Thread.sleep(forTimeInterval: 0.5)
        XCTAssertEqual(meter.values.count, countAfterShutdown)
    }

    // With metrics disabled no gauge series are ever created.
    func testDisabledCreatesNoGauges() throws {
        var configuration = self.makeConfiguration()
        configuration.metricsEnabled = false
        configuration.metricsPollIntervalMillis = 50
        configuration.metricsSessionName = "v4"

        let client = CassandraClient(configuration: configuration, logger: self.makeLogger())
        defer { XCTAssertNoThrow(try client.shutdown()) }

        _ = try client.query("select release_version from system.local").wait()
        Thread.sleep(forTimeInterval: 0.4)  // several would-be intervals

        XCTAssertTrue(
            self.testMetrics.meters.allSatisfy { $0.label != self.connectionsTotal },
            "no gauges should exist when metrics are disabled"
        )
    }

    // metricsEnabled but interval nil or 0 => poller off, no gauges.
    func testNilAndZeroIntervalDisablePoller() throws {
        try self.assertIntervalDisablesPoller(nil)
        try self.assertIntervalDisablesPoller(0)
    }

    private func assertIntervalDisablesPoller(_ interval: UInt32?) throws {
        self.testMetrics.reset()
        var configuration = self.makeConfiguration()
        configuration.metricsEnabled = true
        configuration.metricsPollIntervalMillis = interval
        configuration.metricsSessionName = "v5"

        let client = CassandraClient(configuration: configuration, logger: self.makeLogger())
        defer { try? client.shutdown() }

        _ = try client.query("select release_version from system.local").wait()
        Thread.sleep(forTimeInterval: 0.3)

        XCTAssertTrue(
            self.testMetrics.meters.allSatisfy { $0.label != self.connectionsTotal },
            "interval \(String(describing: interval)) should not schedule the poller"
        )
    }

    // Shutdown during an in-flight connect: the connect's compare-and-set loses, poller never starts, deinit holds.
    func testShutdownDuringInFlightConnect() throws {
        let session = "v6"
        let completionBox =
            NIOLockedValueBox<(@Sendable (Result<[String], Swift.Error>) -> Void)?>(nil)
        let providerInvoked = DispatchSemaphore(value: 0)
        let env = ProcessInfo.processInfo.environment
        let host = env["CASSANDRA_HOST"] ?? "127.0.0.1"

        var configuration = self.makeConfiguration()
        configuration.metricsEnabled = true
        configuration.metricsPollIntervalMillis = 50
        configuration.metricsSessionName = session
        // Withhold the contact points: capture the completion and signal, but don't call it yet.
        configuration.contactPointsProvider = { completion in
            completionBox.withLockedValue { $0 = completion }
            providerInvoked.signal()
        }

        // A shared group so `client.shutdown()` doesn't tear down the loop the in-flight connect
        // still uses when we later release the withheld completion.
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { XCTAssertNoThrow(try group.syncShutdownGracefully()) }
        let client = CassandraClient(
            eventLoopGroupProvider: .shared(group),
            configuration: configuration,
            logger: self.makeLogger()
        )
        defer { try? client.shutdown() }

        // Kick off a connect on a background thread; it blocks in the withheld provider.
        let queryDone = DispatchSemaphore(value: 0)
        DispatchQueue.global().async {
            _ = try? client.query("select release_version from system.local").wait()
            queryDone.signal()
        }
        XCTAssertEqual(providerInvoked.wait(timeout: .now() + 5), .success, "connect never started")

        // Shut down while the connect is still in flight.
        XCTAssertNoThrow(try client.shutdown())

        // Now let the connect finish; the CAS must lose and never start the poller.
        completionBox.withLockedValue { $0 }?(.success([host]))
        _ = queryDone.wait(timeout: .now() + 5)
        Thread.sleep(forTimeInterval: 0.3)  // past several would-be ticks

        XCTAssertTrue(
            self.testMetrics.meters.allSatisfy { $0.label != self.connectionsTotal },
            "poller must not start when the connect's CAS loses to shutdown"
        )
        // Reaching here without a precondition crash confirms deinit's invariant held.
    }
}
