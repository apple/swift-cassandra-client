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

import CDataStaxDriver
import CoreMetrics
import Metrics
import MetricsTestKit
import XCTest

@testable import CassandraClient

/// Unit tests for the snapshot->gauge mapping. No cluster required.
final class MetricsPollerTests: XCTestCase {
    private var testMetrics: TestMetrics { MetricsTestSupport.testMetrics }

    override func setUp() {
        super.setUp()
        MetricsTestSupport.bootstrap()
        self.testMetrics.reset()
    }

    /// Build a snapshot with a distinct value per field so mapping mistakes are visible.
    private func makeSnapshot() -> CassandraMetrics {
        var m = CassMetrics()
        m.requests.min = 1
        m.requests.max = 2
        m.requests.mean = 3
        m.requests.stddev = 4
        m.requests.median = 5
        m.requests.percentile_75th = 6
        m.requests.percentile_95th = 7
        m.requests.percentile_98th = 8
        m.requests.percentile_99th = 9
        m.requests.percentile_999th = 10
        // Rates and deprecated fields are set to non-zero to prove they are NOT published.
        m.requests.mean_rate = 111
        m.requests.one_minute_rate = 222
        m.stats.total_connections = 42
        m.stats.available_connections = 7
        m.stats.exceeded_pending_requests_water_mark = 8
        m.errors.connection_timeouts = 9
        return CassandraMetrics(metrics: m)
    }

    // The pure mapping produces exactly the decided gauges with the snapshot values (µs).
    func testMappingProducesOnlyDecidedGauges() {
        let pairs = CassandraClient.MetricsMapping.gaugeValues(from: self.makeSnapshot())
        let byName = Dictionary(uniqueKeysWithValues: pairs.map { ($0.name, $0.value) })

        let expected: [String: UInt] = [
            "cassandra.requests.latency_us.min": 1,
            "cassandra.requests.latency_us.max": 2,
            "cassandra.requests.latency_us.mean": 3,
            "cassandra.requests.latency_us.stddev": 4,
            "cassandra.requests.latency_us.median": 5,
            "cassandra.requests.latency_us.p75": 6,
            "cassandra.requests.latency_us.p95": 7,
            "cassandra.requests.latency_us.p98": 8,
            "cassandra.requests.latency_us.p99": 9,
            "cassandra.requests.latency_us.p999": 10,
            "cassandra.connections.total": 42,
        ]
        XCTAssertEqual(byName, expected)
        // Exactly 11 series, no duplicates.
        XCTAssertEqual(pairs.count, 11)
        // No rate or deprecated-field series leak in.
        for name in byName.keys {
            XCTAssertFalse(name.contains("rate"))
            XCTAssertFalse(name.contains("available"))
            XCTAssertFalse(name.contains("water_mark"))
            XCTAssertFalse(name.contains("timeout"))
        }
    }

    // With a session name every gauge carries the `session` dimension; without, none does.
    func testSessionDimensionAppliedWhenNamed() throws {
        let gauges = CassandraClient.SnapshotGauges(sessionName: "svc-a")
        gauges.record(self.makeSnapshot())

        // Each decided gauge exists with the session dimension and the mapped value.
        let meter = try self.testMetrics.expectMeter(
            "cassandra.requests.latency_us.p99",
            [("session", "svc-a")]
        )
        XCTAssertEqual(meter.lastValue, 9)
        let total = try self.testMetrics.expectMeter(
            "cassandra.connections.total",
            [("session", "svc-a")]
        )
        XCTAssertEqual(total.lastValue, 42)
    }

    func testNoDimensionWhenUnnamed() throws {
        let gauges = CassandraClient.SnapshotGauges(sessionName: nil)
        gauges.record(self.makeSnapshot())

        let meter = try self.testMetrics.expectMeter("cassandra.requests.latency_us.min", [])
        XCTAssertEqual(meter.lastValue, 1)
        // No dimensions attached.
        XCTAssertTrue(meter.dimensions.isEmpty)
    }
}
