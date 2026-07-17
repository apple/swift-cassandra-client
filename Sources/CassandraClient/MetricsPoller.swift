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

import Metrics

extension CassandraClient {
    /// Maps a driver snapshot onto the set of gauges published by the metrics poller.
    ///
    /// Kept free of scheduling so it can be unit-tested with a synthesized snapshot. Only the
    /// latency percentiles and total connection count are published; the driver's pre-computed
    /// rates and the deprecated snapshot fields are excluded (a metrics backend derives rate from
    /// a request counter, and the deprecated fields carry no signal).
    internal enum MetricsMapping {
        /// The `(name, value)` pairs to record for `metrics`, latencies in microseconds.
        static func gaugeValues(from metrics: CassandraMetrics) -> [(name: String, value: UInt)] {
            [
                (RequestMetric.latencyMin, metrics.requestsMin),
                (RequestMetric.latencyMax, metrics.requestsMax),
                (RequestMetric.latencyMean, metrics.requestsMean),
                (RequestMetric.latencyStdDev, metrics.requestsStdDev),
                (RequestMetric.latencyMedian, metrics.requestsMedian),
                (RequestMetric.latencyP75, metrics.requestsPercentile75th),
                (RequestMetric.latencyP95, metrics.requestsPercentile95th),
                (RequestMetric.latencyP98, metrics.requestsPercentile98th),
                (RequestMetric.latencyP99, metrics.requestsPercentile99th),
                (RequestMetric.latencyP999, metrics.requestsPercentile999th),
                (ConnectionMetric.total, metrics.statsTotalConnections),
            ]
        }
    }

    /// A pre-created `Meter` per published gauge, built once at poller start.
    ///
    /// Each tick calls ``record(_:)`` to `set` the current snapshot onto the handles; the handles
    /// themselves are never re-created, so a tick allocates nothing. `Sendable` so the poller's
    /// tick closure can capture it across the concurrency boundary.
    internal struct SnapshotGauges: Sendable {
        private let meters: [String: Meter]

        /// Creates a gauge per published name. When `sessionName` is set it is attached as a
        /// `session` dimension on every gauge, so multiple sessions don't clobber each other's series.
        init(sessionName: String?) {
            let dimensions: [(String, String)] =
                sessionName.map { [(MetricDimension.session, $0)] } ?? []
            var meters: [String: Meter] = [:]
            for (name, _) in MetricsMapping.gaugeValues(from: .zero) {
                meters[name] = Meter(label: name, dimensions: dimensions)
            }
            self.meters = meters
        }

        /// Sets the current snapshot values onto the pre-created gauges.
        func record(_ metrics: CassandraMetrics) {
            for (name, value) in MetricsMapping.gaugeValues(from: metrics) {
                self.meters[name]?.set(value)
            }
        }
    }
}
