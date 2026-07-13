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

// MARK: - Centralised metric string constants

extension CassandraClient {
    /// Driver request-latency gauge names, in microseconds (see ``CassandraMetrics``).
    internal enum RequestMetric {
        static let latencyMin = "cassandra.requests.latency_us.min"
        static let latencyMax = "cassandra.requests.latency_us.max"
        static let latencyMean = "cassandra.requests.latency_us.mean"
        static let latencyStdDev = "cassandra.requests.latency_us.stddev"
        static let latencyMedian = "cassandra.requests.latency_us.median"
        static let latencyP75 = "cassandra.requests.latency_us.p75"
        static let latencyP95 = "cassandra.requests.latency_us.p95"
        static let latencyP98 = "cassandra.requests.latency_us.p98"
        static let latencyP99 = "cassandra.requests.latency_us.p99"
        static let latencyP999 = "cassandra.requests.latency_us.p999"
    }

    /// Driver connection gauge names.
    internal enum ConnectionMetric {
        static let total = "cassandra.connections.total"
    }

    /// Dimension keys shared by the emitted metrics.
    internal enum MetricDimension {
        static let session = "session"
    }
}
