//===----------------------------------------------------------------------===//
//
// This source file is part of the Swift Cassandra Client open source project
//
// Copyright (c) 2022 Apple Inc. and the Swift Cassandra Client project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of Swift Cassandra Client project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

@_implementationOnly import CDataStaxDriver

/// ``CassandraClient`` metrics.
public struct CassandraMetrics: Codable {
  // MARK: - Requests

  /// Minimum in microseconds
  public let requestsMin: UInt
  /// Maximum in microseconds
  public let requestsMax: UInt
  /// Mean in microseconds
  public let requestsMean: UInt
  /// Standard deviation in microseconds
  public let requestsStdDev: UInt
  /// Median in microseconds
  public let requestsMedian: UInt
  /// 75th percentile in microseconds
  public let requestsPercentile75th: UInt
  /// 95th percentile in microseconds
  public let requestsPercentile95th: UInt
  /// 98th percentile in microseconds
  public let requestsPercentile98th: UInt
  /// 99the percentile in microseconds
  public let requestsPercentile99th: UInt
  /// 99.9th percentile in microseconds
  public let requestsPercentile999th: UInt
  ///  Mean rate in requests per second
  public let requestsMeanRate: Double
  /// 1 minute rate in requests per second
  public let requestsOneMinuteRate: Double
  ///  5 minute rate in requests per second
  public let requestsFiveMinuteRate: Double
  /// 15 minute rate in requests per second
  public let requestsFifteenMinuteRate: Double

  // MARK: - Stats

  /// The total number of connections
  public let statsTotalConnections: UInt
  /// The number of connections available to take requests
  public let statsAvailableConnections: UInt
  /// Occurrences when requests exceeded a pool's water mark
  public let statsExceededPendingRequestsWaterMark: UInt
  /// Occurrences when number of bytes exceeded a connection's water mark
  public let statsExceededWriteBytesWaterMark: UInt

  // MARK: - Errors

  /// Occurrences of a connection timeout
  public let errorsConnectionTimeouts: UInt
  /// Occurrences of requests that timed out waiting for a connection
  public let errorsPendingRequestTimeouts: UInt
  /// Occurrences of requests that timed out waiting for a request to finish
  public let errorsRequestTimeouts: UInt

  init(metrics: CDataStaxDriver.CassMetrics) {
    self.requestsMin = UInt(metrics.requests.min)
    self.requestsMax = UInt(metrics.requests.max)
    self.requestsMean = UInt(metrics.requests.mean)
    self.requestsStdDev = UInt(metrics.requests.stddev)
    self.requestsMedian = UInt(metrics.requests.median)
    self.requestsPercentile75th = UInt(metrics.requests.percentile_75th)
    self.requestsPercentile95th = UInt(metrics.requests.percentile_95th)
    self.requestsPercentile98th = UInt(metrics.requests.percentile_98th)
    self.requestsPercentile99th = UInt(metrics.requests.percentile_99th)
    self.requestsPercentile999th = UInt(metrics.requests.percentile_999th)
    self.requestsMeanRate = metrics.requests.mean_rate
    self.requestsOneMinuteRate = metrics.requests.one_minute_rate
    self.requestsFiveMinuteRate = metrics.requests.five_minute_rate
    self.requestsFifteenMinuteRate = metrics.requests.fifteen_minute_rate
    self.statsTotalConnections = UInt(metrics.stats.total_connections)
    self.statsAvailableConnections = UInt(metrics.stats.available_connections)
    self.statsExceededPendingRequestsWaterMark = UInt(
      metrics.stats.exceeded_pending_requests_water_mark)
    self.statsExceededWriteBytesWaterMark = UInt(metrics.stats.exceeded_write_bytes_water_mark)
    self.errorsConnectionTimeouts = UInt(metrics.errors.connection_timeouts)
    self.errorsPendingRequestTimeouts = UInt(metrics.errors.pending_request_timeouts)
    self.errorsRequestTimeouts = UInt(metrics.errors.request_timeouts)
  }
}
