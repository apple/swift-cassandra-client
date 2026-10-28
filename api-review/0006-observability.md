# Observability

## Overview

- **Area:** Observability — 24 of the module's public symbols.
- **Note:** This PR exists to collect API review feedback ahead of 1.0. It will be closed rather than merged; anything it settles lands as its own change.
- **Generated from:** `Sources/CassandraClient` at `e60f45a`, with Apple Swift version 6.3.1 (swiftlang-6.3.1.1.2 clang-2100.0.123.102). Derived, not maintained by hand — see `api-review/README.md` to regenerate.

Inherited protocol members are listed without their standard-library documentation, grouped under the protocol that declared each. Members the symbol graph does not emit are marked `[reconstructed]`.

## Public API

````swift
/// ``CassandraClient`` metrics.
public struct CassandraMetrics : Decodable, Encodable, Hashable, Sendable {
    /// Occurrences of a connection timeout
    public let errorsConnectionTimeouts: UInt
    /// Occurrences of requests that timed out waiting for a connection
    public let errorsPendingRequestTimeouts: UInt
    /// Occurrences of requests that timed out waiting for a request to finish
    public let errorsRequestTimeouts: UInt
    /// 15 minute rate in requests per second
    public let requestsFifteenMinuteRate: Double
    /// 5 minute rate in requests per second
    public let requestsFiveMinuteRate: Double
    /// Maximum in microseconds
    public let requestsMax: UInt
    /// Mean in microseconds
    public let requestsMean: UInt
    /// Mean rate in requests per second
    public let requestsMeanRate: Double
    /// Median in microseconds
    public let requestsMedian: UInt
    /// Minimum in microseconds
    public let requestsMin: UInt
    /// 1 minute rate in requests per second
    public let requestsOneMinuteRate: Double
    /// 75th percentile in microseconds
    public let requestsPercentile75th: UInt
    /// 95th percentile in microseconds
    public let requestsPercentile95th: UInt
    /// 98th percentile in microseconds
    public let requestsPercentile98th: UInt
    /// 99.9th percentile in microseconds
    public let requestsPercentile999th: UInt
    /// 99the percentile in microseconds
    public let requestsPercentile99th: UInt
    /// Standard deviation in microseconds
    public let requestsStdDev: UInt
    /// The number of connections available to take requests
    public let statsAvailableConnections: UInt
    /// Occurrences when requests exceeded a pool's water mark
    public let statsExceededPendingRequestsWaterMark: UInt
    /// Occurrences when number of bytes exceeded a connection's water mark
    public let statsExceededWriteBytesWaterMark: UInt
    /// The total number of connections
    public let statsTotalConnections: UInt

    // [inherited from Equatable] 1 member
    static func != (lhs: Self, rhs: Self) -> Bool

    // [compiler-synthesized] 1 member
    init(from decoder: any Decoder) throws

    // [reconstructed] 2 members guaranteed by a conformance above that the symbol graph does not emit
    public static func == (lhs: Self, rhs: Self) -> Bool
    public func encode(to encoder: Encoder) throws
}
````
