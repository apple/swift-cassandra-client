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

// MARK: - Centralised tracing span-name and attribute-key constants

extension CassandraClient {
    /// Low-cardinality span operation names + their OpenTelemetry `db.operation.name` value. The span name
    /// is the operation (never the query text) so trace backends can group requests.
    internal enum SpanName {
        case execute
        case prepare
        case batch

        /// The span's display name (low cardinality).
        var label: String {
            switch self {
            case .execute: return "Cassandra execute"
            case .prepare: return "Cassandra prepare"
            case .batch: return "Cassandra batch"
            }
        }

        /// The `db.operation.name` attribute value.
        var operation: String {
            switch self {
            case .execute: return "execute"
            case .prepare: return "prepare"
            case .batch: return "batch"
            }
        }
    }

    /// Attribute keys not taken from swift-otel-semantic-conventions' stable `db.*` API. The Cassandra
    /// (`cassandra.*`) attributes are gated behind that package's non-default `Experimental` trait (they're
    /// UNSTABLE upstream), so we emit them as our own constants rather than depend on an experimental surface.
    internal enum TraceAttributeKey {
        /// Current OTel key (`db.cassandra.consistency_level` is deprecated upstream in favor of this).
        static let consistencyLevel = "cassandra.consistency.level"
        /// Shared failure classification — the same concept logging emits as `request.errorCategory` and
        /// metrics tag as `errorCategory`; value is the ``CassandraClient/Error`` category (`category.rawValue`).
        static let errorCategory = "cassandra.error.category"
    }
}
