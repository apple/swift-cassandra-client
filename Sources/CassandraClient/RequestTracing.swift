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

import Tracing

extension CassandraClient {
    /// Shared request-tracing helper. Like ``RequestLog``, it holds no `Session` reference, so it is unit-testable
    /// with an in-memory tracer and a synthesized error — no cluster.
    internal enum RequestTrace {
        /// Wrap an async request body in a `.client` span carrying the OpenTelemetry DB attributes, recording a
        /// sanitized error status on throw, and rethrowing the original error unchanged.
        ///
        /// Uses a *manual* span (not the `withSpan` closure) deliberately: it avoids the operation-closure
        /// `@Sendable` capture question for the non-`Sendable` `Statement`, and it avoids `withSpan`'s
        /// auto-`recordError`, which would serialize the raw `Error.description` (the server message is PII).
        @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
        static func traced<Value>(
            _ operation: SpanName,
            query: String?,
            consistency: CassandraClient.Consistency?,
            keyspace: String?,
            _ body: () async throws -> Value
        ) async throws -> Value {
            var span = InstrumentationSystem.tracer.startAnySpan(operation.label, ofKind: .client)
            defer { span.end() }

            // Skip all attribute/string work when the span isn't recording (e.g. the NoOp tracer or a
            // sampled-out span), so the disabled path stays cheap.
            if span.isRecording {
                span.attributes[TraceAttributeKey.system] = "cassandra"
                span.attributes[TraceAttributeKey.operation] = operation.operation
                if let query {
                    span.attributes[TraceAttributeKey.queryText] = query
                }
                if let keyspace {
                    span.attributes[TraceAttributeKey.namespace] = keyspace
                }
                if let consistency {
                    span.attributes[TraceAttributeKey.consistencyLevel] = Self.otelToken(consistency)
                }
            }

            do {
                return try await body()
            } catch {
                if span.isRecording {
                    // PII: never hand the raw error to the span — `Error.description` embeds the server message.
                    // Set the status from the code label only, and tag the shared failure category
                    // (`db.cassandra.error.category`).
                    if let cassError = error as? CassandraClient.Error {
                        span.setStatus(SpanStatus(code: .error, message: cassError.shortDescription))
                        span.attributes[TraceAttributeKey.errorCategory] = cassError.category.rawValue
                    } else {
                        span.setStatus(SpanStatus(code: .error))
                    }
                }
                throw error
            }
        }

        /// Map a consistency level to its OpenTelemetry `db.cassandra.consistency_level` token. Kept here (not a
        /// `CustomStringConvertible` on the public ``CassandraClient/Consistency``) and exhaustive, so a new
        /// enum case can't silently emit a wrong string.
        static func otelToken(_ consistency: CassandraClient.Consistency) -> String {
            switch consistency {
            case .any: return "any"
            case .one: return "one"
            case .two: return "two"
            case .three: return "three"
            case .quorum: return "quorum"
            case .all: return "all"
            case .localQuorum: return "local_quorum"
            case .eachQuorum: return "each_quorum"
            case .serial: return "serial"
            case .localSerial: return "local_serial"
            case .localOne: return "local_one"
            }
        }
    }
}
