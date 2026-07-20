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

import OTelSemanticConventions
import Tracing

extension CassandraClient {
    /// Shared request-tracing helper. Like ``RequestLog``, it holds no `Session` reference, so it is unit-testable
    /// with an in-memory tracer and a synthesized error — no cluster.
    internal enum RequestTrace {
        /// Wrap an async request body in a `.client` span carrying the OpenTelemetry DB attributes, recording a
        /// sanitized error status on failure, and rethrowing the original error unchanged.
        ///
        /// The operation closure returns a `Result` rather than throwing so that `withSpan`'s built-in error
        /// recording never runs — it would serialize the raw `Error.description`, which embeds the
        /// server-provided message (PII). We record only the code label + shared category ourselves.
        @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
        static func traced<Value>(
            _ operation: SpanName,
            query: String?,
            consistency: CassandraClient.Consistency?,
            keyspace: String?,
            _ body: () async throws -> Value
        ) async throws -> Value {
            // Use `withSpan` for span lifecycle + task-local context propagation. The operation closure
            // returns a `Result` instead of throwing, so `withSpan`'s built-in error recording — which would
            // serialize the raw `Error.description` (the server-provided message, PII) — never fires; we record
            // a sanitized status ourselves and rethrow the original error to the caller.
            let result: Result<Value, Swift.Error> = await withSpan(operation.label, ofKind: .client) { span in
                if span.isRecording {
                    span.attributes.db.system.name = .init(rawValue: "cassandra")
                    span.attributes.db.operation.name = operation.operation
                    if let query {
                        span.attributes.db.query.text = query
                    }
                    if let keyspace {
                        span.attributes.db.namespace = keyspace
                    }
                    if let consistency {
                        span.attributes[TraceAttributeKey.consistencyLevel] = Self.otelToken(consistency)
                    }
                }
                do {
                    return .success(try await body())
                } catch {
                    if span.isRecording {
                        // PII: never hand the raw error to the span — `Error.description` embeds the server
                        // message. Record the code label only, plus the shared failure category.
                        if let cassError = error as? CassandraClient.Error {
                            span.setStatus(SpanStatus(code: .error, message: cassError.shortDescription))
                            span.attributes[TraceAttributeKey.errorCategory] = cassError.category.rawValue
                        } else {
                            span.setStatus(SpanStatus(code: .error))
                        }
                    }
                    return .failure(error)
                }
            }
            return try result.get()
        }

        /// Map a consistency level to its OpenTelemetry `cassandra.consistency.level` token — exhaustive, so a
        /// new ``CassandraClient/Consistency`` case can't silently emit a wrong string.
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
