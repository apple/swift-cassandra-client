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
import Logging
import NIOCore

extension CassandraClient {
    /// Shared request-logging helper. Deliberately free of any `Session` reference so it can be unit-tested
    /// directly with a synthesized error and a capturing `LogHandler`.
    internal enum RequestLog {
        /// Log a failed request at `.debug` (library convention — never warning/error), with best-effort metadata.
        /// The error field uses `shortDescription` (code label) — never `description`, which can echo the
        /// server-provided message (potential PII).
        static func logFailure(
            _ error: CassandraClient.Error,
            query: String? = nil,
            consistency: CassandraClient.Consistency? = nil,
            startedAt: DispatchTime? = nil,
            boundValues: String? = nil,
            logger: Logger
        ) {
            let category = error.category
            var metadata: Logger.Metadata = [LogKey.errorCategory: "\(category.rawValue)"]
            if let query {
                metadata[LogKey.query] = "\(Self.truncated(query))"
            }
            if let consistency {
                metadata[LogKey.consistency] = "\(consistency)"
            }
            if let startedAt {
                metadata[LogKey.latencyMs] = "\(Self.elapsedMillis(since: startedAt))"
            }
            if let boundValues {
                metadata[LogKey.boundValues] = "\(boundValues)"
            }
            // The error rides swift-log's `error:` param (first-class on event-based handlers), never the
            // message. A wrapper carries only the code label — the raw `Error.description` embeds the
            // server message (PII), and the param serialises `"\(error)"`.
            logger.debug(
                "request failed",
                error: RedactedError(description: error.shortDescription),
                metadata: metadata
            )
        }

        /// If a successful request's elapsed time is at least the threshold, log a `.debug` "slow query" record.
        /// `thresholdMillis == nil` skips all timing work (hot-path guard); `0` logs every success.
        static func checkSlowSuccess(
            startedAt: DispatchTime,
            query: String,
            thresholdMillis: UInt32?,
            boundValues: String? = nil,
            logger: Logger
        ) {
            guard let thresholdMillis else { return }
            let elapsed = Self.elapsedMillis(since: startedAt)
            guard elapsed >= thresholdMillis else { return }
            var metadata: Logger.Metadata = [
                LogKey.query: "\(Self.truncated(query))",
                LogKey.latencyMs: "\(elapsed)",
            ]
            if let boundValues {
                metadata[LogKey.boundValues] = "\(boundValues)"
            }
            logger.debug("slow query", metadata: metadata)
        }

        /// Emit the failure or slow-success record for a completed request outcome — shared by the
        /// `EventLoopFuture` and async wrappers so both log identically.
        private static func logOutcome<Value>(
            _ result: Result<Value, Swift.Error>,
            startedAt: DispatchTime,
            query: String?,
            consistency: CassandraClient.Consistency?,
            thresholdMillis: UInt32?,
            boundValues: String?,
            logger: Logger
        ) {
            switch result {
            case .success:
                if let query {
                    Self.checkSlowSuccess(
                        startedAt: startedAt,
                        query: query,
                        thresholdMillis: thresholdMillis,
                        boundValues: boundValues,
                        logger: logger
                    )
                }
            case .failure(let error):
                if let cassError = error as? CassandraClient.Error {
                    Self.logFailure(
                        cassError,
                        query: query,
                        consistency: consistency,
                        startedAt: startedAt,
                        boundValues: boundValues,
                        logger: logger
                    )
                }
            }
        }

        /// Attach failure + slow-success logging to a bridged `EventLoopFuture`, returning it unchanged.
        /// The completion closure captures only the Sendable values passed in — never a `Statement`/`Batch`.
        static func instrument<Value>(
            _ future: EventLoopFuture<Value>,
            startedAt: DispatchTime,
            query: String?,
            consistency: CassandraClient.Consistency?,
            thresholdMillis: UInt32?,
            boundValues: String? = nil,
            logger: Logger
        ) -> EventLoopFuture<Value> {
            future.always { result in
                Self.logOutcome(
                    result,
                    startedAt: startedAt,
                    query: query,
                    consistency: consistency,
                    thresholdMillis: thresholdMillis,
                    boundValues: boundValues,
                    logger: logger
                )
            }
        }

        /// Run an async request body with the same failure + slow-success logging.
        @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
        static func instrumented<Value>(
            startedAt: DispatchTime,
            query: String?,
            consistency: CassandraClient.Consistency?,
            thresholdMillis: UInt32?,
            boundValues: String? = nil,
            logger: Logger,
            _ body: () async throws -> Value
        ) async throws -> Value {
            let result: Result<Value, Swift.Error>
            do {
                result = .success(try await body())
            } catch {
                result = .failure(error)
            }
            Self.logOutcome(
                result,
                startedAt: startedAt,
                query: query,
                consistency: consistency,
                thresholdMillis: thresholdMillis,
                boundValues: boundValues,
                logger: logger
            )
            return try result.get()
        }

        /// Format bound parameter values into a single string, each value capped to `maxLoggedValueLength`.
        /// Callers pass this only when `logBoundValues` is set (values are potential PII).
        static func formatValues(_ parameters: [CassandraClient.Statement.Value]) -> String {
            let cap = Configuration.maxLoggedValueLength
            return parameters.map { value in
                let string = "\(value)"
                return string.count > cap ? string.prefix(cap) + "…" : string
            }.joined(separator: ", ")
        }

        private static func truncated(_ query: String) -> String {
            let cap = Configuration.maxLoggedQueryLength
            return query.count > cap ? query.prefix(cap) + "…" : query
        }

        /// Monotonic elapsed milliseconds (uptime clock — unaffected by wall-clock/NTP adjustments).
        private static func elapsedMillis(since start: DispatchTime) -> UInt64 {
            let ns = DispatchTime.now().uptimeNanoseconds &- start.uptimeNanoseconds
            return ns / 1_000_000
        }

        /// Carries only a code label as its string form, so swift-log's `error:` param never serialises
        /// the server-provided message our real `Error.description` embeds (PII).
        private struct RedactedError: Swift.Error, CustomStringConvertible {
            let description: String
        }
    }
}
