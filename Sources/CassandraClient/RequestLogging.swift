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
        /// The log level for a failure category (a mapping *from* the category, kept separate from the
        /// categoriser so the category stays reusable by metrics).
        static func level(for category: ErrorCategory) -> Logger.Level {
            switch category {
            case .transient: return .debug
            case .unavailable, .callerFault, .server: return .warning
            case .wrapperInternal: return .error
            }
        }

        /// Log a failed request once, at the level for its category, with best-effort metadata.
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
            logger.log(level: Self.level(for: category), "\(error.shortDescription)", metadata: metadata)
        }

        /// If a successful request's elapsed time exceeds the threshold, log a `.info` "slow query" record.
        /// `thresholdMillis == nil` skips all timing work (hot-path guard).
        static func checkSlowSuccess(
            startedAt: DispatchTime,
            query: String,
            thresholdMillis: UInt32?,
            boundValues: String? = nil,
            logger: Logger
        ) {
            guard let thresholdMillis else { return }
            let elapsed = Self.elapsedMillis(since: startedAt)
            guard elapsed > thresholdMillis else { return }
            var metadata: Logger.Metadata = [
                LogKey.query: "\(Self.truncated(query))",
                LogKey.latencyMs: "\(elapsed)",
            ]
            if let boundValues {
                metadata[LogKey.boundValues] = "\(boundValues)"
            }
            logger.info("slow query", metadata: metadata)
        }

        /// Attach failure + slow-success logging to a bridged `EventLoopFuture`, returning it unchanged.
        /// The completion closure captures only the Sendable values passed in — never a `Statement`/`Batch`.
        static func instrument<Value>(
            _ future: EventLoopFuture<Value>,
            enabled: Bool,
            startedAt: DispatchTime,
            query: String?,
            consistency: CassandraClient.Consistency?,
            thresholdMillis: UInt32?,
            boundValues: String? = nil,
            logger: Logger
        ) -> EventLoopFuture<Value> {
            guard enabled else { return future }
            return future.always { result in
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
        }

        /// Run an async request body with the same failure + slow-success logging.
        @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
        static func instrumented<Value>(
            enabled: Bool,
            startedAt: DispatchTime,
            query: String?,
            consistency: CassandraClient.Consistency?,
            thresholdMillis: UInt32?,
            boundValues: String? = nil,
            logger: Logger,
            _ body: () async throws -> Value
        ) async throws -> Value {
            guard enabled else { return try await body() }
            do {
                let value = try await body()
                if let query {
                    Self.checkSlowSuccess(
                        startedAt: startedAt,
                        query: query,
                        thresholdMillis: thresholdMillis,
                        boundValues: boundValues,
                        logger: logger
                    )
                }
                return value
            } catch {
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
                throw error
            }
        }

        /// Format bound parameter values into a single string, each value capped to `maxLoggedValueLength`.
        /// Callers pass this only when `logBoundValues` is set (values are potential PII).
        static func formatValues(_ parameters: [CassandraClient.Statement.Value]) -> String {
            let cap = Configuration.maxLoggedValueLength
            return parameters.map { value in
                let string = "\(value)"
                return string.count > cap ? String(string.prefix(cap)) + "…" : string
            }.joined(separator: ", ")
        }

        private static func truncated(_ query: String) -> String {
            let cap = Configuration.maxLoggedQueryLength
            return query.count > cap ? String(query.prefix(cap)) + "…" : query
        }

        /// Monotonic elapsed milliseconds (uptime clock — unaffected by wall-clock/NTP adjustments).
        private static func elapsedMillis(since start: DispatchTime) -> UInt64 {
            let ns = DispatchTime.now().uptimeNanoseconds &- start.uptimeNanoseconds
            return ns / 1_000_000
        }
    }
}
