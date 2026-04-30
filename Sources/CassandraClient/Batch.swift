//===----------------------------------------------------------------------===//
//
// This source file is part of the Swift Cassandra Client open source project
//
// Copyright (c) 2022-2026 Apple Inc. and the Swift Cassandra Client project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of Swift Cassandra Client project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

@_implementationOnly import CDataStaxDriver
import Foundation

extension CassandraClient {
    /// The type of a batch operation.
    public struct BatchType: Equatable {
        let rawValue: CassBatchType

        /// All statements are applied atomically with a write to the batch log.
        public static let logged = BatchType(rawValue: CASS_BATCH_TYPE_LOGGED)
        /// Statements are applied without atomicity guarantees.
        public static let unlogged = BatchType(rawValue: CASS_BATCH_TYPE_UNLOGGED)
        /// All statements must be counter updates.
        public static let counter = BatchType(rawValue: CASS_BATCH_TYPE_COUNTER)
    }

    /// A batch of statements to execute in Cassandra.
    public struct Batch: ~Copyable {
        internal let rawPointer: OpaquePointer

        internal init(configuration: Configuration) throws {
            self.rawPointer = cass_batch_new(configuration.type.rawValue)

            if let consistency = configuration.consistency {
                try checkResult {
                    cass_batch_set_consistency(self.rawPointer, consistency.cassConsistency)
                }
            }
            if let serialConsistency = configuration.serialConsistency {
                try checkResult {
                    cass_batch_set_serial_consistency(self.rawPointer, serialConsistency.cassConsistency)
                }
            }
            if let timestamp = configuration.timestamp {
                let microseconds = Int64(timestamp.timeIntervalSince1970 * 1_000_000)
                try checkResult {
                    cass_batch_set_timestamp(self.rawPointer, cass_int64_t(microseconds))
                }
            }
            if let requestTimeout = configuration.requestTimeout {
                try checkResult {
                    cass_batch_set_request_timeout(self.rawPointer, requestTimeout)
                }
            }
            if let isIdempotent = configuration.isIdempotent {
                try checkResult {
                    cass_batch_set_is_idempotent(self.rawPointer, isIdempotent ? cass_true : cass_false)
                }
            }
            if let tracing = configuration.tracing {
                try checkResult {
                    cass_batch_set_tracing(self.rawPointer, tracing ? cass_true : cass_false)
                }
            }
            if let keyspace = configuration.keyspace {
                try checkResult {
                    cass_batch_set_keyspace_n(self.rawPointer, keyspace, keyspace.utf8.count)
                }
            }
        }

        deinit {
            cass_batch_free(self.rawPointer)
        }

        /// Add a statement to this batch.
        public mutating func add(statement: Statement) throws {
            try checkResult {
                cass_batch_add_statement(self.rawPointer, statement.rawPointer)
            }
        }

        /// Batch configuration options.
        public struct Configuration {
            /// The batch type. Defaults to `.logged`.
            public var type: CassandraClient.BatchType = .logged
            /// The batch's consistency level.
            public var consistency: CassandraClient.Consistency?
            /// The batch's serial consistency level for conditional updates.
            public var serialConsistency: CassandraClient.SerialConsistency?
            /// The batch's write timestamp.
            public var timestamp: Foundation.Date?
            /// The batch's request timeout in milliseconds.
            public var requestTimeout: UInt64?
            /// Whether the batch is idempotent.
            public var isIdempotent: Bool?
            /// Whether tracing is enabled for this batch.
            public var tracing: Bool?
            /// The keyspace for the batch.
            public var keyspace: String?

            public init() {}
        }
    }
}

private func checkResult(body: () -> CassError) throws {
    let result = body()
    guard result == CASS_OK else {
        throw CassandraClient.Error(result, message: "Failed to configure Batch")
    }
}
