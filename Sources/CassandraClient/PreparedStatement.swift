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

@_implementationOnly import CDataStaxDriver

extension CassandraClient {
    /// A server-side prepared statement that can be efficiently executed multiple times with different parameters.
    public final class PreparedStatement: @unchecked Sendable {
        internal let rawPointer: OpaquePointer
        private let _parameterCount: Int

        internal init(rawPointer: OpaquePointer) {
            self.rawPointer = rawPointer
            // Count parameters by iterating until we get an error from parameterName.
            var count = 0
            var namePtr: UnsafePointer<CChar>?
            var nameLength = Int()
            while cass_prepared_parameter_name(rawPointer, count, &namePtr, &nameLength) == CASS_OK {
                count += 1
            }
            self._parameterCount = count
        }

        deinit {
            cass_prepared_free(self.rawPointer)
        }

        /// The number of bind parameters in this prepared statement.
        public var parameterCount: Int {
            self._parameterCount
        }

        /// Gets the column name for the parameter at the given index.
        ///
        /// - Parameter index: Zero-based parameter index.
        /// - Returns: The column name, or `nil` if the index is out of bounds.
        public func parameterName(at index: Int) -> String? {
            var namePtr: UnsafePointer<CChar>?
            var nameLength = Int()
            let result = cass_prepared_parameter_name(self.rawPointer, index, &namePtr, &nameLength)
            guard result == CASS_OK, let namePtr = namePtr else {
                return nil
            }
            let buffer = UnsafeBufferPointer(start: namePtr, count: nameLength)
            return buffer.withMemoryRebound(to: UInt8.self) {
                String(decoding: $0, as: UTF8.self)
            }
        }

        /// Creates a bound statement from this prepared statement, ready for parameter binding and execution.
        ///
        /// The returned `OpaquePointer` is a `CassStatement*` that must be freed with `cass_statement_free`.
        internal func bind() -> OpaquePointer {
            cass_prepared_bind(self.rawPointer)
        }
    }
}
