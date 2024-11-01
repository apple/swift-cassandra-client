//===----------------------------------------------------------------------===//
//
// This source file is part of the Swift Cassandra Client open source project
//
// Copyright (c) 2022-2023 Apple Inc. and the Swift Cassandra Client project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of Swift Cassandra Client project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

@_implementationOnly import CDataStaxDriver
import Foundation  // for date and uuid

extension CassandraClient {
    /// A prepared statement to run in a Cassandra database.
    public final class Statement: CustomStringConvertible {
        internal let query: String
        internal let parameters: [Value]
        internal let options: Options
        internal let rawPointer: OpaquePointer

        /// Create a new `Statement`.
        public init(query: String, parameters: [Value] = [], options: Options = .init()) throws {
            self.query = query
            self.parameters = parameters
            self.options = options
            self.rawPointer = cass_statement_new(query, parameters.count)

            for (index, parameter) in parameters.enumerated() {
                let result: CassError
                switch parameter {
                case .null:
                    result = cass_statement_bind_null(self.rawPointer, index)
                case .int8(let value):
                    result = cass_statement_bind_int8(self.rawPointer, index, value)
                case .int16(let value):
                    result = cass_statement_bind_int16(self.rawPointer, index, value)
                case .int32(let value):
                    result = cass_statement_bind_int32(self.rawPointer, index, value)
                case .int64(let value):
                    result = cass_statement_bind_int64(self.rawPointer, index, cass_int64_t(value))
                case .float32(let value):
                    result = cass_statement_bind_float(self.rawPointer, index, value)
                case .double(let value):
                    result = cass_statement_bind_double(self.rawPointer, index, value)
                case .bool(let value):
                    result = cass_statement_bind_bool(
                        self.rawPointer,
                        index,
                        value ? cass_bool_t(1) : cass_bool_t(0)
                    )
                case .string(let value):
                    result = cass_statement_bind_string(self.rawPointer, index, value)
                case .uuid(let value):
                    let uuid = CassUuid(value.uuid)
                    result = cass_statement_bind_uuid(self.rawPointer, index, uuid)
                case .timeuuid(let value):
                    let timeuuid = CassUuid(value.uuid)
                    result = cass_statement_bind_uuid(self.rawPointer, index, timeuuid)
                case .date(let value):
                    let timestamp = Int64(value.timeIntervalSince1970 * 1000)
                    result = cass_statement_bind_int64(self.rawPointer, index, cass_int64_t(timestamp))
                case .rawDate(let value):
                    result = cass_statement_bind_uint32(self.rawPointer, index, value)
                case .rawTimestamp(let value):
                    result = cass_statement_bind_int64(self.rawPointer, index, cass_int64_t(value))
                case .bytes(let value):
                    let this = self
                    result = value.withUnsafeBufferPointer { buffer in
                        cass_statement_bind_bytes(this.rawPointer, index, buffer.baseAddress, buffer.count)
                    }
                case .bytesUnsafe(let buffer):
                    result = cass_statement_bind_bytes(
                        self.rawPointer,
                        index,
                        buffer.baseAddress,
                        buffer.count
                    )
                case .int8Array(let array):
                    result = try self.bindArray(array, at: index)
                case .int16Array(let array):
                    result = try self.bindArray(array, at: index)
                case .int32Array(let array):
                    result = try self.bindArray(array, at: index)
                case .int64Array(let array):
                    result = try self.bindArray(array, at: index)
                case .float32Array(let array):
                    result = try self.bindArray(array, at: index)
                case .doubleArray(let array):
                    result = try self.bindArray(array, at: index)
                case .stringArray(let array):
                    result = try self.bindArray(array, at: index)
                }

                guard result == CASS_OK else {
                    throw CassandraClient.Error(result)
                }
            }

            if let consistency = options.consistency {
                try checkResult {
                    cass_statement_set_consistency(self.rawPointer, consistency.cassConsistency)
                }
            }

            if let requestTimeout = options.requestTimeout {
                try checkResult { cass_statement_set_request_timeout(self.rawPointer, requestTimeout) }
            }
        }

        private func bindArray<T>(_ array: [T], at index: Int) throws -> CassError {
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_LIST, array.count)
            for element in array {
                let appendResult: CassError
                switch element {
                case let value as Int8:
                    appendResult = cass_collection_append_int8(collection, value)
                case let value as Int16:
                    appendResult = cass_collection_append_int16(collection, value)
                case let value as Int32:
                    appendResult = cass_collection_append_int32(collection, value)
                case let value as Int64:
                    appendResult = cass_collection_append_int64(collection, value)
                case let value as Float32:
                    appendResult = cass_collection_append_float(collection, value)
                case let value as Double:
                    appendResult = cass_collection_append_double(collection, value)
                case let value as String:
                    appendResult = cass_collection_append_string(collection, value)
                default:
                    throw CassandraClient.Error.badParams("Array of \(T.self) is not supported")
                }

                guard appendResult == CASS_OK else {
                    throw CassandraClient.Error(appendResult)
                }
            }
            return cass_statement_bind_collection(self.rawPointer, index, collection)
        }

        func setPagingSize(_ pagingSize: Int32) throws {
            try checkResult { cass_statement_set_paging_size(self.rawPointer, pagingSize) }
        }

        /// Sets the starting page of the returned paginated results.
        ///
        /// The paging state token can be obtained by the `pagingStateToken()`
        /// function on `Rows`.
        ///
        /// - Warning: The paging state should not be exposed to or come from
        /// untrusted environments. The paging state could be spoofed and
        /// potentially used to gain access to other data.
        public func setPagingStateToken(_ pagingStateToken: PagingStateToken) throws {
            try checkResult {
                pagingStateToken.withUnsafeBytes {
                    let buffer = $0.bindMemory(to: CChar.self)
                    return cass_statement_set_paging_state_token(
                        self.rawPointer,
                        buffer.baseAddress,
                        buffer.count
                    )
                }
            }
        }

        deinit {
            cass_statement_free(self.rawPointer)
        }

        public var description: String {
            "\(self.query) \(self.parameters)"
        }

        /// Value types
        public enum Value {
            case null
            case int8(Int8)
            case int16(Int16)
            case int32(Int32)
            case int64(Int64)
            case float32(Float32)
            case double(Double)
            case bool(Bool)
            case string(String)
            case uuid(Foundation.UUID)
            case timeuuid(TimeBasedUUID)
            case date(Foundation.Date)
            case rawDate(daysSinceEpoch: UInt32)
            case rawTimestamp(millisecondsSinceEpoch: Int64)
            case bytes([UInt8])
            case bytesUnsafe(UnsafeBufferPointer<UInt8>)

            case int8Array([Int8])
            case int16Array([Int16])
            case int32Array([Int32])
            case int64Array([Int64])
            case float32Array([Float32])
            case doubleArray([Double])
            case stringArray([String])
        }

        public struct Options: CustomStringConvertible {
            /// Sets the statement's consistency level. Default is `.localOne`.
            public var consistency: CassandraClient.Consistency?
            /// Sets the statement's request timeout in milliseconds. Default is `CASS_UINT64_MAX`
            public var requestTimeout: UInt64?

            public init(consistency: CassandraClient.Consistency? = nil, requestTimeout: UInt64? = nil) {
                self.consistency = consistency
                self.requestTimeout = requestTimeout
            }

            public var description: String {
                """
                Options {
                consistency: \(String(describing: self.consistency)),
                requestTimeout: \(String(describing: self.requestTimeout))
                }
                """
            }
        }
    }
}

private func checkResult(body: () -> CassError) throws {
    let result = body()
    guard result == CASS_OK else {
        throw CassandraClient.Error(result, message: "Failed to configure Statement")
    }
}
