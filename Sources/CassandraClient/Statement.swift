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

internal import CDataStaxDriver
import Foundation  // for date and uuid

extension CassandraClient {
    /// A prepared statement to run in a Cassandra database.
    ///
    /// Not `Sendable`: a `Statement` must not be used concurrently, or reused/mutated until a prior
    /// `execute` has completed (the driver reads it asynchronously while the request is in flight).
    public final class Statement: CustomStringConvertible {
        internal let query: String
        internal let parameters: [Value]
        internal let options: Options
        internal let rawPointer: OpaquePointer
        private let encryptor: Encryptor?

        /// Create a new `Statement`.
        public convenience init(query: String, parameters: [Value] = [], options: Options = .init()) throws {
            try self.init(query: query, parameters: parameters, options: options, encryptor: nil)
        }

        /// Internal init that accepts an encryptor injected by Session from Configuration.
        internal init(query: String, parameters: [Value], options: Options, encryptor: Encryptor?) throws {
            self.query = query
            self.parameters = parameters
            self.options = options
            self.encryptor = encryptor
            self.rawPointer = cass_statement_new(query, parameters.count)

            try self.bindParameters()
        }

        /// Internal init for prepared statements. Takes a pre-bound `CassStatement*` from `cass_prepared_bind()`.
        internal init(
            preparedRawPointer: OpaquePointer,
            query: String = "(prepared)",
            parameters: [Value],
            options: Options,
            encryptor: Encryptor?
        ) throws {
            self.query = query
            self.parameters = parameters
            self.options = options
            self.encryptor = encryptor
            self.rawPointer = preparedRawPointer

            try self.bindParameters()
        }

        private func bindParameters() throws {
            for (index, parameter) in parameters.enumerated() {
                let result: CassError
                switch parameter.backing {
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
                case .encryptedString(let wrapped, let context):
                    guard let context = context else {
                        throw CassandraClient.Error.encryptionConfigError(
                            "Encrypted value at index \(index) has no encryption context. Use prepared statement execution with encryptionTable option, or provide context manually."
                        )
                    }
                    result = try self.bindEncrypted(
                        Data(wrapped.value.utf8),
                        context: context,
                        at: index
                    )
                case .encryptedBytes(let wrapped, let context):
                    guard let context = context else {
                        throw CassandraClient.Error.encryptionConfigError(
                            "Encrypted value at index \(index) has no encryption context. Use prepared statement execution with encryptionTable option, or provide context manually."
                        )
                    }
                    result = try self.bindEncrypted(
                        Data(wrapped.value),
                        context: context,
                        at: index
                    )
                case .encryptedInt32(let wrapped, let context):
                    guard let context = context else {
                        throw CassandraClient.Error.encryptionConfigError(
                            "Encrypted value at index \(index) has no encryption context. Use prepared statement execution with encryptionTable option, or provide context manually."
                        )
                    }
                    var bigEndian = wrapped.value.bigEndian
                    result = try self.bindEncrypted(
                        Data(bytes: &bigEndian, count: 4),
                        context: context,
                        at: index
                    )
                case .encryptedInt64(let wrapped, let context):
                    guard let context = context else {
                        throw CassandraClient.Error.encryptionConfigError(
                            "Encrypted value at index \(index) has no encryption context. Use prepared statement execution with encryptionTable option, or provide context manually."
                        )
                    }
                    var bigEndian = wrapped.value.bigEndian
                    result = try self.bindEncrypted(
                        Data(bytes: &bigEndian, count: 8),
                        context: context,
                        at: index
                    )
                case .encryptedDouble(let wrapped, let context):
                    guard let context = context else {
                        throw CassandraClient.Error.encryptionConfigError(
                            "Encrypted value at index \(index) has no encryption context. Use prepared statement execution with encryptionTable option, or provide context manually."
                        )
                    }
                    var bits = wrapped.value.bitPattern.bigEndian
                    result = try self.bindEncrypted(
                        Data(bytes: &bits, count: 8),
                        context: context,
                        at: index
                    )
                case .encryptedUUID(let wrapped, let context):
                    guard let context = context else {
                        throw CassandraClient.Error.encryptionConfigError(
                            "Encrypted value at index \(index) has no encryption context. Use prepared statement execution with encryptionTable option, or provide context manually."
                        )
                    }
                    let u = wrapped.value.uuid
                    let plaintext = Data([
                        u.0, u.1, u.2, u.3, u.4, u.5, u.6, u.7,
                        u.8, u.9, u.10, u.11, u.12, u.13, u.14, u.15,
                    ])
                    result = try self.bindEncrypted(
                        plaintext,
                        context: context,
                        at: index
                    )
                case .encryptedDate(let wrapped, let context):
                    guard let context = context else {
                        throw CassandraClient.Error.encryptionConfigError(
                            "Encrypted value at index \(index) has no encryption context. Use prepared statement execution with encryptionTable option, or provide context manually."
                        )
                    }
                    var bigEndian = Int64(wrapped.value.timeIntervalSince1970 * 1000).bigEndian
                    result = try self.bindEncrypted(
                        Data(bytes: &bigEndian, count: 8),
                        context: context,
                        at: index
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
                case .uuidArray(let array):
                    result = try self.bindArray(array, at: index)
                default:
                    result = try self.bindMapCases(parameter, index)
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
                let milliseconds = try requestTimeout.driverMilliseconds(UInt64.self, name: "requestTimeout")
                try checkResult { cass_statement_set_request_timeout(self.rawPointer, milliseconds) }
            }

            try checkResult {
                cass_statement_set_is_idempotent(self.rawPointer, options.isIdempotent ? cass_true : cass_false)
            }
        }

        /// Encrypt plaintext and bind the result as bytes at the given parameter index.
        private func bindEncrypted(
            _ plaintext: Data,
            context: EncryptionContext,
            at index: Int
        ) throws -> CassError {
            guard let encryptor = self.encryptor else {
                throw CassandraClient.Error.encryptionConfigError(
                    "Encryptor required but not set in Configuration"
                )
            }
            let encrypted = try encryptor.encrypt(plaintext, context: context)
            let this = self
            return encrypted.withUnsafeBytes { buffer in
                let typed = buffer.bindMemory(to: UInt8.self)
                return cass_statement_bind_bytes(this.rawPointer, index, typed.baseAddress, typed.count)
            }
        }

        private func bindArray<T>(_ array: [T], at index: Int) throws -> CassError {
            guard let collection = cass_collection_new(CASS_COLLECTION_TYPE_LIST, array.count) else {
                throw CassandraClient.Error.badParams("Failed to create collection")
            }
            defer { cass_collection_free(collection) }

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
                case let value as Foundation.UUID:
                    let cassUuid = CassUuid(value.uuid)
                    appendResult = cass_collection_append_uuid(collection, cassUuid)
                default:
                    throw CassandraClient.Error.badParams("Array of \(T.self) is not supported")
                }

                guard appendResult == CASS_OK else {
                    throw CassandraClient.Error(appendResult)
                }
            }
            return cass_statement_bind_collection(self.rawPointer, index, collection)
        }

        /// Sets the maximum number of rows the server returns per page for this statement.
        ///
        /// This bounds the `execute` variants that take a `Statement` and no `pageSize`, which
        /// otherwise return every row: the result becomes a single page, and pairing this with
        /// ``setPagingStateToken(_:)`` walks the rest by hand. The `execute` variants that do take a
        /// `pageSize` set it from that argument, overwriting whatever is set here. The `query`
        /// variants build their own statement, so this never reaches them.
        ///
        /// - Parameter pagingSize: Rows per page. Must be in `1...Int32.max`; a non-positive size
        ///   disables paging in the driver rather than limiting the page, and is rejected here.
        public func setPagingSize(_ pagingSize: Int) throws {
            guard let size = Int32(exactly: pagingSize), size > 0 else {
                throw CassandraClient.Error.badParams(
                    "Paging size must be between 1 and \(Int32.max), got \(pagingSize)"
                )
            }
            try checkResult { cass_statement_set_paging_size(self.rawPointer, size) }
        }

        /// Sets the starting page of the returned paginated results.
        ///
        /// The paging state token can be obtained by
        /// ``CassandraClient/Rows/opaquePagingStateToken()``.
        public func setPagingStateToken(_ pagingStateToken: OpaquePagingStateToken) throws {
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
        public struct Value: CustomStringConvertible {
            internal enum Backing {
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

                case encryptedString(Encrypted<String>, context: EncryptionContext?)
                case encryptedBytes(Encrypted<[UInt8]>, context: EncryptionContext?)
                case encryptedInt32(Encrypted<Int32>, context: EncryptionContext?)
                case encryptedInt64(Encrypted<Int64>, context: EncryptionContext?)
                case encryptedDouble(Encrypted<Double>, context: EncryptionContext?)
                case encryptedUUID(Encrypted<Foundation.UUID>, context: EncryptionContext?)
                case encryptedDate(Encrypted<Foundation.Date>, context: EncryptionContext?)

                case int8Array([Int8])
                case int16Array([Int16])
                case int32Array([Int32])
                case int64Array([Int64])
                case float32Array([Float32])
                case doubleArray([Double])
                case stringArray([String])
                case uuidArray([Foundation.UUID])

                case int8Int8Map([Int8: Int8])
                case int8Int16Map([Int8: Int16])
                case int8Int32Map([Int8: Int32])
                case int8Int64Map([Int8: Int64])
                case int8Float32Map([Int8: Float32])
                case int8DoubleMap([Int8: Double])
                case int8BoolMap([Int8: Bool])
                case int8StringMap([Int8: String])
                case int8UUIDMap([Int8: Foundation.UUID])

                case int16Int8Map([Int16: Int8])
                case int16Int16Map([Int16: Int16])
                case int16Int32Map([Int16: Int32])
                case int16Int64Map([Int16: Int64])
                case int16Float32Map([Int16: Float32])
                case int16DoubleMap([Int16: Double])
                case int16BoolMap([Int16: Bool])
                case int16StringMap([Int16: String])
                case int16UUIDMap([Int16: Foundation.UUID])

                case int32Int8Map([Int32: Int8])
                case int32Int16Map([Int32: Int16])
                case int32Int32Map([Int32: Int32])
                case int32Int64Map([Int32: Int64])
                case int32Float32Map([Int32: Float32])
                case int32DoubleMap([Int32: Double])
                case int32BoolMap([Int32: Bool])
                case int32StringMap([Int32: String])
                case int32UUIDMap([Int32: Foundation.UUID])

                case int64Int8Map([Int64: Int8])
                case int64Int16Map([Int64: Int16])
                case int64Int32Map([Int64: Int32])
                case int64Int64Map([Int64: Int64])
                case int64Float32Map([Int64: Float32])
                case int64DoubleMap([Int64: Double])
                case int64BoolMap([Int64: Bool])
                case int64StringMap([Int64: String])
                case int64UUIDMap([Int64: Foundation.UUID])

                case stringInt8Map([String: Int8])
                case stringInt16Map([String: Int16])
                case stringInt32Map([String: Int32])
                case stringInt64Map([String: Int64])
                case stringFloat32Map([String: Float32])
                case stringDoubleMap([String: Double])
                case stringBoolMap([String: Bool])
                case stringStringMap([String: String])
                case stringUUIDMap([String: Foundation.UUID])

                case uuidInt8Map([Foundation.UUID: Int8])
                case uuidInt16Map([Foundation.UUID: Int16])
                case uuidInt32Map([Foundation.UUID: Int32])
                case uuidInt64Map([Foundation.UUID: Int64])
                case uuidFloat32Map([Foundation.UUID: Float32])
                case uuidDoubleMap([Foundation.UUID: Double])
                case uuidBoolMap([Foundation.UUID: Bool])
                case uuidStringMap([Foundation.UUID: String])
                case uuidUUIDMap([Foundation.UUID: Foundation.UUID])

                case timeuuidInt8Map([TimeBasedUUID: Int8])
                case timeuuidInt16Map([TimeBasedUUID: Int16])
                case timeuuidInt32Map([TimeBasedUUID: Int32])
                case timeuuidInt64Map([TimeBasedUUID: Int64])
                case timeuuidFloat32Map([TimeBasedUUID: Float32])
                case timeuuidDoubleMap([TimeBasedUUID: Double])
                case timeuuidBoolMap([TimeBasedUUID: Bool])
                case timeuuidStringMap([TimeBasedUUID: String])
                case timeuuidUUIDMap([TimeBasedUUID: Foundation.UUID])
            }

            internal let backing: Backing

            public var description: String { "\(self.backing)" }

            private init(backing: Backing) {
                self.backing = backing
            }

            public static var null: Value { Value(backing: .null) }
            public static func int8(_ value: Int8) -> Value { Value(backing: .int8(value)) }
            public static func int16(_ value: Int16) -> Value { Value(backing: .int16(value)) }
            public static func int32(_ value: Int32) -> Value { Value(backing: .int32(value)) }
            public static func int64(_ value: Int64) -> Value { Value(backing: .int64(value)) }
            public static func float32(_ value: Float32) -> Value { Value(backing: .float32(value)) }
            public static func double(_ value: Double) -> Value { Value(backing: .double(value)) }
            public static func bool(_ value: Bool) -> Value { Value(backing: .bool(value)) }
            public static func string(_ value: String) -> Value { Value(backing: .string(value)) }
            public static func uuid(_ value: Foundation.UUID) -> Value { Value(backing: .uuid(value)) }
            public static func timeuuid(_ value: TimeBasedUUID) -> Value { Value(backing: .timeuuid(value)) }
            public static func date(_ value: Foundation.Date) -> Value { Value(backing: .date(value)) }
            public static func rawDate(daysSinceEpoch: UInt32) -> Value {
                Value(backing: .rawDate(daysSinceEpoch: daysSinceEpoch))
            }
            public static func rawTimestamp(millisecondsSinceEpoch: Int64) -> Value {
                Value(backing: .rawTimestamp(millisecondsSinceEpoch: millisecondsSinceEpoch))
            }
            public static func bytes(_ value: [UInt8]) -> Value { Value(backing: .bytes(value)) }
            public static func bytesUnsafe(_ value: UnsafeBufferPointer<UInt8>) -> Value {
                Value(backing: .bytesUnsafe(value))
            }

            public static func encryptedString(_ value: Encrypted<String>, context: EncryptionContext? = nil) -> Value {
                Value(backing: .encryptedString(value, context: context))
            }
            public static func encryptedBytes(_ value: Encrypted<[UInt8]>, context: EncryptionContext? = nil) -> Value {
                Value(backing: .encryptedBytes(value, context: context))
            }
            public static func encryptedInt32(_ value: Encrypted<Int32>, context: EncryptionContext? = nil) -> Value {
                Value(backing: .encryptedInt32(value, context: context))
            }
            public static func encryptedInt64(_ value: Encrypted<Int64>, context: EncryptionContext? = nil) -> Value {
                Value(backing: .encryptedInt64(value, context: context))
            }
            public static func encryptedDouble(_ value: Encrypted<Double>, context: EncryptionContext? = nil) -> Value {
                Value(backing: .encryptedDouble(value, context: context))
            }
            public static func encryptedUUID(
                _ value: Encrypted<Foundation.UUID>,
                context: EncryptionContext? = nil
            ) -> Value { Value(backing: .encryptedUUID(value, context: context)) }
            public static func encryptedDate(
                _ value: Encrypted<Foundation.Date>,
                context: EncryptionContext? = nil
            ) -> Value { Value(backing: .encryptedDate(value, context: context)) }

            public static func int8Array(_ value: [Int8]) -> Value { Value(backing: .int8Array(value)) }
            public static func int16Array(_ value: [Int16]) -> Value { Value(backing: .int16Array(value)) }
            public static func int32Array(_ value: [Int32]) -> Value { Value(backing: .int32Array(value)) }
            public static func int64Array(_ value: [Int64]) -> Value { Value(backing: .int64Array(value)) }
            public static func float32Array(_ value: [Float32]) -> Value { Value(backing: .float32Array(value)) }
            public static func doubleArray(_ value: [Double]) -> Value { Value(backing: .doubleArray(value)) }
            public static func stringArray(_ value: [String]) -> Value { Value(backing: .stringArray(value)) }
            public static func uuidArray(_ value: [Foundation.UUID]) -> Value { Value(backing: .uuidArray(value)) }

            public static func int8Int8Map(_ value: [Int8: Int8]) -> Value { Value(backing: .int8Int8Map(value)) }
            public static func int8Int16Map(_ value: [Int8: Int16]) -> Value { Value(backing: .int8Int16Map(value)) }
            public static func int8Int32Map(_ value: [Int8: Int32]) -> Value { Value(backing: .int8Int32Map(value)) }
            public static func int8Int64Map(_ value: [Int8: Int64]) -> Value { Value(backing: .int8Int64Map(value)) }
            public static func int8Float32Map(_ value: [Int8: Float32]) -> Value {
                Value(backing: .int8Float32Map(value))
            }
            public static func int8DoubleMap(_ value: [Int8: Double]) -> Value { Value(backing: .int8DoubleMap(value)) }
            public static func int8BoolMap(_ value: [Int8: Bool]) -> Value { Value(backing: .int8BoolMap(value)) }
            public static func int8StringMap(_ value: [Int8: String]) -> Value { Value(backing: .int8StringMap(value)) }
            public static func int8UUIDMap(_ value: [Int8: Foundation.UUID]) -> Value {
                Value(backing: .int8UUIDMap(value))
            }

            public static func int16Int8Map(_ value: [Int16: Int8]) -> Value { Value(backing: .int16Int8Map(value)) }
            public static func int16Int16Map(_ value: [Int16: Int16]) -> Value { Value(backing: .int16Int16Map(value)) }
            public static func int16Int32Map(_ value: [Int16: Int32]) -> Value { Value(backing: .int16Int32Map(value)) }
            public static func int16Int64Map(_ value: [Int16: Int64]) -> Value { Value(backing: .int16Int64Map(value)) }
            public static func int16Float32Map(_ value: [Int16: Float32]) -> Value {
                Value(backing: .int16Float32Map(value))
            }
            public static func int16DoubleMap(_ value: [Int16: Double]) -> Value {
                Value(backing: .int16DoubleMap(value))
            }
            public static func int16BoolMap(_ value: [Int16: Bool]) -> Value { Value(backing: .int16BoolMap(value)) }
            public static func int16StringMap(_ value: [Int16: String]) -> Value {
                Value(backing: .int16StringMap(value))
            }
            public static func int16UUIDMap(_ value: [Int16: Foundation.UUID]) -> Value {
                Value(backing: .int16UUIDMap(value))
            }

            public static func int32Int8Map(_ value: [Int32: Int8]) -> Value { Value(backing: .int32Int8Map(value)) }
            public static func int32Int16Map(_ value: [Int32: Int16]) -> Value { Value(backing: .int32Int16Map(value)) }
            public static func int32Int32Map(_ value: [Int32: Int32]) -> Value { Value(backing: .int32Int32Map(value)) }
            public static func int32Int64Map(_ value: [Int32: Int64]) -> Value { Value(backing: .int32Int64Map(value)) }
            public static func int32Float32Map(_ value: [Int32: Float32]) -> Value {
                Value(backing: .int32Float32Map(value))
            }
            public static func int32DoubleMap(_ value: [Int32: Double]) -> Value {
                Value(backing: .int32DoubleMap(value))
            }
            public static func int32BoolMap(_ value: [Int32: Bool]) -> Value { Value(backing: .int32BoolMap(value)) }
            public static func int32StringMap(_ value: [Int32: String]) -> Value {
                Value(backing: .int32StringMap(value))
            }
            public static func int32UUIDMap(_ value: [Int32: Foundation.UUID]) -> Value {
                Value(backing: .int32UUIDMap(value))
            }

            public static func int64Int8Map(_ value: [Int64: Int8]) -> Value { Value(backing: .int64Int8Map(value)) }
            public static func int64Int16Map(_ value: [Int64: Int16]) -> Value { Value(backing: .int64Int16Map(value)) }
            public static func int64Int32Map(_ value: [Int64: Int32]) -> Value { Value(backing: .int64Int32Map(value)) }
            public static func int64Int64Map(_ value: [Int64: Int64]) -> Value { Value(backing: .int64Int64Map(value)) }
            public static func int64Float32Map(_ value: [Int64: Float32]) -> Value {
                Value(backing: .int64Float32Map(value))
            }
            public static func int64DoubleMap(_ value: [Int64: Double]) -> Value {
                Value(backing: .int64DoubleMap(value))
            }
            public static func int64BoolMap(_ value: [Int64: Bool]) -> Value { Value(backing: .int64BoolMap(value)) }
            public static func int64StringMap(_ value: [Int64: String]) -> Value {
                Value(backing: .int64StringMap(value))
            }
            public static func int64UUIDMap(_ value: [Int64: Foundation.UUID]) -> Value {
                Value(backing: .int64UUIDMap(value))
            }

            public static func stringInt8Map(_ value: [String: Int8]) -> Value { Value(backing: .stringInt8Map(value)) }
            public static func stringInt16Map(_ value: [String: Int16]) -> Value {
                Value(backing: .stringInt16Map(value))
            }
            public static func stringInt32Map(_ value: [String: Int32]) -> Value {
                Value(backing: .stringInt32Map(value))
            }
            public static func stringInt64Map(_ value: [String: Int64]) -> Value {
                Value(backing: .stringInt64Map(value))
            }
            public static func stringFloat32Map(_ value: [String: Float32]) -> Value {
                Value(backing: .stringFloat32Map(value))
            }
            public static func stringDoubleMap(_ value: [String: Double]) -> Value {
                Value(backing: .stringDoubleMap(value))
            }
            public static func stringBoolMap(_ value: [String: Bool]) -> Value { Value(backing: .stringBoolMap(value)) }
            public static func stringStringMap(_ value: [String: String]) -> Value {
                Value(backing: .stringStringMap(value))
            }
            public static func stringUUIDMap(_ value: [String: Foundation.UUID]) -> Value {
                Value(backing: .stringUUIDMap(value))
            }

            public static func uuidInt8Map(_ value: [Foundation.UUID: Int8]) -> Value {
                Value(backing: .uuidInt8Map(value))
            }
            public static func uuidInt16Map(_ value: [Foundation.UUID: Int16]) -> Value {
                Value(backing: .uuidInt16Map(value))
            }
            public static func uuidInt32Map(_ value: [Foundation.UUID: Int32]) -> Value {
                Value(backing: .uuidInt32Map(value))
            }
            public static func uuidInt64Map(_ value: [Foundation.UUID: Int64]) -> Value {
                Value(backing: .uuidInt64Map(value))
            }
            public static func uuidFloat32Map(_ value: [Foundation.UUID: Float32]) -> Value {
                Value(backing: .uuidFloat32Map(value))
            }
            public static func uuidDoubleMap(_ value: [Foundation.UUID: Double]) -> Value {
                Value(backing: .uuidDoubleMap(value))
            }
            public static func uuidBoolMap(_ value: [Foundation.UUID: Bool]) -> Value {
                Value(backing: .uuidBoolMap(value))
            }
            public static func uuidStringMap(_ value: [Foundation.UUID: String]) -> Value {
                Value(backing: .uuidStringMap(value))
            }
            public static func uuidUUIDMap(_ value: [Foundation.UUID: Foundation.UUID]) -> Value {
                Value(backing: .uuidUUIDMap(value))
            }

            public static func timeuuidInt8Map(_ value: [TimeBasedUUID: Int8]) -> Value {
                Value(backing: .timeuuidInt8Map(value))
            }
            public static func timeuuidInt16Map(_ value: [TimeBasedUUID: Int16]) -> Value {
                Value(backing: .timeuuidInt16Map(value))
            }
            public static func timeuuidInt32Map(_ value: [TimeBasedUUID: Int32]) -> Value {
                Value(backing: .timeuuidInt32Map(value))
            }
            public static func timeuuidInt64Map(_ value: [TimeBasedUUID: Int64]) -> Value {
                Value(backing: .timeuuidInt64Map(value))
            }
            public static func timeuuidFloat32Map(_ value: [TimeBasedUUID: Float32]) -> Value {
                Value(backing: .timeuuidFloat32Map(value))
            }
            public static func timeuuidDoubleMap(_ value: [TimeBasedUUID: Double]) -> Value {
                Value(backing: .timeuuidDoubleMap(value))
            }
            public static func timeuuidBoolMap(_ value: [TimeBasedUUID: Bool]) -> Value {
                Value(backing: .timeuuidBoolMap(value))
            }
            public static func timeuuidStringMap(_ value: [TimeBasedUUID: String]) -> Value {
                Value(backing: .timeuuidStringMap(value))
            }
            public static func timeuuidUUIDMap(_ value: [TimeBasedUUID: Foundation.UUID]) -> Value {
                Value(backing: .timeuuidUUIDMap(value))
            }

            /// Whether this value is an encrypted regular column value (`.encryptedXxx` cases).
            internal var isEncrypted: Bool {
                switch self.backing {
                case .encryptedString, .encryptedBytes, .encryptedInt32,
                    .encryptedInt64, .encryptedDouble, .encryptedUUID,
                    .encryptedDate:
                    return true
                default:
                    return false
                }
            }

            /// The column name from the encryption context, if this is an encrypted value.
            internal var encryptedColumnName: String? {
                switch self.backing {
                case .encryptedString(_, let ctx),
                    .encryptedBytes(_, let ctx),
                    .encryptedInt32(_, let ctx),
                    .encryptedInt64(_, let ctx),
                    .encryptedDouble(_, let ctx),
                    .encryptedUUID(_, let ctx),
                    .encryptedDate(_, let ctx):
                    return ctx?.column
                default:
                    return nil
                }
            }

            /// The encryption context, if this is an encrypted value with an explicit context.
            internal var encryptionContext: CassandraClient.EncryptionContext? {
                switch self.backing {
                case .encryptedString(_, let ctx),
                    .encryptedBytes(_, let ctx),
                    .encryptedInt32(_, let ctx),
                    .encryptedInt64(_, let ctx),
                    .encryptedDouble(_, let ctx),
                    .encryptedUUID(_, let ctx),
                    .encryptedDate(_, let ctx):
                    return ctx
                default:
                    return nil
                }
            }

            /// Returns a copy of this value with the given encryption context applied.
            internal func withContext(_ context: CassandraClient.EncryptionContext) -> Self {
                switch self.backing {
                case .encryptedString(let v, _): return Value(backing: .encryptedString(v, context: context))
                case .encryptedBytes(let v, _): return Value(backing: .encryptedBytes(v, context: context))
                case .encryptedInt32(let v, _): return Value(backing: .encryptedInt32(v, context: context))
                case .encryptedInt64(let v, _): return Value(backing: .encryptedInt64(v, context: context))
                case .encryptedDouble(let v, _): return Value(backing: .encryptedDouble(v, context: context))
                case .encryptedUUID(let v, _): return Value(backing: .encryptedUUID(v, context: context))
                case .encryptedDate(let v, _): return Value(backing: .encryptedDate(v, context: context))
                default: return self
                }
            }
        }

        public struct Options: Sendable, CustomStringConvertible {
            /// Sets the statement's consistency level. `nil` inherits
            /// ``CassandraClient/Configuration/consistency``.
            public var consistency: CassandraClient.Consistency?
            /// Sets the statement's request timeout, rounded up to whole milliseconds. `nil` inherits
            /// ``CassandraClient/Configuration/requestTimeout``.
            public var requestTimeout: Duration?
            /// Whether the statement is safe to run more than once. Default `false`.
            ///
            /// A statement not marked idempotent is treated as unsafe to replay, which holds the driver back
            /// from two recovery behaviors:
            ///
            /// - When a coordinator reports that it is overloaded, shutting down, or hit an internal
            ///   error, the driver hands the request to the next host in its query plan instead of
            ///   failing the call. Without this, a single node draining during a rolling restart
            ///   surfaces as an error even though the rest of the cluster can serve the request.
            /// - Speculative execution, configured by
            ///   ``CassandraClient/Configuration/speculativeExecutionPolicy``, has no effect at all
            ///   until a statement is marked safe to replay.
            ///
            /// - Important: Only mark a statement idempotent when running it twice leaves the same
            ///   result as running it once. Counter updates, lightweight transactions, and appends to
            ///   collections are not idempotent, and replaying one corrupts the value it touches.
            ///   Plain reads, and writes that set fixed values, are.
            ///
            /// - Note: This has no effect on a statement added to a ``CassandraClient/Batch``. The
            ///   driver decides whether to replay a batch from the batch's own setting, so use
            ///   ``CassandraClient/Batch/Configuration/isIdempotent`` for that.
            public var isIdempotent: Bool = false

            /// Closure that extracts encryption context from each row during Codable decoding.
            public var encryptionContextBuilder:
                (
                    @Sendable (CassandraClient.Row) throws -> CassandraClient.EncryptionContext.Base
                )?

            /// Table name for column-registration-based automatic decryption.
            /// Use `"table"` (combined with the session keyspace) or `"keyspace.table"` for cross-keyspace queries.
            /// When set and a matching ``EncryptionSchema`` is registered, the decoder builds
            /// ``EncryptionContext/Base`` automatically. ``encryptionContextBuilder`` takes precedence if both are set.
            ///
            /// - Important: The query must SELECT all primary key columns registered in the schema.
            ///   The decoder reads these columns from each result row to build the ``PrimaryKey`` for key derivation.
            ///   Omitting a key column will cause decryption to fail at runtime.
            ///
            /// - Important: What binding validation enforces depends on the execution path.
            ///   Prepared statement execution maps each parameter to its column via statement metadata
            ///   and checks both directions: a registered column must receive an encrypted value, and an
            ///   unregistered one must not. Other execution paths have no parameter column names, so they
            ///   check only that each encrypted value's ``EncryptionContext`` names a registered column —
            ///   a plaintext value bound to a registered column is written as-is. Neither path verifies
            ///   that a value's context names the column it is actually bound to.
            public var encryptionTable: String?

            /// Whether any encryption options are set (context builder or table name).
            internal var hasEncryptionOptions: Bool {
                self.encryptionContextBuilder != nil || self.encryptionTable != nil
            }

            public init(
                consistency: CassandraClient.Consistency? = nil,
                requestTimeout: Duration? = nil,
                isIdempotent: Bool = false
            ) {
                self.consistency = consistency
                self.requestTimeout = requestTimeout
                self.isIdempotent = isIdempotent
            }

            public var description: String {
                """
                Options {
                consistency: \(String(describing: self.consistency)),
                requestTimeout: \(String(describing: self.requestTimeout)),
                isIdempotent: \(String(describing: self.isIdempotent))
                }
                """
            }
        }
    }
}

// `Statement` is intentionally not `Sendable`; see the type's documentation.
@available(*, unavailable)
extension CassandraClient.Statement: Sendable {}

private func checkResult(body: () -> CassError) throws {
    let result = body()
    guard result == CASS_OK else {
        throw CassandraClient.Error(result, message: "Failed to configure Statement")
    }
}
