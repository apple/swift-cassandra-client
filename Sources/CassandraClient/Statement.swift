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
        private let _encryptor: AnyObject?

        @available(macOS 15.0, iOS 18.0, visionOS 2.0, *)
        private var encryptor: Encryptor? { self._encryptor as? Encryptor }

        /// Create a new `Statement`.
        public convenience init(query: String, parameters: [Value] = [], options: Options = .init()) throws {
            try self.init(query: query, parameters: parameters, options: options, _encryptor: nil)
        }

        /// Internal init that accepts an encryptor injected by Session from Configuration.
        internal init(query: String, parameters: [Value], options: Options, _encryptor: AnyObject?) throws {
            self.query = query
            self.parameters = parameters
            self.options = options
            self._encryptor = _encryptor
            self.rawPointer = cass_statement_new(query, parameters.count)

            try self.bindParameters()
        }

        private func bindParameters() throws {
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
                case .encryptedString(let wrapped, let context):
                    result = try self.bindEncrypted(
                        Data(wrapped.value.utf8),
                        context: context,
                        at: index
                    )
                case .encryptedBytes(let wrapped, let context):
                    result = try self.bindEncrypted(
                        Data(wrapped.value),
                        context: context,
                        at: index
                    )
                case .encryptedInt32(let wrapped, let context):
                    var bigEndian = wrapped.value.bigEndian
                    result = try self.bindEncrypted(
                        Data(bytes: &bigEndian, count: 4),
                        context: context,
                        at: index
                    )
                case .encryptedInt64(let wrapped, let context):
                    var bigEndian = wrapped.value.bigEndian
                    result = try self.bindEncrypted(
                        Data(bytes: &bigEndian, count: 8),
                        context: context,
                        at: index
                    )
                case .encryptedDouble(let wrapped, let context):
                    var bits = wrapped.value.bitPattern.bigEndian
                    result = try self.bindEncrypted(
                        Data(bytes: &bits, count: 8),
                        context: context,
                        at: index
                    )
                case .encryptedUUID(let wrapped, let context):
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
                try checkResult { cass_statement_set_request_timeout(self.rawPointer, requestTimeout) }
            }
        }

        /// Encrypt plaintext and bind the result as bytes at the given parameter index.
        private func bindEncrypted(
            _ plaintext: Data,
            context: EncryptionContext,
            at index: Int
        ) throws -> CassError {
            guard #available(macOS 15.0, iOS 18.0, visionOS 2.0, *) else {
                throw CassandraClient.Error.encryptionError("Encryption requires macOS 15.0+")
            }
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

            case encryptedString(Encrypted<String>, context: EncryptionContext)
            case encryptedBytes(Encrypted<[UInt8]>, context: EncryptionContext)
            case encryptedInt32(Encrypted<Int32>, context: EncryptionContext)
            case encryptedInt64(Encrypted<Int64>, context: EncryptionContext)
            case encryptedDouble(Encrypted<Double>, context: EncryptionContext)
            case encryptedUUID(Encrypted<Foundation.UUID>, context: EncryptionContext)
            case encryptedDate(Encrypted<Foundation.Date>, context: EncryptionContext)

            case int8Array([Int8])
            case int16Array([Int16])
            case int32Array([Int32])
            case int64Array([Int64])
            case float32Array([Float32])
            case doubleArray([Double])
            case stringArray([String])

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

            /// Whether this value is an encrypted regular column value (`.encryptedXxx` cases).
            internal var isEncrypted: Bool {
                switch self {
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
                switch self {
                case .encryptedString(_, let ctx),
                    .encryptedBytes(_, let ctx),
                    .encryptedInt32(_, let ctx),
                    .encryptedInt64(_, let ctx),
                    .encryptedDouble(_, let ctx),
                    .encryptedUUID(_, let ctx),
                    .encryptedDate(_, let ctx):
                    return ctx.column
                default:
                    return nil
                }
            }
        }

        public struct Options: CustomStringConvertible {
            /// Sets the statement's consistency level. Default is `.localOne`.
            public var consistency: CassandraClient.Consistency?
            /// Sets the statement's request timeout in milliseconds. Default is `CASS_UINT64_MAX`
            public var requestTimeout: UInt64?

            /// Type-erased backing store for ``encryptionContextBuilder``.
            private var _encryptionContextBuilder: Any?

            /// Closure that extracts encryption context from each row during Codable decoding.
            @available(macOS 15.0, iOS 18.0, visionOS 2.0, *)
            public var encryptionContextBuilder:
                (
                    (CassandraClient.Row) throws -> CassandraClient.EncryptionContext.Base
                )?
            {
                get {
                    self._encryptionContextBuilder
                        as? (CassandraClient.Row) throws -> CassandraClient.EncryptionContext.Base
                }
                set { self._encryptionContextBuilder = newValue }
            }

            /// Backing store for ``encryptionTable``.
            private var _encryptionTable: String?

            /// Table name for column-registration-based automatic decryption.
            /// Use `"table"` (combined with the session keyspace) or `"keyspace.table"` for cross-keyspace queries.
            /// When set and a matching ``EncryptionSchema`` is registered, the decoder builds
            /// ``EncryptionContext/Base`` automatically. ``encryptionContextBuilder`` takes precedence if both are set.
            @available(macOS 15.0, iOS 18.0, visionOS 2.0, *)
            public var encryptionTable: String? {
                get { self._encryptionTable }
                set { self._encryptionTable = newValue }
            }

            /// Whether any encryption options are set (context builder or table name).
            internal var hasEncryptionOptions: Bool {
                self._encryptionContextBuilder != nil || self._encryptionTable != nil
            }

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
