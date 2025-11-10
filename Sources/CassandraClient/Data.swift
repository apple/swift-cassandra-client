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
import Foundation
import Logging
import NIO

public protocol PagingStateToken: ContiguousBytes {}

extension CassandraClient {
    /// Resulting row(s) of a Cassandra query. Data are returned all at once.
    public final class Rows: Sequence {
        internal let rawPointer: OpaquePointer

        internal init(_ resultFutureRawPointer: OpaquePointer) {
            self.rawPointer = cass_future_get_result(resultFutureRawPointer)
        }

        deinit {
            cass_result_free(self.rawPointer)
        }

        public var first: Row? {
            self.makeIterator().next()
        }

        public var count: Int {
            cass_result_row_count(self.rawPointer)
        }

        public var columnsCount: Int {
            cass_result_column_count(self.rawPointer)
        }

        /// Get column name by index
        /// - Parameter index: The column index (0-based)
        /// - Returns: The column name
        /// - Throws: CassandraClient.Error if index is out of bounds or column name cannot be retrieved
        public func columnName(at index: Int) throws -> String {
            guard index >= 0 && index < self.columnsCount else {
                throw CassandraClient.Error(CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS)
            }

            var namePtr: UnsafePointer<CChar>?
            var nameLength: Int = 0

            let result = cass_result_column_name(self.rawPointer, index, &namePtr, &nameLength)
            guard result == CASS_OK, let name = namePtr else {
                throw CassandraClient.Error(result)
            }

            return String(cString: name)
        }

        /// Get all column names
        /// - Returns: Array of column names
        /// - Throws: CassandraClient.Error if any column name cannot be retrieved
        public func columnNames() throws -> [String] {
            try (0..<self.columnsCount).map { try columnName(at: $0) }
        }

        public func makeIterator() -> Iterator {
            Iterator(rows: self)
        }

        /// Returns a reusable paging token.
        ///
        /// - Warning: This token is not suitable or safe for sharing externally.
        public func opaquePagingStateToken() throws -> OpaquePagingStateToken {
            try OpaquePagingStateToken(token: self.rawPagingStateToken())
        }

        private func rawPagingStateToken() throws -> [UInt8] {
            var buffer: UnsafePointer<CChar>?
            var length = 0

            // The underlying memory is freed with the Rows result
            let result = cass_result_paging_state_token(self.rawPointer, &buffer, &length)
            guard result == CASS_OK, let bytesPointer = buffer else {
                throw CassandraClient.Error(result)
            }

            let tokenBytes: [UInt8] = bytesPointer.withMemoryRebound(to: UInt8.self, capacity: length) {
                let bufferPointer = UnsafeBufferPointer(start: $0, count: length)
                return Array(unsafeUninitializedCapacity: length) { storagePointer, storageCount in
                    var (unwritten, endIndex) = storagePointer.initialize(from: bufferPointer)
                    precondition(unwritten.next() == nil)
                    storageCount = storagePointer.distance(from: storagePointer.startIndex, to: endIndex)
                }
            }

            return tokenBytes
        }

        public final class Iterator: IteratorProtocol {
            public typealias Element = Row

            internal let rawPointer: OpaquePointer
            // Used to make sure the result isn't freed while a reference to one
            // of its rows still exists
            private let parent: Rows

            internal init(rows: Rows) {
                self.rawPointer = cass_iterator_from_result(rows.rawPointer)
                self.parent = rows
            }

            deinit {
                cass_iterator_free(self.rawPointer)
            }

            public func next() -> Row? {
                guard cass_iterator_next(self.rawPointer) == cass_true else {
                    return nil
                }

                return Row(iterator: self)
            }
        }
    }

    /// A resulting row of a Cassandra query.
    public struct Row {
        internal let rawPointer: OpaquePointer
        // Used to make sure the iterator isn't freed while a reference to one
        // of its rows still exists
        private let parent: Rows.Iterator

        internal init(iterator: Rows.Iterator) {
            self.rawPointer = cass_iterator_get_row(iterator.rawPointer)
            self.parent = iterator
        }

        /// Access column with the given `index`.
        public func column(_ index: Int) -> Column? {
            Column(row: self, index: index)
        }

        /// Access column with the given `name`.
        public func column(_ name: String) -> Column? {
            Column(row: self, name: name)
        }
    }

    /// A column in a resulting ``Row`` of a Cassandra query.
    ///
    /// Note that the value is only good as long as the iterator it came from hasn't been advanced.
    public struct Column {
        let rawPointer: OpaquePointer
        // Used to make sure the row isn't freed while a reference to one
        // of its columns still exists
        private let parent: Row

        internal init?(row: Row, index: Int) {
            guard let rawPointer = cass_row_get_column(row.rawPointer, index) else {
                return nil
            }
            self.rawPointer = rawPointer
            self.parent = row
        }

        internal init?(row: Row, name: String) {
            guard let rawPointer = cass_row_get_column_by_name(row.rawPointer, name) else {
                return nil
            }
            self.rawPointer = rawPointer
            self.parent = row
        }

        func isNull() -> Bool {
            cass_value_is_null(self.rawPointer) == cass_true
        }
    }

    /// A reusable page token that can be used by `Statement` to resume querying
    /// at a specific position.
    public struct OpaquePagingStateToken: PagingStateToken {
        let token: [UInt8]

        public func withUnsafeBytes<R>(_ body: (UnsafeRawBufferPointer) throws -> R) rethrows -> R {
            try self.token.withUnsafeBytes(body)
        }
    }
}

// MARK: - Utils

private func toString(cassValue: OpaquePointer) -> String? {
    var value: UnsafePointer<CChar>?
    var valueSize = 0
    let error = cass_value_get_string(cassValue, &value, &valueSize)
    guard let definiteValue = value, error == CASS_OK else {
        return nil
    }
    let stringBuffer = UnsafeBufferPointer(start: definiteValue, count: valueSize)
    return stringBuffer.withMemoryRebound(to: UInt8.self) {
        String(decoding: $0, as: UTF8.self)
    }
}

// MARK: - Int8

extension CassandraClient.Column {
    /// Get column value as `Int8`.
    public var int8: Int8? {
        var value: Int8 = 0
        let error = cass_value_get_int8(self.rawPointer, &value)
        return error == CASS_OK ? value : nil
    }
}

extension CassandraClient.Row {
    /// Get column value as `Int8`.
    public func column(_ name: String) -> Int8? {
        self.column(name)?.int8
    }

    /// Get column value as `Int8`.
    public func column(_ index: Int) -> Int8? {
        self.column(index)?.int8
    }
}

// MARK: - Int16

extension CassandraClient.Column {
    /// Get column value as `Int16`.
    public var int16: Int16? {
        var value: Int16 = 0
        let error = cass_value_get_int16(self.rawPointer, &value)
        return error == CASS_OK ? value : nil
    }
}

extension CassandraClient.Row {
    /// Get column value as `Int16`.
    public func column(_ name: String) -> Int16? {
        self.column(name)?.int16
    }

    /// Get column value as `Int16`.
    public func column(_ index: Int) -> Int16? {
        self.column(index)?.int16
    }
}

// MARK: - Int32

extension CassandraClient.Column {
    /// Get column value as `Int32`.
    public var int32: Int32? {
        var value: Int32 = 0
        let error = cass_value_get_int32(self.rawPointer, &value)
        return error == CASS_OK ? value : nil
    }
}

extension CassandraClient.Row {
    /// Get column value as `Int32`.
    public func column(_ name: String) -> Int32? {
        self.column(name)?.int32
    }

    /// Get column value as `Int32`.
    public func column(_ index: Int) -> Int32? {
        self.column(index)?.int32
    }
}

// MARK: - UInt32

extension CassandraClient.Column {
    /// Get column value as `UInt32`.
    public var uint32: UInt32? {
        var value: UInt32 = 0
        let error = cass_value_get_uint32(self.rawPointer, &value)
        return error == CASS_OK ? value : nil
    }
}

extension CassandraClient.Row {
    /// Get column value as `UInt32`.
    public func column(_ name: String) -> UInt32? {
        self.column(name)?.uint32
    }

    /// Get column value as `UInt32`.
    public func column(_ index: Int) -> UInt32? {
        self.column(index)?.uint32
    }
}

// MARK: - Int64

extension CassandraClient.Column {
    /// Get column value as `Int64`.
    public var int64: Int64? {
        var value: cass_int64_t = 0
        let error = cass_value_get_int64(self.rawPointer, &value)
        return error == CASS_OK ? Int64(value) : nil
    }
}

extension CassandraClient.Row {
    /// Get column value as `Int64`.
    public func column(_ name: String) -> Int64? {
        self.column(name)?.int64
    }

    /// Get column value as `Int64`.
    public func column(_ index: Int) -> Int64? {
        self.column(index)?.int64
    }
}

// MARK: - Float32

extension CassandraClient.Column {
    /// Get column value as `Float32`.
    public var float32: Float32? {
        var value: Float32 = 0
        let error = cass_value_get_float(self.rawPointer, &value)
        return error == CASS_OK ? value : nil
    }
}

extension CassandraClient.Row {
    /// Get column value as `Float32`.
    public func column(_ name: String) -> Float32? {
        self.column(name)?.float32
    }

    /// Get column value as `Float32`.
    public func column(_ index: Int) -> Float32? {
        self.column(index)?.float32
    }
}

// MARK: - Double

extension CassandraClient.Column {
    /// Get column value as `Double`.
    public var double: Double? {
        var value: Double = 0
        let error = cass_value_get_double(self.rawPointer, &value)
        return error == CASS_OK ? value : nil
    }
}

extension CassandraClient.Row {
    /// Get column value as `Double`.
    public func column(_ name: String) -> Double? {
        self.column(name)?.double
    }

    /// Get column value as `Double`.
    public func column(_ index: Int) -> Double? {
        self.column(index)?.double
    }
}

// MARK: - Bool

extension CassandraClient.Column {
    /// Get column value as `Bool`.
    public var bool: Bool? {
        var value = cass_bool_t(0)
        let error = cass_value_get_bool(self.rawPointer, &value)
        return error == CASS_OK ? value == cass_true : nil
    }
}

extension CassandraClient.Row {
    /// Get column value as `Bool`.
    public func column(_ name: String) -> Bool? {
        self.column(name)?.bool
    }

    /// Get column value as `Bool`.
    public func column(_ index: Int) -> Bool? {
        self.column(index)?.bool
    }
}

// MARK: - String

extension CassandraClient.Column {
    /// Get column value as `String`.
    public var string: String? {
        toString(cassValue: self.rawPointer)
    }
}

extension CassandraClient.Row {
    /// Get column value as `String`.
    public func column(_ name: String) -> String? {
        self.column(name)?.string
    }

    /// Get column value as `String`.
    public func column(_ index: Int) -> String? {
        self.column(index)?.string
    }
}

// MARK: - UUID

/// Time-based UUID (version 1).
public struct TimeBasedUUID: Codable, Hashable, Equatable, CustomStringConvertible {
    private let underlying: Foundation.UUID

    internal var uuid: uuid_t {
        self.underlying.uuid
    }

    public var uuidString: String {
        self.underlying.uuidString
    }

    public init() {
        self.underlying = UUIDGenerator.instance.generateTimeBased()
    }

    internal init(uuid: CassUuid) {
        self.underlying = uuid.uuid()
    }

    public var description: String {
        self.underlying.description
    }

    /// Wrapper around `CassUuidGen` for generating time-based UUID.
    ///
    /// - SeeAlso: https://docs.datastax.com/en/developer/cpp-driver/2.15/topics/basics/uuids/
    private class UUIDGenerator {
        static let instance = UUIDGenerator()

        let rawPointer: OpaquePointer

        init() {
            self.rawPointer = cass_uuid_gen_new()
        }

        deinit {
            cass_uuid_gen_free(self.rawPointer)
        }

        func generateTimeBased() -> Foundation.UUID {
            var value = CassUuid()
            cass_uuid_gen_time(self.rawPointer, &value)
            return value.uuid()
        }
    }
}

extension CassandraClient.Column {
    /// Get column value as `UUID`.
    public var uuid: Foundation.UUID? {
        var value = CassUuid()
        let error = cass_value_get_uuid(self.rawPointer, &value)
        guard error == CASS_OK else {
            return nil
        }
        return value.uuid()
    }

    /// Get column value as ``TimeBasedUUID``.
    public var timeuuid: TimeBasedUUID? {
        var value = CassUuid()
        let error = cass_value_get_uuid(self.rawPointer, &value)
        guard error == CASS_OK else {
            return nil
        }
        return TimeBasedUUID(uuid: value)
    }
}

extension CassandraClient.Row {
    /// Get column value as `UUID`.
    public func column(_ name: String) -> Foundation.UUID? {
        self.column(name)?.uuid
    }

    /// Get column value as `UUID`.
    public func column(_ index: Int) -> Foundation.UUID? {
        self.column(index)?.uuid
    }

    /// Get column value as ``TimeBasedUUID``.
    public func column(_ name: String) -> TimeBasedUUID? {
        self.column(name)?.timeuuid
    }

    /// Get column value as ``TimeBasedUUID``.
    public func column(_ index: Int) -> TimeBasedUUID? {
        self.column(index)?.timeuuid
    }
}

extension CassUuid {
    internal init(_ uuid: uuid_t) {
        self.init()

        var timeAndVersion = [UInt8]()
        timeAndVersion.append(uuid.6)
        timeAndVersion.append(uuid.7)
        timeAndVersion.append(uuid.4)
        timeAndVersion.append(uuid.5)
        timeAndVersion.append(uuid.0)
        timeAndVersion.append(uuid.1)
        timeAndVersion.append(uuid.2)
        timeAndVersion.append(uuid.3)
        time_and_version = cass_uint64_t(
            UInt64(
                bigEndian: timeAndVersion.withUnsafeBufferPointer {
                    ($0.baseAddress!.withMemoryRebound(to: UInt64.self, capacity: 1) { $0 })
                }.pointee
            )
        )

        var clockSeqAndNode = [UInt8]()
        clockSeqAndNode.append(uuid.8)
        clockSeqAndNode.append(uuid.9)
        clockSeqAndNode.append(uuid.10)
        clockSeqAndNode.append(uuid.11)
        clockSeqAndNode.append(uuid.12)
        clockSeqAndNode.append(uuid.13)
        clockSeqAndNode.append(uuid.14)
        clockSeqAndNode.append(uuid.15)
        clock_seq_and_node = cass_uint64_t(
            UInt64(
                bigEndian: clockSeqAndNode.withUnsafeBufferPointer {
                    ($0.baseAddress!.withMemoryRebound(to: UInt64.self, capacity: 1) { $0 })
                }.pointee
            )
        )
    }

    internal func uuid() -> Foundation.UUID {
        var buffer: uuid_t

        let timeAndVersion = withUnsafeBytes(of: time_and_version.bigEndian) { Array($0) }
        buffer.0 = timeAndVersion[4]
        buffer.1 = timeAndVersion[5]
        buffer.2 = timeAndVersion[6]
        buffer.3 = timeAndVersion[7]
        buffer.4 = timeAndVersion[2]
        buffer.5 = timeAndVersion[3]
        buffer.6 = timeAndVersion[0]
        buffer.7 = timeAndVersion[1]

        let clockSeqAndNode = withUnsafeBytes(of: clock_seq_and_node.bigEndian) { Array($0) }
        buffer.8 = clockSeqAndNode[0]
        buffer.9 = clockSeqAndNode[1]
        buffer.10 = clockSeqAndNode[2]
        buffer.11 = clockSeqAndNode[3]
        buffer.12 = clockSeqAndNode[4]
        buffer.13 = clockSeqAndNode[5]
        buffer.14 = clockSeqAndNode[6]
        buffer.15 = clockSeqAndNode[7]

        return Foundation.UUID(uuid: buffer)
    }
}

// MARK: - Date

extension CassandraClient.Column {
    /// Get column date value as `UInt32`.
    public var date: UInt32? {
        var value: UInt32 = 0
        let error = cass_value_get_uint32(self.rawPointer, &value)
        return error == CASS_OK ? value : nil
    }
}

// MARK: - Timestamp

extension CassandraClient.Column {
    /// Get column timestamp value as `Int64`.
    public var timestamp: Int64? {
        self.int64
    }
}

// MARK: - Bytes

extension CassandraClient.Column {
    /// Get column value as `[UInt8]`.
    public var bytes: [UInt8]? {
        var value: UnsafePointer<UInt8>?
        var size = 0
        let error = cass_value_get_bytes(self.rawPointer, &value, &size)
        return error == CASS_OK ? Array(UnsafeBufferPointer(start: value, count: size)) : nil
    }
}

extension CassandraClient.Row {
    /// Get column value as `[UInt8]`.
    public func column(_ name: String) -> [UInt8]? {
        self.column(name)?.bytes
    }

    /// Get column value as `[UInt8]`.
    public func column(_ index: Int) -> [UInt8]? {
        self.column(index)?.bytes
    }
}

// MARK: - Unsafe bytes

extension CassandraClient.Column {
    /// Get column value as buffer pointer and pass it to the given closure.
    public func withUnsafeBuffer<R>(closure: (UnsafeBufferPointer<UInt8>?) throws -> R) rethrows -> R {
        var value: UnsafePointer<UInt8>?
        var valueSize = Int()
        let error = cass_value_get_bytes(self.rawPointer, &value, &valueSize)
        if error == CASS_OK {
            return try closure(UnsafeBufferPointer(start: value, count: valueSize))
        } else {
            return try closure(nil)
        }
    }
}

// MARK: - Arrays

extension CassandraClient.Column {
    /// Get column value as `[Int8]`.
    public var int8Array: [Int8]? {
        self.toArray(type: Int8.self)
    }

    /// Get column value as `[Int16]`.
    public var int16Array: [Int16]? {
        self.toArray(type: Int16.self)
    }

    /// Get column value as `[Int32]`.
    public var int32Array: [Int32]? {
        self.toArray(type: Int32.self)
    }

    /// Get column value as `[Int64]`.
    public var int64Array: [Int64]? {
        self.toArray(type: Int64.self)
    }

    /// Get column value as `[Float32]`.
    public var float32Array: [Float32]? {
        self.toArray(type: Float32.self)
    }

    /// Get column value as `[Double]`.
    public var doubleArray: [Double]? {
        self.toArray(type: Double.self)
    }

    /// Get column value as `[String]`.
    public var stringArray: [String]? {
        self.toArray(type: String.self)
    }

    private func toArray<T>(type: T.Type) -> [T]? {
        var array: [T] = []

        let iterator = cass_iterator_from_collection(self.rawPointer)
        while cass_iterator_next(iterator) == cass_true {
            let valuePointer = cass_iterator_get_value(iterator)
            let value: T?
            switch type {
            case is Int8.Type:
                var v: Int8 = 0
                let error = cass_value_get_int8(valuePointer, &v)
                value = error == CASS_OK ? v as? T : nil
            case is Int16.Type:
                var v: Int16 = 0
                let error = cass_value_get_int16(valuePointer, &v)
                value = error == CASS_OK ? v as? T : nil
            case is Int32.Type:
                var v: Int32 = 0
                let error = cass_value_get_int32(valuePointer, &v)
                value = error == CASS_OK ? v as? T : nil
            case is Int64.Type:
                var v: Int64 = 0
                let error = cass_value_get_int64(valuePointer, &v)
                value = error == CASS_OK ? v as? T : nil
            case is Float32.Type:
                var v: Float32 = 0
                let error = cass_value_get_float(valuePointer, &v)
                value = error == CASS_OK ? v as? T : nil
            case is Double.Type:
                var v: Double = 0
                let error = cass_value_get_double(valuePointer, &v)
                value = error == CASS_OK ? v as? T : nil
            case is String.Type:
                value = valuePointer.flatMap { toString(cassValue: $0) as? T }
            default:
                value = nil
            }
            guard let value = value else {
                continue
            }
            array.append(value)
        }

        return array
    }
}

extension CassandraClient.Row {
    /// Get column value as `[Int8]`.
    public func column(_ name: String) -> [Int8]? {
        self.column(name)?.int8Array
    }

    /// Get column value as `[Int8]`.
    public func column(_ index: Int) -> [Int8]? {
        self.column(index)?.int8Array
    }

    /// Get column value as `[Int16]`.
    public func column(_ name: String) -> [Int16]? {
        self.column(name)?.int16Array
    }

    /// Get column value as `[Int16]`.
    public func column(_ index: Int) -> [Int16]? {
        self.column(index)?.int16Array
    }

    /// Get column value as `[Int32]`.
    public func column(_ name: String) -> [Int32]? {
        self.column(name)?.int32Array
    }

    /// Get column value as `[Int32]`.
    public func column(_ index: Int) -> [Int32]? {
        self.column(index)?.int32Array
    }

    /// Get column value as `[Int64]`.
    public func column(_ name: String) -> [Int64]? {
        self.column(name)?.int64Array
    }

    /// Get column value as `[Int64]`.
    public func column(_ index: Int) -> [Int64]? {
        self.column(index)?.int64Array
    }

    /// Get column value as `[Float32]`.
    public func column(_ name: String) -> [Float32]? {
        self.column(name)?.float32Array
    }

    /// Get column value as `[Float32]`.
    public func column(_ index: Int) -> [Float32]? {
        self.column(index)?.float32Array
    }

    /// Get column value as `[Double]`.
    public func column(_ name: String) -> [Double]? {
        self.column(name)?.doubleArray
    }

    /// Get column value as `[Double]`.
    public func column(_ index: Int) -> [Double]? {
        self.column(index)?.doubleArray
    }

    /// Get column value as `[String]`.
    public func column(_ name: String) -> [String]? {
        self.column(name)?.stringArray
    }

    /// Get column value as `[String]`.
    public func column(_ index: Int) -> [String]? {
        self.column(index)?.stringArray
    }
}
