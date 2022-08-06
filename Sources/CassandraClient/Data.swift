//===----------------------------------------------------------------------===//
//
// This source file is part of the Swift Cassandra Client open source project
//
// Copyright (c) 2022 Apple Inc. and the Swift Cassandra Client project authors
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

public extension CassandraClient {
    final class Rows: Sequence {
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

        public func makeIterator() -> Iterator {
            Iterator(rows: self)
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

    final class PaginatedRows {
        let session: Session
        let statement: Statement
        let eventLoop: EventLoop?
        let logger: Logger?

        /// If `true`, calling `nextPage()` will return a new set of `Rows`.
        /// Otherwise it will be `CassandraClient.Error.rowsExhausted`.
        public private(set) var hasMorePages: Bool = true

        internal init(session: Session, statement: Statement, on eventLoop: EventLoop, logger: Logger?) {
            self.session = session
            self.statement = statement
            self.eventLoop = eventLoop
            self.logger = logger
        }

        /// Fetches next page of rows or `CassandraClient.Error.rowsExhausted` if there were no more pages.
        public func nextPage() -> EventLoopFuture<Rows> {
            guard let eventLoop = self.eventLoop else {
                preconditionFailure("EventLoop must not be nil")
            }
            guard self.hasMorePages else {
                return eventLoop.makeFailedFuture(CassandraClient.Error.rowsExhausted)
            }

            let future = self.session.execute(statement: self.statement, on: eventLoop, logger: self.logger)
            future.whenComplete { result in
                switch result {
                case .success(let rows):
                    self.hasMorePages = cass_result_has_more_pages(rows.rawPointer) == cass_true
                    if self.hasMorePages {
                        cass_statement_set_paging_state(self.statement.rawPointer, rows.rawPointer)
                    }
                case .failure:
                    self.hasMorePages = false
                }
            }
            return future
        }

        /// Iterates through all rows in all pages and invokes the given closure on each.
        public func forEach(_ body: @escaping (Row) throws -> Void) -> EventLoopFuture<Void> {
            precondition(self.hasMorePages, "Only one of 'forEach' or 'map' can be called once per PaginatedRows")

            guard let eventLoop = self.eventLoop else {
                preconditionFailure("EventLoop must not be nil")
            }

            func _forEach() -> EventLoopFuture<Void> {
                self.nextPage().flatMap { rows in
                    do {
                        try rows.forEach(body)
                    } catch {
                        return eventLoop.makeFailedFuture(error)
                    }

                    guard self.hasMorePages else {
                        return eventLoop.makeSucceededFuture(())
                    }
                    return _forEach()
                }
            }
            return _forEach()
        }

        /// Iterates through all rows in all pages and applies `transform` on each.
        public func map<T>(_ transform: @escaping (Row) throws -> T) -> EventLoopFuture<[T]> {
            precondition(self.hasMorePages, "Only one of 'forEach' or 'map' can be called once per PaginatedRows")

            guard let eventLoop = self.eventLoop else {
                preconditionFailure("EventLoop must not be nil in EventLoop based APIs")
            }

            func _map(_ accumulated: [T]) -> EventLoopFuture<[T]> {
                self.nextPage().flatMap { rows in
                    do {
                        let transformed = try rows.map(transform)
                        let newAccumulated = accumulated + transformed
                        guard self.hasMorePages else {
                            return eventLoop.makeSucceededFuture(newAccumulated)
                        }
                        return _map(newAccumulated)
                    } catch {
                        return eventLoop.makeFailedFuture(error)
                    }
                }
            }
            return _map([])
        }

        #if compiler(>=5.5) && canImport(_Concurrency)
        @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
        internal init(session: Session, statement: Statement, logger: Logger?) {
            self.session = session
            self.statement = statement
            self.eventLoop = nil
            self.logger = logger
        }

        /// Fetches next page of rows or `CassandraClient.Error.rowsExhausted` if there were no more pages.
        @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
        public func nextPage() async throws -> Rows {
            guard self.hasMorePages else {
                throw CassandraClient.Error.rowsExhausted
            }

            do {
                let rows = try await session.execute(statement: self.statement, logger: self.logger)
                self.hasMorePages = cass_result_has_more_pages(rows.rawPointer) == cass_true
                if self.hasMorePages {
                    cass_statement_set_paging_state(self.statement.rawPointer, rows.rawPointer)
                }
                return rows
            } catch {
                self.hasMorePages = false
                throw error
            }
        }

        /// Iterates through all rows in all pages and invokes the given closure on each.
        @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
        public func forEach(_ body: @escaping (Row) throws -> Void) async throws {
            precondition(self.hasMorePages, "Only one of 'forEach' or 'map' can be called once per PaginatedRows")

            func _forEach() async throws {
                let rows = try await nextPage()
                try rows.forEach(body)

                guard self.hasMorePages else {
                    return
                }
                try await _forEach()
            }
            try await _forEach()
        }

        /// Iterates through all rows in all pages and applies `transform` on each.
        @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
        public func map<T>(_ transform: @escaping (Row) throws -> T) async throws -> [T] {
            precondition(self.hasMorePages, "Only one of 'forEach' or 'map' can be called once per PaginatedRows")

            func _map(_ accumulated: [T]) async throws -> [T] {
                let rows = try await nextPage()
                let transformed = try rows.map(transform)
                let newAccumulated = accumulated + transformed

                guard self.hasMorePages else {
                    return newAccumulated
                }
                return try await _map(newAccumulated)
            }
            return try await _map([])
        }
        #endif
    }

    struct Row {
        internal let rawPointer: OpaquePointer
        // Used to make sure the iterator isn't freed while a reference to one
        // of its rows still exists
        private let parent: Rows.Iterator

        internal init(iterator: Rows.Iterator) {
            self.rawPointer = cass_iterator_get_row(iterator.rawPointer)
            self.parent = iterator
        }

        public func column(_ index: Int) -> Column? {
            Column(row: self, index: index)
        }

        public func column(_ name: String) -> Column? {
            Column(row: self, name: name)
        }
    }

    /// Note that the value is only good as long as the iterator it came from hasn't been advanced.
    struct Column {
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
}

// MARK: - Int8

public extension CassandraClient.Column {
    var int8: Int8? {
        var value: Int8 = 0
        let error = cass_value_get_int8(rawPointer, &value)
        if error == CASS_OK {
            return value
        } else {
            return nil
        }
    }
}

public extension CassandraClient.Row {
    func column(_ name: String) -> Int8? {
        self.column(name)?.int8
    }

    func column(_ index: Int) -> Int8? {
        self.column(index)?.int8
    }
}

// MARK: - Int16

public extension CassandraClient.Column {
    var int16: Int16? {
        var value: Int16 = 0
        let error = cass_value_get_int16(rawPointer, &value)
        return error == CASS_OK ? value : nil
    }
}

public extension CassandraClient.Row {
    func column(_ name: String) -> Int16? {
        self.column(name)?.int16
    }

    func column(_ index: Int) -> Int16? {
        self.column(index)?.int16
    }
}

// MARK: - Int32

public extension CassandraClient.Column {
    var int32: Int32? {
        var value: Int32 = 0
        let error = cass_value_get_int32(rawPointer, &value)
        return error == CASS_OK ? value : nil
    }
}

public extension CassandraClient.Row {
    func column(_ name: String) -> Int32? {
        self.column(name)?.int32
    }

    func column(_ index: Int) -> Int32? {
        self.column(index)?.int32
    }
}

// MARK: - UInt32

public extension CassandraClient.Column {
    var uint32: UInt32? {
        var value: UInt32 = 0
        let error = cass_value_get_uint32(rawPointer, &value)
        return error == CASS_OK ? value : nil
    }
}

public extension CassandraClient.Row {
    func column(_ name: String) -> UInt32? {
        self.column(name)?.uint32
    }

    func column(_ index: Int) -> UInt32? {
        self.column(index)?.uint32
    }
}

// MARK: - Int64

public extension CassandraClient.Column {
    var int64: Int64? {
        var value: cass_int64_t = 0
        let error = cass_value_get_int64(rawPointer, &value)
        return error == CASS_OK ? Int64(value) : nil
    }
}

public extension CassandraClient.Row {
    func column(_ name: String) -> Int64? {
        self.column(name)?.int64
    }

    func column(_ index: Int) -> Int64? {
        self.column(index)?.int64
    }
}

// MARK: - Float32

public extension CassandraClient.Column {
    var float32: Float32? {
        var value: Float32 = 0
        let error = cass_value_get_float(rawPointer, &value)
        return error == CASS_OK ? value : nil
    }
}

public extension CassandraClient.Row {
    func column(_ name: String) -> Float32? {
        self.column(name)?.float32
    }

    func column(_ index: Int) -> Float32? {
        self.column(index)?.float32
    }
}

// MARK: - Double

public extension CassandraClient.Column {
    var double: Double? {
        var value: Double = 0
        let error = cass_value_get_double(rawPointer, &value)
        return error == CASS_OK ? value : nil
    }
}

public extension CassandraClient.Row {
    func column(_ name: String) -> Double? {
        self.column(name)?.double
    }

    func column(_ index: Int) -> Double? {
        self.column(index)?.double
    }
}

// MARK: - Bool

public extension CassandraClient.Column {
    var bool: Bool? {
        var value = cass_bool_t(0)
        let error = cass_value_get_bool(rawPointer, &value)
        return error == CASS_OK ? value == cass_true : nil
    }
}

public extension CassandraClient.Row {
    func column(_ name: String) -> Bool? {
        self.column(name)?.bool
    }

    func column(_ index: Int) -> Bool? {
        self.column(index)?.bool
    }
}

// MARK: - String

public extension CassandraClient.Column {
    var string: String? {
        var value: UnsafePointer<CChar>?
        var valueSize = 0
        let error = cass_value_get_string(rawPointer, &value, &valueSize)
        guard let definiteValue = value, error == CASS_OK else {
            return nil
        }
        let stringBuffer = UnsafeBufferPointer(start: definiteValue, count: valueSize)
        return stringBuffer.withMemoryRebound(to: UInt8.self) {
            String(decoding: $0, as: UTF8.self)
        }
    }
}

public extension CassandraClient.Row {
    func column(_ name: String) -> String? {
        self.column(name)?.string
    }

    func column(_ index: Int) -> String? {
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
            rawPointer = cass_uuid_gen_new()
        }

        deinit {
            cass_uuid_gen_free(self.rawPointer)
        }

        func generateTimeBased() -> Foundation.UUID {
            var value = CassUuid()
            cass_uuid_gen_time(rawPointer, &value)
            return value.uuid()
        }
    }
}

public extension CassandraClient.Column {
    var uuid: Foundation.UUID? {
        var value = CassUuid()
        let error = cass_value_get_uuid(rawPointer, &value)
        guard error == CASS_OK else {
            return nil
        }
        return value.uuid()
    }

    var timeuuid: TimeBasedUUID? {
        var value = CassUuid()
        let error = cass_value_get_uuid(rawPointer, &value)
        guard error == CASS_OK else {
            return nil
        }
        return TimeBasedUUID(uuid: value)
    }
}

public extension CassandraClient.Row {
    func column(_ name: String) -> Foundation.UUID? {
        self.column(name)?.uuid
    }

    func column(_ index: Int) -> Foundation.UUID? {
        self.column(index)?.uuid
    }

    func column(_ name: String) -> TimeBasedUUID? {
        self.column(name)?.timeuuid
    }

    func column(_ index: Int) -> TimeBasedUUID? {
        self.column(index)?.timeuuid
    }
}

internal extension CassUuid {
    init(_ uuid: uuid_t) {
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
        time_and_version = cass_uint64_t(UInt64(bigEndian: timeAndVersion.withUnsafeBufferPointer {
            ($0.baseAddress!.withMemoryRebound(to: UInt64.self, capacity: 1) { $0 })
        }.pointee))

        var clockSeqAndNode = [UInt8]()
        clockSeqAndNode.append(uuid.8)
        clockSeqAndNode.append(uuid.9)
        clockSeqAndNode.append(uuid.10)
        clockSeqAndNode.append(uuid.11)
        clockSeqAndNode.append(uuid.12)
        clockSeqAndNode.append(uuid.13)
        clockSeqAndNode.append(uuid.14)
        clockSeqAndNode.append(uuid.15)
        clock_seq_and_node = cass_uint64_t(UInt64(bigEndian: clockSeqAndNode.withUnsafeBufferPointer {
            ($0.baseAddress!.withMemoryRebound(to: UInt64.self, capacity: 1) { $0 })
        }.pointee))
    }

    func uuid() -> Foundation.UUID {
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

public extension CassandraClient.Column {
    var date: UInt32? {
        var value: UInt32 = 0
        let error = cass_value_get_uint32(rawPointer, &value)
        return error == CASS_OK ? value : nil
    }
}

// MARK: - Timestamp

public extension CassandraClient.Column {
    var timestamp: Int64? {
        self.int64
    }
}

// MARK: - Bytes

public extension CassandraClient.Column {
    var bytes: [UInt8]? {
        var value: UnsafePointer<UInt8>?
        var size = 0
        let error = cass_value_get_bytes(rawPointer, &value, &size)
        return error == CASS_OK ? Array(UnsafeBufferPointer(start: value, count: size)) : nil
    }
}

public extension CassandraClient.Row {
    func column(_ name: String) -> [UInt8]? {
        self.column(name)?.bytes
    }

    func column(_ index: Int) -> [UInt8]? {
        self.column(index)?.bytes
    }
}

// MARK: - Unsafe bytes

public extension CassandraClient.Column {
    func withUnsafeBuffer<R>(closure: (UnsafeBufferPointer<UInt8>?) throws -> R) rethrows -> R {
        var value: UnsafePointer<UInt8>?
        var valueSize = Int()
        let error = cass_value_get_bytes(rawPointer, &value, &valueSize)
        if error == CASS_OK {
            return try closure(UnsafeBufferPointer(start: value, count: valueSize))
        } else {
            return try closure(nil)
        }
    }
}
