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

import Foundation  // for date and uuid

extension CassandraClient {
    internal struct RowDecoder: Decoder {
        private let row: Row

        var codingPath = [CodingKey]()

        var userInfo = [CodingUserInfoKey: Any]()

        init(row: Row) {
            self.row = row
        }

        /// Create a decoder with encryption support for decoding `Encrypted<T>` fields.
        @available(macOS 15.0, iOS 18.0, visionOS 2.0, *)
        init(row: Row, encryptor: Encryptor, rowContext: EncryptionContext.Base) {
            self.row = row
            self.userInfo[.cassandraEncryptor] = encryptor
            self.userInfo[.cassandraRowContext] = rowContext
        }

        public func container<Key>(keyedBy _: Key.Type) throws -> KeyedDecodingContainer<Key> {
            KeyedDecodingContainer(RowDecodingContainer<Key>(row: self.row, userInfo: self.userInfo))
        }

        public func unkeyedContainer() throws -> UnkeyedDecodingContainer {
            throw DecodingError.notSupported("unkeyedContainer not supported")
        }

        public func singleValueContainer() throws -> SingleValueDecodingContainer {
            throw DecodingError.notSupported("singleValueContainer not supported")
        }
    }

    private struct RowDecodingContainer<Key: CodingKey>: KeyedDecodingContainerProtocol {
        private let row: Row
        private let userInfo: [CodingUserInfoKey: Any]

        public var codingPath = [CodingKey]()

        init(row: Row, userInfo: [CodingUserInfoKey: Any] = [:]) {
            self.row = row
            self.userInfo = userInfo
        }

        public var allKeys: [Key] {
            []
        }

        public func contains(_ key: Key) -> Bool {
            let column: Column? = self.row.column(key.stringValue)
            return column != nil
        }

        public func decodeNil(forKey key: Key) throws -> Bool {
            guard let column: Column = row.column(key.stringValue) else {
                throw DecodingError.typeMismatch("\(key.stringValue) not found.")
            }
            return column.isNull()
        }

        public func decode(_: Bool.Type, forKey key: Key) throws -> Bool {
            guard let value: Bool = row.column(key.stringValue) else {
                throw DecodingError.typeMismatch(
                    "value for \(key.stringValue) not found or of incorrect data type."
                )
            }
            return value
        }

        public func decode(_: Int.Type, forKey key: Key) throws -> Int {
            guard let value: Int32 = row.column(key.stringValue) else {
                throw DecodingError.typeMismatch(
                    "value for \(key.stringValue) not found or of incorrect data type."
                )
            }
            return Int(value)  // will always fit since storage is 32
        }

        public func decode(_: Int8.Type, forKey key: Key) throws -> Int8 {
            guard let value: Int8 = row.column(key.stringValue) else {
                throw DecodingError.typeMismatch(
                    "value for \(key.stringValue) not found or of incorrect data type."
                )
            }
            return value
        }

        public func decode(_: Int16.Type, forKey key: Key) throws -> Int16 {
            guard let value: Int16 = row.column(key.stringValue) else {
                throw DecodingError.typeMismatch(
                    "value for \(key.stringValue) not found or of incorrect data type."
                )
            }
            return value
        }

        public func decode(_: Int32.Type, forKey key: Key) throws -> Int32 {
            guard let value: Int32 = row.column(key.stringValue) else {
                throw DecodingError.typeMismatch(
                    "value for \(key.stringValue) not found or of incorrect data type."
                )
            }
            return value
        }

        public func decode(_: Int64.Type, forKey key: Key) throws -> Int64 {
            guard let value: Int64 = row.column(key.stringValue) else {
                throw DecodingError.typeMismatch(
                    "value for \(key.stringValue) not found or of incorrect data type."
                )
            }
            return value
        }

        public func decode(_: UInt.Type, forKey _: Key) throws -> UInt {
            throw DecodingError.notSupported("UInt is not supported")
        }

        public func decode(_: UInt8.Type, forKey _: Key) throws -> UInt8 {
            throw DecodingError.notSupported("UInt8 is not supported")
        }

        public func decode(_: UInt16.Type, forKey _: Key) throws -> UInt16 {
            throw DecodingError.notSupported("UInt16 is not supported")
        }

        public func decode(_: UInt32.Type, forKey _: Key) throws -> UInt32 {
            throw DecodingError.notSupported("UInt32 is not supported")
        }

        public func decode(_: UInt64.Type, forKey _: Key) throws -> UInt64 {
            throw DecodingError.notSupported("UInt64 is not supported")
        }

        public func decode(_: Float.Type, forKey key: Key) throws -> Float {
            guard let value: Float32 = row.column(key.stringValue) else {
                throw DecodingError.typeMismatch(
                    "value for \(key.stringValue) not found or of incorrect data type."
                )
            }
            return value
        }

        public func decode(_: Double.Type, forKey key: Key) throws -> Double {
            guard let value: Double = row.column(key.stringValue) else {
                throw DecodingError.typeMismatch(
                    "value for \(key.stringValue) not found or of incorrect data type."
                )
            }
            return value
        }

        public func decode(_: String.Type, forKey key: Key) throws -> String {
            guard let value: String = row.column(key.stringValue) else {
                throw DecodingError.typeMismatch(
                    "value for \(key.stringValue) not found or of incorrect data type."
                )
            }
            return value
        }

        // FIXME: is there a nicer way?
        public func decode<T: Decodable>(_ type: T.Type, forKey key: Key) throws -> T {
            // Encrypted types — decrypt column and deserialize
            if type == Encrypted<String>.self {
                let data = try decryptColumnData(key: key)
                guard let string = String(data: data, encoding: .utf8) else {
                    throw DecodingError.typeMismatch("Decrypted data for \(key.stringValue) is not valid UTF-8")
                }
                return Encrypted<String>(string) as! T
            } else if type == Encrypted<Int32>.self {
                let data = try decryptColumnData(key: key)
                guard data.count == 4 else {
                    throw DecodingError.typeMismatch("Expected 4 bytes for Int32, got \(data.count)")
                }
                guard let value = data.parseInt32BigEndian() else {
                    throw DecodingError.typeMismatch("Expected 4 bytes for Int32, got \(data.count)")
                }
                return Encrypted<Int32>(value) as! T
            } else if type == Encrypted<Int64>.self {
                let data = try decryptColumnData(key: key)
                guard data.count == 8 else {
                    throw DecodingError.typeMismatch("Expected 8 bytes for Int64, got \(data.count)")
                }
                guard let value = data.parseInt64BigEndian() else {
                    throw DecodingError.typeMismatch("Expected 8 bytes for Int64, got \(data.count)")
                }
                return Encrypted<Int64>(value) as! T
            } else if type == Encrypted<Double>.self {
                let data = try decryptColumnData(key: key)
                guard data.count == 8 else {
                    throw DecodingError.typeMismatch("Expected 8 bytes for Double, got \(data.count)")
                }
                guard let bits = data.parseUInt64BigEndian() else {
                    throw DecodingError.typeMismatch("Expected 8 bytes for Double, got \(data.count)")
                }
                return Encrypted<Double>(Double(bitPattern: bits)) as! T
            } else if type == Encrypted<Foundation.UUID>.self {
                let data = try decryptColumnData(key: key)
                guard data.count == 16 else {
                    throw DecodingError.typeMismatch("Expected 16 bytes for UUID, got \(data.count)")
                }
                let u: uuid_t = (
                    data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                    data[8], data[9], data[10], data[11], data[12], data[13], data[14], data[15]
                )
                return Encrypted<Foundation.UUID>(Foundation.UUID(uuid: u)) as! T
            } else if type == Encrypted<[UInt8]>.self {
                let data = try decryptColumnData(key: key)
                return Encrypted<[UInt8]>(Array(data)) as! T
            } else if type == Encrypted<Foundation.Date>.self {
                let data = try decryptColumnData(key: key)
                guard data.count == 8 else {
                    throw DecodingError.typeMismatch("Expected 8 bytes for Date, got \(data.count)")
                }
                guard let millis = data.parseInt64BigEndian() else {
                    throw DecodingError.typeMismatch("Expected 8 bytes for Date, got \(data.count)")
                }
                return Encrypted<Foundation.Date>(Foundation.Date(timeIntervalSince1970: Double(millis) / 1000.0)) as! T
            } else if type == [UInt8].self {
                guard let value: [UInt8] = row.column(key.stringValue) else {
                    throw DecodingError.typeMismatch(
                        "value for \(key.stringValue) not found or of incorrect data type."
                    )
                }
                return value as! T
            } else if type == Foundation.Date.self {
                guard let value: Int64 = row.column(key.stringValue)?.timestamp else {
                    throw DecodingError.typeMismatch(
                        "value for \(key.stringValue) not found or of incorrect data type."
                    )
                }
                return Foundation.Date(timeIntervalSince1970: Double(value) / 1000) as! T
            } else if type == Foundation.UUID.self {
                guard let value: Foundation.UUID = row.column(key.stringValue)?.uuid else {
                    throw DecodingError.typeMismatch(
                        "value for \(key.stringValue) not found or of incorrect data type."
                    )
                }
                return value as! T
            } else if type == TimeBasedUUID.self {
                guard let value: TimeBasedUUID = row.column(key.stringValue)?.timeuuid else {
                    throw DecodingError.typeMismatch(
                        "value for \(key.stringValue) not found or of incorrect data type."
                    )
                }
                return value as! T
            } else if type == [Int8].self {
                guard let value: [Int8] = row.column(key.stringValue) else {
                    throw DecodingError.typeMismatch(
                        "value for \(key.stringValue) not found or of incorrect data type."
                    )
                }
                return value as! T
            } else if type == [Int16].self {
                guard let value: [Int16] = row.column(key.stringValue) else {
                    throw DecodingError.typeMismatch(
                        "value for \(key.stringValue) not found or of incorrect data type."
                    )
                }
                return value as! T
            } else if type == [Int32].self {
                guard let value: [Int32] = row.column(key.stringValue) else {
                    throw DecodingError.typeMismatch(
                        "value for \(key.stringValue) not found or of incorrect data type."
                    )
                }
                return value as! T
            } else if type == [Int64].self {
                guard let value: [Int64] = row.column(key.stringValue) else {
                    throw DecodingError.typeMismatch(
                        "value for \(key.stringValue) not found or of incorrect data type."
                    )
                }
                return value as! T
            } else if type == [Float32].self {
                guard let value: [Float32] = row.column(key.stringValue) else {
                    throw DecodingError.typeMismatch(
                        "value for \(key.stringValue) not found or of incorrect data type."
                    )
                }
                return value as! T
            } else if type == [Double].self {
                guard let value: [Double] = row.column(key.stringValue) else {
                    throw DecodingError.typeMismatch(
                        "value for \(key.stringValue) not found or of incorrect data type."
                    )
                }
                return value as! T
            } else if type == [String].self {
                guard let value: [String] = row.column(key.stringValue) else {
                    throw DecodingError.typeMismatch(
                        "value for \(key.stringValue) not found or of incorrect data type."
                    )
                }
                return value as! T
            } else {
                guard let column: Column = row.column(key.stringValue) else {
                    throw DecodingError.typeMismatch(
                        "value for \(key.stringValue) not found or of incorrect data type."
                    )
                }
                guard column.valueIsUDT || column.valueIsList else {
                    throw DecodingError.notSupported("Decoding of \(type) is not supported.")
                }
                return try T(from: CassandraClient.UDTDecoder(column: column))
            }
        }

        public func nestedContainer<NestedKey>(
            keyedBy _: NestedKey.Type,
            forKey _: Key
        ) throws
            -> KeyedDecodingContainer<NestedKey>
        {
            throw DecodingError.notSupported()
        }

        public func nestedUnkeyedContainer(forKey _: Key) throws -> UnkeyedDecodingContainer {
            throw DecodingError.notSupported()
        }

        private func _superDecoder(forKey _: __owned CodingKey) throws -> Decoder {
            throw DecodingError.notSupported()
        }

        public func superDecoder() throws -> Decoder {
            throw DecodingError.notSupported()
        }

        public func superDecoder(forKey _: Key) throws -> Decoder {
            throw DecodingError.notSupported()
        }

        /// Decrypt column data using encryptor and context from userInfo.
        private func decryptColumnData(key: Key) throws -> Data {
            guard #available(macOS 15.0, iOS 18.0, visionOS 2.0, *) else {
                throw DecodingError.notSupported("Encryption requires macOS 15.0+")
            }
            guard let encryptor = userInfo[.cassandraEncryptor] as? CassandraClient.Encryptor else {
                throw DecodingError.notSupported(
                    "Encryptor not provided in decoder userInfo. Use RowDecoder(row:encryptor:rowContext:)"
                )
            }
            guard let rowContext = userInfo[.cassandraRowContext] as? CassandraClient.EncryptionContext.Base else {
                throw DecodingError.notSupported("EncryptionContext.Base missing from decoder userInfo")
            }
            let context = rowContext.forColumn(key.stringValue)
            guard let column: Column = row.column(key.stringValue) else {
                throw DecodingError.typeMismatch("value for \(key.stringValue) not found.")
            }
            guard let data = try column.decryptedData(encryptor: encryptor, context: context) else {
                throw DecodingError.typeMismatch("value for \(key.stringValue) is null.")
            }
            return data
        }
    }
}

private enum DecodingError: Error {
    case typeMismatch(String)
    case notSupported(String? = nil)
}

// MARK: - UDT decoding

extension CassandraClient {
    /// Decodes a UDT column (or a `list`/`set` of UDTs) into a `Decodable` value.
    internal struct UDTDecoder: Decoder {
        private let column: Column

        var codingPath = [CodingKey]()
        var userInfo = [CodingUserInfoKey: Any]()

        init(column: Column) {
            self.column = column
        }

        func container<Key>(keyedBy _: Key.Type) throws -> KeyedDecodingContainer<Key> {
            KeyedDecodingContainer(UDTFieldContainer<Key>(column: self.column))
        }

        func unkeyedContainer() throws -> UnkeyedDecodingContainer {
            guard let elements = self.column.udtArray else {
                throw DecodingError.typeMismatch("expected a list of UDTs")
            }
            return UDTArrayContainer(elements: elements)
        }

        func singleValueContainer() throws -> SingleValueDecodingContainer {
            throw DecodingError.notSupported("singleValueContainer not supported")
        }
    }

    private struct UDTFieldContainer<Key: CodingKey>: KeyedDecodingContainerProtocol {
        private let column: Column

        var codingPath = [CodingKey]()
        var allKeys: [Key] { [] }

        init(column: Column) {
            self.column = column
        }

        private func field(_ key: Key) throws -> Column {
            guard let field = self.column.field(key.stringValue) else {
                throw DecodingError.typeMismatch("field \(key.stringValue) not found.")
            }
            return field
        }

        func contains(_ key: Key) -> Bool {
            self.column.field(key.stringValue) != nil
        }

        func decodeNil(forKey key: Key) throws -> Bool {
            guard let field = self.column.field(key.stringValue) else { return true }
            return field.isNull()
        }

        func decode(_: Bool.Type, forKey key: Key) throws -> Bool {
            try unwrap(try self.field(key).bool, key)
        }

        func decode(_: Int.Type, forKey key: Key) throws -> Int {
            Int(try unwrap(try self.field(key).int32, key))
        }

        func decode(_: Int8.Type, forKey key: Key) throws -> Int8 {
            try unwrap(try self.field(key).int8, key)
        }

        func decode(_: Int16.Type, forKey key: Key) throws -> Int16 {
            try unwrap(try self.field(key).int16, key)
        }

        func decode(_: Int32.Type, forKey key: Key) throws -> Int32 {
            try unwrap(try self.field(key).int32, key)
        }

        func decode(_: Int64.Type, forKey key: Key) throws -> Int64 {
            try unwrap(try self.field(key).int64, key)
        }

        func decode(_: Float.Type, forKey key: Key) throws -> Float {
            try unwrap(try self.field(key).float32, key)
        }

        func decode(_: Double.Type, forKey key: Key) throws -> Double {
            try unwrap(try self.field(key).double, key)
        }

        func decode(_: String.Type, forKey key: Key) throws -> String {
            try unwrap(try self.field(key).string, key)
        }

        func decode(_: UInt.Type, forKey _: Key) throws -> UInt { throw DecodingError.notSupported("UInt") }
        func decode(_: UInt8.Type, forKey _: Key) throws -> UInt8 { throw DecodingError.notSupported("UInt8") }
        func decode(_: UInt16.Type, forKey _: Key) throws -> UInt16 { throw DecodingError.notSupported("UInt16") }
        func decode(_: UInt32.Type, forKey _: Key) throws -> UInt32 { throw DecodingError.notSupported("UInt32") }
        func decode(_: UInt64.Type, forKey _: Key) throws -> UInt64 { throw DecodingError.notSupported("UInt64") }

        func decode<T: Decodable>(_ type: T.Type, forKey key: Key) throws -> T {
            let field = try self.field(key)
            if let value = decodeKnownType(type, from: field) {
                return value
            }
            guard field.valueIsUDT || field.valueIsList else {
                throw DecodingError.notSupported("Decoding of \(type) is not supported.")
            }
            return try T(from: UDTDecoder(column: field))
        }

        func nestedContainer<NestedKey>(
            keyedBy _: NestedKey.Type,
            forKey _: Key
        ) throws
            -> KeyedDecodingContainer<NestedKey>
        {
            throw DecodingError.notSupported()
        }

        func nestedUnkeyedContainer(forKey _: Key) throws -> UnkeyedDecodingContainer {
            throw DecodingError.notSupported()
        }

        func superDecoder() throws -> Decoder { throw DecodingError.notSupported() }
        func superDecoder(forKey _: Key) throws -> Decoder { throw DecodingError.notSupported() }
    }

    private struct UDTArrayContainer: UnkeyedDecodingContainer {
        private let elements: [Column]

        var codingPath = [CodingKey]()
        var count: Int? { self.elements.count }
        var isAtEnd: Bool { self.currentIndex >= self.elements.count }
        private(set) var currentIndex = 0

        init(elements: [Column]) {
            self.elements = elements
        }

        mutating func decodeNil() throws -> Bool {
            guard self.elements[self.currentIndex].isNull() else { return false }
            self.currentIndex += 1
            return true
        }

        mutating func decode<T: Decodable>(_: T.Type) throws -> T {
            let element = self.elements[self.currentIndex]
            self.currentIndex += 1
            return try T(from: UDTDecoder(column: element))
        }

        mutating func decode(_: Bool.Type) throws -> Bool { throw DecodingError.notSupported() }
        mutating func decode(_: String.Type) throws -> String { throw DecodingError.notSupported() }
        mutating func decode(_: Double.Type) throws -> Double { throw DecodingError.notSupported() }
        mutating func decode(_: Float.Type) throws -> Float { throw DecodingError.notSupported() }
        mutating func decode(_: Int.Type) throws -> Int { throw DecodingError.notSupported() }
        mutating func decode(_: Int8.Type) throws -> Int8 { throw DecodingError.notSupported() }
        mutating func decode(_: Int16.Type) throws -> Int16 { throw DecodingError.notSupported() }
        mutating func decode(_: Int32.Type) throws -> Int32 { throw DecodingError.notSupported() }
        mutating func decode(_: Int64.Type) throws -> Int64 { throw DecodingError.notSupported() }
        mutating func decode(_: UInt.Type) throws -> UInt { throw DecodingError.notSupported() }
        mutating func decode(_: UInt8.Type) throws -> UInt8 { throw DecodingError.notSupported() }
        mutating func decode(_: UInt16.Type) throws -> UInt16 { throw DecodingError.notSupported() }
        mutating func decode(_: UInt32.Type) throws -> UInt32 { throw DecodingError.notSupported() }
        mutating func decode(_: UInt64.Type) throws -> UInt64 { throw DecodingError.notSupported() }

        mutating func nestedContainer<NestedKey>(
            keyedBy _: NestedKey.Type
        ) throws
            -> KeyedDecodingContainer<NestedKey>
        {
            throw DecodingError.notSupported()
        }

        mutating func nestedUnkeyedContainer() throws -> UnkeyedDecodingContainer {
            throw DecodingError.notSupported()
        }

        mutating func superDecoder() throws -> Decoder { throw DecodingError.notSupported() }
    }
}

/// Decode the scalar/array types a UDT field may hold; returns `nil` if `type` is not one of them.
private func decodeKnownType<T: Decodable>(_ type: T.Type, from column: CassandraClient.Column) -> T? {
    switch type {
    case is Foundation.UUID.Type:
        return column.uuid as? T
    case is TimeBasedUUID.Type:
        return column.timeuuid as? T
    case is Foundation.Date.Type:
        return column.timestamp.map { Foundation.Date(timeIntervalSince1970: Double($0) / 1000) } as? T
    case is [UInt8].Type:
        return column.bytes as? T
    case is [Int8].Type:
        return column.int8Array as? T
    case is [Int16].Type:
        return column.int16Array as? T
    case is [Int32].Type:
        return column.int32Array as? T
    case is [Int64].Type:
        return column.int64Array as? T
    case is [Float32].Type:
        return column.float32Array as? T
    case is [Double].Type:
        return column.doubleArray as? T
    case is [String].Type:
        return column.stringArray as? T
    default:
        return nil
    }
}

private func unwrap<T>(_ value: T?, _ key: CodingKey) throws -> T {
    guard let value = value else {
        throw DecodingError.typeMismatch(
            "value for \(key.stringValue) not found or of incorrect data type."
        )
    }
    return value
}
