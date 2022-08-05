import Foundation // for date and uuid

internal extension CassandraClient {
    struct RowDecoder: Decoder {
        private let row: Row

        var codingPath = [CodingKey]()

        var userInfo = [CodingUserInfoKey: Any]()

        init(row: Row) {
            self.row = row
        }

        public func container<Key>(keyedBy type: Key.Type) throws -> KeyedDecodingContainer<Key> {
            KeyedDecodingContainer(RowDecodingContainer<Key>(row: self.row))
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

        public var codingPath = [CodingKey]()

        init(row: Row) {
            self.row = row
        }

        public var allKeys: [Key] {
            return []
        }

        public func contains(_ key: Key) -> Bool {
            let column: Column? = self.row.column(key.stringValue)
            return column != nil
        }

        public func decodeNil(forKey key: Key) throws -> Bool {
            guard let column: Column = self.row.column(key.stringValue) else {
                throw DecodingError.typeMismatch("\(key.stringValue) not found.")
            }
            return column.isNull()
        }

        public func decode(_ type: Bool.Type, forKey key: Key) throws -> Bool {
            guard let value: Bool = self.row.column(key.stringValue) else {
                throw DecodingError.typeMismatch("value for \(key.stringValue) not found or of incorrect data type.")
            }
            return value
        }

        public func decode(_ type: Int.Type, forKey key: Key) throws -> Int {
            guard let value: Int32 = self.row.column(key.stringValue) else {
                throw DecodingError.typeMismatch("value for \(key.stringValue) not found or of incorrect data type.")
            }
            return Int(value) // will always fit since storage is 32
        }

        public func decode(_ type: Int8.Type, forKey key: Key) throws -> Int8 {
            guard let value: Int8 = self.row.column(key.stringValue) else {
                throw DecodingError.typeMismatch("value for \(key.stringValue) not found or of incorrect data type.")
            }
            return value
        }

        public func decode(_ type: Int16.Type, forKey key: Key) throws -> Int16 {
            guard let value: Int16 = self.row.column(key.stringValue) else {
                throw DecodingError.typeMismatch("value for \(key.stringValue) not found or of incorrect data type.")
            }
            return value
        }

        public func decode(_ type: Int32.Type, forKey key: Key) throws -> Int32 {
            guard let value: Int32 = self.row.column(key.stringValue) else {
                throw DecodingError.typeMismatch("value for \(key.stringValue) not found or of incorrect data type.")
            }
            return value
        }

        public func decode(_ type: Int64.Type, forKey key: Key) throws -> Int64 {
            guard let value: Int64 = self.row.column(key.stringValue) else {
                throw DecodingError.typeMismatch("value for \(key.stringValue) not found or of incorrect data type.")
            }
            return value
        }

        public func decode(_ type: UInt.Type, forKey key: Key) throws -> UInt {
            throw DecodingError.notSupported("UInt is not supported")
        }

        public func decode(_ type: UInt8.Type, forKey key: Key) throws -> UInt8 {
            throw DecodingError.notSupported("UInt8 is not supported")
        }

        public func decode(_ type: UInt16.Type, forKey key: Key) throws -> UInt16 {
            throw DecodingError.notSupported("UInt16 is not supported")
        }

        public func decode(_ type: UInt32.Type, forKey key: Key) throws -> UInt32 {
            throw DecodingError.notSupported("UInt32 is not supported")
        }

        public func decode(_ type: UInt64.Type, forKey key: Key) throws -> UInt64 {
            throw DecodingError.notSupported("UInt64 is not supported")
        }

        public func decode(_ type: Float.Type, forKey key: Key) throws -> Float {
            guard let value: Float32 = self.row.column(key.stringValue) else {
                throw DecodingError.typeMismatch("value for \(key.stringValue) not found or of incorrect data type.")
            }
            return value
        }

        public func decode(_ type: Double.Type, forKey key: Key) throws -> Double {
            guard let value: Double = self.row.column(key.stringValue) else {
                throw DecodingError.typeMismatch("value for \(key.stringValue) not found or of incorrect data type.")
            }
            return value
        }

        public func decode(_ type: String.Type, forKey key: Key) throws -> String {
            guard let value: String = self.row.column(key.stringValue) else {
                throw DecodingError.typeMismatch("value for \(key.stringValue) not found or of incorrect data type.")
            }
            return value
        }

        // FIXME: is there a nicer way?
        public func decode<T: Decodable>(_ type: T.Type, forKey key: Key) throws -> T {
            if type == [UInt8].self {
                guard let value: [UInt8] = self.row.column(key.stringValue) else {
                    throw DecodingError.typeMismatch("value for \(key.stringValue) not found or of incorrect data type.")
                }
                return value as! T
            } else if type == Foundation.Date.self {
                guard let value: Int64 = self.row.column(key.stringValue)?.timestamp else {
                    throw DecodingError.typeMismatch("value for \(key.stringValue) not found or of incorrect data type.")
                }
                return Foundation.Date(timeIntervalSince1970: Double(value) / 1000) as! T
            } else if type == Foundation.UUID.self {
                guard let value: Foundation.UUID = self.row.column(key.stringValue)?.uuid else {
                    throw DecodingError.typeMismatch("value for \(key.stringValue) not found or of incorrect data type.")
                }
                return value as! T
            } else if type == TimeBasedUUID.self {
                guard let value: TimeBasedUUID = self.row.column(key.stringValue)?.timeuuid else {
                    throw DecodingError.typeMismatch("value for \(key.stringValue) not found or of incorrect data type.")
                }
                return value as! T
            } else {
                throw DecodingError.notSupported("Decoding of \(type) is not supported.")
            }
        }

        public func nestedContainer<NestedKey>(keyedBy type: NestedKey.Type, forKey key: Key) throws -> KeyedDecodingContainer<NestedKey> {
            throw DecodingError.notSupported()
        }

        public func nestedUnkeyedContainer(forKey key: Key) throws -> UnkeyedDecodingContainer {
            throw DecodingError.notSupported()
        }

        private func _superDecoder(forKey key: __owned CodingKey) throws -> Decoder {
            throw DecodingError.notSupported()
        }

        public func superDecoder() throws -> Decoder {
            throw DecodingError.notSupported()
        }

        public func superDecoder(forKey key: Key) throws -> Decoder {
            throw DecodingError.notSupported()
        }
    }
}

private enum DecodingError: Error {
    case typeMismatch(String)
    case notSupported(String? = nil)
}
