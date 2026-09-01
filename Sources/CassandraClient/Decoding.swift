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
            throw DecodingError.unsupportedOperation("unkeyedContainer not supported", codingPath: self.codingPath)
        }

        public func singleValueContainer() throws -> SingleValueDecodingContainer {
            throw DecodingError.unsupportedOperation(
                "singleValueContainer not supported",
                codingPath: self.codingPath
            )
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
                throw DecodingError.columnMissing(forKey: key, codingPath: self.codingPath)
            }
            return column.isNull()
        }

        /// The error for a typed column read that produced `nil`. The typed accessors collapse four
        /// distinct causes into `nil` — a column the row lacks, a NULL value, a stored type that cannot
        /// convert, and a text column whose bytes are not valid UTF-8 — so probe the untyped accessor
        /// to tell them apart.
        ///
        /// The probe repeats the lookup the caller just made. That is deliberate: it runs only once that
        /// lookup has already returned `nil`, so it never costs the success path, and classifying here
        /// rather than at each call site keeps every one of them a single `throw`.
        private func columnFailure(_ type: Any.Type, forKey key: Key) -> Swift.DecodingError {
            guard let column: Column = self.row.column(key.stringValue) else {
                return .columnMissing(forKey: key, codingPath: self.codingPath)
            }
            guard !column.isNull() else {
                return .columnNull(type, forKey: key, codingPath: self.codingPath)
            }
            // A textual column that cannot produce a `String` holds bytes that are not valid UTF-8. The
            // column type is what the property asked for, so this is corruption, not a mismatch.
            if type == String.self, column.isTextType {
                return .malformedPayload(
                    "value for \(key.stringValue) is not valid UTF-8",
                    forKey: key,
                    codingPath: self.codingPath
                )
            }
            return .columnTypeMismatch(type, forKey: key, codingPath: self.codingPath)
        }

        public func decode(_: Bool.Type, forKey key: Key) throws -> Bool {
            guard let value: Bool = row.column(key.stringValue) else {
                throw self.columnFailure(Bool.self, forKey: key)
            }
            return value
        }

        public func decode(_: Int.Type, forKey key: Key) throws -> Int {
            guard let value: Int32 = row.column(key.stringValue) else {
                throw self.columnFailure(Int.self, forKey: key)
            }
            return Int(value)  // will always fit since storage is 32
        }

        public func decode(_: Int8.Type, forKey key: Key) throws -> Int8 {
            guard let value: Int8 = row.column(key.stringValue) else {
                throw self.columnFailure(Int8.self, forKey: key)
            }
            return value
        }

        public func decode(_: Int16.Type, forKey key: Key) throws -> Int16 {
            guard let value: Int16 = row.column(key.stringValue) else {
                throw self.columnFailure(Int16.self, forKey: key)
            }
            return value
        }

        public func decode(_: Int32.Type, forKey key: Key) throws -> Int32 {
            guard let value: Int32 = row.column(key.stringValue) else {
                throw self.columnFailure(Int32.self, forKey: key)
            }
            return value
        }

        public func decode(_: Int64.Type, forKey key: Key) throws -> Int64 {
            guard let value: Int64 = row.column(key.stringValue) else {
                throw self.columnFailure(Int64.self, forKey: key)
            }
            return value
        }

        public func decode(_: UInt.Type, forKey key: Key) throws -> UInt {
            throw DecodingError.unsupportedType(
                UInt.self,
                forKey: key,
                codingPath: self.codingPath,
                "UInt is not supported"
            )
        }

        public func decode(_: UInt8.Type, forKey key: Key) throws -> UInt8 {
            throw DecodingError.unsupportedType(
                UInt8.self,
                forKey: key,
                codingPath: self.codingPath,
                "UInt8 is not supported"
            )
        }

        public func decode(_: UInt16.Type, forKey key: Key) throws -> UInt16 {
            throw DecodingError.unsupportedType(
                UInt16.self,
                forKey: key,
                codingPath: self.codingPath,
                "UInt16 is not supported"
            )
        }

        public func decode(_: UInt32.Type, forKey key: Key) throws -> UInt32 {
            throw DecodingError.unsupportedType(
                UInt32.self,
                forKey: key,
                codingPath: self.codingPath,
                "UInt32 is not supported"
            )
        }

        public func decode(_: UInt64.Type, forKey key: Key) throws -> UInt64 {
            throw DecodingError.unsupportedType(
                UInt64.self,
                forKey: key,
                codingPath: self.codingPath,
                "UInt64 is not supported"
            )
        }

        public func decode(_: Float.Type, forKey key: Key) throws -> Float {
            guard let value: Float32 = row.column(key.stringValue) else {
                throw self.columnFailure(Float.self, forKey: key)
            }
            return value
        }

        public func decode(_: Double.Type, forKey key: Key) throws -> Double {
            guard let value: Double = row.column(key.stringValue) else {
                throw self.columnFailure(Double.self, forKey: key)
            }
            return value
        }

        public func decode(_: String.Type, forKey key: Key) throws -> String {
            guard let value: String = row.column(key.stringValue) else {
                throw self.columnFailure(String.self, forKey: key)
            }
            return value
        }

        // FIXME: is there a nicer way?
        public func decode<T: Decodable>(_ type: T.Type, forKey key: Key) throws -> T {
            // Encrypted types — decrypt column and deserialize
            if type == Encrypted<String>.self {
                let data = try decryptColumnData(key: key, as: type)
                guard let string = String(data: data, encoding: .utf8) else {
                    throw DecodingError.malformedPayload(
                        "Decrypted data for \(key.stringValue) is not valid UTF-8",
                        forKey: key,
                        codingPath: self.codingPath
                    )
                }
                return Encrypted<String>(string) as! T
            } else if type == Encrypted<Int32>.self {
                let data = try decryptColumnData(key: key, as: type)
                guard data.count == 4 else {
                    throw DecodingError.malformedPayload(
                        "Expected 4 bytes for Int32, got \(data.count)",
                        forKey: key,
                        codingPath: self.codingPath
                    )
                }
                guard let value = data.parseInt32BigEndian() else {
                    throw DecodingError.malformedPayload(
                        "Expected 4 bytes for Int32, got \(data.count)",
                        forKey: key,
                        codingPath: self.codingPath
                    )
                }
                return Encrypted<Int32>(value) as! T
            } else if type == Encrypted<Int64>.self {
                let data = try decryptColumnData(key: key, as: type)
                guard data.count == 8 else {
                    throw DecodingError.malformedPayload(
                        "Expected 8 bytes for Int64, got \(data.count)",
                        forKey: key,
                        codingPath: self.codingPath
                    )
                }
                guard let value = data.parseInt64BigEndian() else {
                    throw DecodingError.malformedPayload(
                        "Expected 8 bytes for Int64, got \(data.count)",
                        forKey: key,
                        codingPath: self.codingPath
                    )
                }
                return Encrypted<Int64>(value) as! T
            } else if type == Encrypted<Double>.self {
                let data = try decryptColumnData(key: key, as: type)
                guard data.count == 8 else {
                    throw DecodingError.malformedPayload(
                        "Expected 8 bytes for Double, got \(data.count)",
                        forKey: key,
                        codingPath: self.codingPath
                    )
                }
                guard let bits = data.parseUInt64BigEndian() else {
                    throw DecodingError.malformedPayload(
                        "Expected 8 bytes for Double, got \(data.count)",
                        forKey: key,
                        codingPath: self.codingPath
                    )
                }
                return Encrypted<Double>(Double(bitPattern: bits)) as! T
            } else if type == Encrypted<Foundation.UUID>.self {
                let data = try decryptColumnData(key: key, as: type)
                guard data.count == 16 else {
                    throw DecodingError.malformedPayload(
                        "Expected 16 bytes for UUID, got \(data.count)",
                        forKey: key,
                        codingPath: self.codingPath
                    )
                }
                let u: uuid_t = (
                    data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                    data[8], data[9], data[10], data[11], data[12], data[13], data[14], data[15]
                )
                return Encrypted<Foundation.UUID>(Foundation.UUID(uuid: u)) as! T
            } else if type == Encrypted<[UInt8]>.self {
                let data = try decryptColumnData(key: key, as: type)
                return Encrypted<[UInt8]>(Array(data)) as! T
            } else if type == Encrypted<Foundation.Date>.self {
                let data = try decryptColumnData(key: key, as: type)
                guard data.count == 8 else {
                    throw DecodingError.malformedPayload(
                        "Expected 8 bytes for Date, got \(data.count)",
                        forKey: key,
                        codingPath: self.codingPath
                    )
                }
                guard let millis = data.parseInt64BigEndian() else {
                    throw DecodingError.malformedPayload(
                        "Expected 8 bytes for Date, got \(data.count)",
                        forKey: key,
                        codingPath: self.codingPath
                    )
                }
                return Encrypted<Foundation.Date>(Foundation.Date(timeIntervalSince1970: Double(millis) / 1000.0)) as! T
            } else if type == [UInt8].self {
                guard let value: [UInt8] = row.column(key.stringValue) else {
                    throw self.columnFailure(type, forKey: key)
                }
                return value as! T
            } else if type == Foundation.Date.self {
                guard let value: Int64 = row.column(key.stringValue)?.timestamp else {
                    throw self.columnFailure(type, forKey: key)
                }
                return Foundation.Date(timeIntervalSince1970: Double(value) / 1000) as! T
            } else if type == Foundation.UUID.self {
                guard let value: Foundation.UUID = row.column(key.stringValue)?.uuid else {
                    throw self.columnFailure(type, forKey: key)
                }
                return value as! T
            } else if type == TimeBasedUUID.self {
                guard let value: TimeBasedUUID = row.column(key.stringValue)?.timeuuid else {
                    throw self.columnFailure(type, forKey: key)
                }
                return value as! T
            } else if type == [Int8].self {
                guard let value: [Int8] = row.column(key.stringValue) else {
                    throw self.columnFailure(type, forKey: key)
                }
                return value as! T
            } else if type == [Int16].self {
                guard let value: [Int16] = row.column(key.stringValue) else {
                    throw self.columnFailure(type, forKey: key)
                }
                return value as! T
            } else if type == [Int32].self {
                guard let value: [Int32] = row.column(key.stringValue) else {
                    throw self.columnFailure(type, forKey: key)
                }
                return value as! T
            } else if type == [Int64].self {
                guard let value: [Int64] = row.column(key.stringValue) else {
                    throw self.columnFailure(type, forKey: key)
                }
                return value as! T
            } else if type == [Float32].self {
                guard let value: [Float32] = row.column(key.stringValue) else {
                    throw self.columnFailure(type, forKey: key)
                }
                return value as! T
            } else if type == [Double].self {
                guard let value: [Double] = row.column(key.stringValue) else {
                    throw self.columnFailure(type, forKey: key)
                }
                return value as! T
            } else if type == [String].self {
                guard let value: [String] = row.column(key.stringValue) else {
                    throw self.columnFailure(type, forKey: key)
                }
                return value as! T
            } else if type == [Foundation.UUID].self {
                guard let value: [Foundation.UUID] = row.column(key.stringValue) else {
                    throw self.columnFailure(type, forKey: key)
                }
                return value as! T
            } else {
                throw DecodingError.unsupportedType(
                    type,
                    forKey: key,
                    codingPath: self.codingPath,
                    "Decoding of \(type) is not supported."
                )
            }
        }

        public func nestedContainer<NestedKey>(
            keyedBy _: NestedKey.Type,
            forKey key: Key
        ) throws
            -> KeyedDecodingContainer<NestedKey>
        {
            throw DecodingError.unsupportedOperation(
                "nested containers are not supported",
                codingPath: self.codingPath + [key]
            )
        }

        public func nestedUnkeyedContainer(forKey key: Key) throws -> UnkeyedDecodingContainer {
            throw DecodingError.unsupportedOperation(
                "nested unkeyed containers are not supported",
                codingPath: self.codingPath + [key]
            )
        }

        private func _superDecoder(forKey key: __owned CodingKey) throws -> Decoder {
            throw DecodingError.unsupportedOperation(
                "superDecoder is not supported",
                codingPath: self.codingPath + [key]
            )
        }

        public func superDecoder() throws -> Decoder {
            throw DecodingError.unsupportedOperation(
                "superDecoder is not supported",
                codingPath: self.codingPath
            )
        }

        public func superDecoder(forKey key: Key) throws -> Decoder {
            throw DecodingError.unsupportedOperation(
                "superDecoder is not supported",
                codingPath: self.codingPath + [key]
            )
        }

        /// Decrypt column data using encryptor and context from userInfo. `type` is the type the
        /// caller asked for, reported if the column is null.
        private func decryptColumnData(key: Key, as type: Any.Type) throws -> Data {
            guard #available(macOS 15.0, iOS 18.0, visionOS 2.0, *) else {
                throw CassandraClient.Error.encryptionConfigError("Encryption requires macOS 15.0+")
            }
            guard let encryptor = userInfo[.cassandraEncryptor] as? CassandraClient.Encryptor else {
                throw CassandraClient.Error.encryptionConfigError(
                    "Encryptor not provided in decoder userInfo. Use RowDecoder(row:encryptor:rowContext:)"
                )
            }
            guard let rowContext = userInfo[.cassandraRowContext] as? CassandraClient.EncryptionContext.Base else {
                throw CassandraClient.Error.encryptionConfigError(
                    "EncryptionContext.Base missing from decoder userInfo"
                )
            }
            let context = rowContext.forColumn(key.stringValue)
            guard let column: Column = row.column(key.stringValue) else {
                throw DecodingError.columnMissing(forKey: key, codingPath: self.codingPath)
            }
            guard let data = try column.decryptedData(encryptor: encryptor, context: context) else {
                throw DecodingError.columnNull(type, forKey: key, codingPath: self.codingPath)
            }
            return data
        }
    }
}

extension Swift.DecodingError {
    /// A column the row does not contain.
    fileprivate static func columnMissing(
        forKey key: some CodingKey,
        codingPath: [CodingKey]
    ) -> Swift.DecodingError {
        .keyNotFound(
            key,
            Context(
                codingPath: codingPath,
                debugDescription: "value for \(key.stringValue) not found."
            )
        )
    }

    /// A column holding NULL, read into a non-Optional property. Optionals never reach here: they
    /// route through `decodeIfPresent` to `decodeNil(forKey:)`.
    fileprivate static func columnNull(
        _ type: Any.Type,
        forKey key: some CodingKey,
        codingPath: [CodingKey]
    ) -> Swift.DecodingError {
        .valueNotFound(
            type,
            Context(
                codingPath: codingPath + [key],
                debugDescription: "value for \(key.stringValue) is null; declare the property Optional."
            )
        )
    }

    /// A column present in the row whose stored type cannot produce `type`.
    fileprivate static func columnTypeMismatch(
        _ type: Any.Type,
        forKey key: some CodingKey,
        codingPath: [CodingKey]
    ) -> Swift.DecodingError {
        .typeMismatch(
            type,
            Context(
                codingPath: codingPath + [key],
                debugDescription: "value for \(key.stringValue) is of incorrect data type."
            )
        )
    }

    /// A type this decoder cannot produce, whatever the row holds.
    fileprivate static func unsupportedType(
        _ type: Any.Type,
        forKey key: some CodingKey,
        codingPath: [CodingKey],
        _ description: String
    ) -> Swift.DecodingError {
        .typeMismatch(type, Context(codingPath: codingPath + [key], debugDescription: description))
    }

    /// A container operation this decoder does not implement. A row is flat, so there is no
    /// requested type to report.
    fileprivate static func unsupportedOperation(
        _ description: String,
        codingPath: [CodingKey]
    ) -> Swift.DecodingError {
        .dataCorrupted(Context(codingPath: codingPath, debugDescription: description))
    }

    /// Stored bytes that do not match the shape the requested type needs.
    fileprivate static func malformedPayload(
        _ description: String,
        forKey key: some CodingKey,
        codingPath: [CodingKey]
    ) -> Swift.DecodingError {
        .dataCorrupted(Context(codingPath: codingPath + [key], debugDescription: description))
    }
}
