//===----------------------------------------------------------------------===//
//
// This source file is part of the Swift Cassandra Client open source project
//
// Copyright (c) 2025 Apple Inc. and the Swift Cassandra Client project authors
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

extension CassandraClient.Column {
    public var int8Int8Map: [Int8: Int8]? {
        self.toMap(keyType: Int8.self, valueType: Int8.self)
    }

    public var int8Int16Map: [Int8: Int16]? {
        self.toMap(keyType: Int8.self, valueType: Int16.self)
    }

    public var int8Int32Map: [Int8: Int32]? {
        self.toMap(keyType: Int8.self, valueType: Int32.self)
    }

    public var int8Int64Map: [Int8: Int64]? {
        self.toMap(keyType: Int8.self, valueType: Int64.self)
    }

    public var int8Float32Map: [Int8: Float32]? {
        self.toMap(keyType: Int8.self, valueType: Float32.self)
    }

    public var int8DoubleMap: [Int8: Double]? {
        self.toMap(keyType: Int8.self, valueType: Double.self)
    }

    public var int8BoolMap: [Int8: Bool]? {
        self.toMap(keyType: Int8.self, valueType: Bool.self)
    }

    public var int8StringMap: [Int8: String]? {
        self.toMap(keyType: Int8.self, valueType: String.self)
    }

    public var int8UUIDMap: [Int8: Foundation.UUID]? {
        self.toMap(keyType: Int8.self, valueType: Foundation.UUID.self)
    }

    public var int16Int8Map: [Int16: Int8]? {
        self.toMap(keyType: Int16.self, valueType: Int8.self)
    }

    public var int16Int16Map: [Int16: Int16]? {
        self.toMap(keyType: Int16.self, valueType: Int16.self)
    }

    public var int16Int32Map: [Int16: Int32]? {
        self.toMap(keyType: Int16.self, valueType: Int32.self)
    }

    public var int16Int64Map: [Int16: Int64]? {
        self.toMap(keyType: Int16.self, valueType: Int64.self)
    }

    public var int16Float32Map: [Int16: Float32]? {
        self.toMap(keyType: Int16.self, valueType: Float32.self)
    }

    public var int16DoubleMap: [Int16: Double]? {
        self.toMap(keyType: Int16.self, valueType: Double.self)
    }

    public var int16BoolMap: [Int16: Bool]? {
        self.toMap(keyType: Int16.self, valueType: Bool.self)
    }

    public var int16StringMap: [Int16: String]? {
        self.toMap(keyType: Int16.self, valueType: String.self)
    }

    public var int16UUIDMap: [Int16: Foundation.UUID]? {
        self.toMap(keyType: Int16.self, valueType: Foundation.UUID.self)
    }

    public var int32Int8Map: [Int32: Int8]? {
        self.toMap(keyType: Int32.self, valueType: Int8.self)
    }

    public var int32Int16Map: [Int32: Int16]? {
        self.toMap(keyType: Int32.self, valueType: Int16.self)
    }

    public var int32Int32Map: [Int32: Int32]? {
        self.toMap(keyType: Int32.self, valueType: Int32.self)
    }

    public var int32Int64Map: [Int32: Int64]? {
        self.toMap(keyType: Int32.self, valueType: Int64.self)
    }

    public var int32Float32Map: [Int32: Float32]? {
        self.toMap(keyType: Int32.self, valueType: Float32.self)
    }

    public var int32DoubleMap: [Int32: Double]? {
        self.toMap(keyType: Int32.self, valueType: Double.self)
    }

    public var int32BoolMap: [Int32: Bool]? {
        self.toMap(keyType: Int32.self, valueType: Bool.self)
    }

    public var int32StringMap: [Int32: String]? {
        self.toMap(keyType: Int32.self, valueType: String.self)
    }

    public var int32UUIDMap: [Int32: Foundation.UUID]? {
        self.toMap(keyType: Int32.self, valueType: Foundation.UUID.self)
    }

    public var int64Int8Map: [Int64: Int8]? {
        self.toMap(keyType: Int64.self, valueType: Int8.self)
    }

    public var int64Int16Map: [Int64: Int16]? {
        self.toMap(keyType: Int64.self, valueType: Int16.self)
    }

    public var int64Int32Map: [Int64: Int32]? {
        self.toMap(keyType: Int64.self, valueType: Int32.self)
    }

    public var int64Int64Map: [Int64: Int64]? {
        self.toMap(keyType: Int64.self, valueType: Int64.self)
    }

    public var int64Float32Map: [Int64: Float32]? {
        self.toMap(keyType: Int64.self, valueType: Float32.self)
    }

    public var int64DoubleMap: [Int64: Double]? {
        self.toMap(keyType: Int64.self, valueType: Double.self)
    }

    public var int64BoolMap: [Int64: Bool]? {
        self.toMap(keyType: Int64.self, valueType: Bool.self)
    }

    public var int64StringMap: [Int64: String]? {
        self.toMap(keyType: Int64.self, valueType: String.self)
    }

    public var int64UUIDMap: [Int64: Foundation.UUID]? {
        self.toMap(keyType: Int64.self, valueType: Foundation.UUID.self)
    }

    public var stringInt8Map: [String: Int8]? {
        self.toMap(keyType: String.self, valueType: Int8.self)
    }

    public var stringInt16Map: [String: Int16]? {
        self.toMap(keyType: String.self, valueType: Int16.self)
    }

    public var stringInt32Map: [String: Int32]? {
        self.toMap(keyType: String.self, valueType: Int32.self)
    }

    public var stringInt64Map: [String: Int64]? {
        self.toMap(keyType: String.self, valueType: Int64.self)
    }

    public var stringFloat32Map: [String: Float32]? {
        self.toMap(keyType: String.self, valueType: Float32.self)
    }

    public var stringDoubleMap: [String: Double]? {
        self.toMap(keyType: String.self, valueType: Double.self)
    }

    public var stringBoolMap: [String: Bool]? {
        self.toMap(keyType: String.self, valueType: Bool.self)
    }

    public var stringStringMap: [String: String]? {
        self.toMap(keyType: String.self, valueType: String.self)
    }

    public var stringUUIDMap: [String: Foundation.UUID]? {
        self.toMap(keyType: String.self, valueType: Foundation.UUID.self)
    }

    public var uuidInt8Map: [Foundation.UUID: Int8]? {
        self.toMap(keyType: Foundation.UUID.self, valueType: Int8.self)
    }

    public var uuidInt16Map: [Foundation.UUID: Int16]? {
        self.toMap(keyType: Foundation.UUID.self, valueType: Int16.self)
    }

    public var uuidInt32Map: [Foundation.UUID: Int32]? {
        self.toMap(keyType: Foundation.UUID.self, valueType: Int32.self)
    }

    public var uuidInt64Map: [Foundation.UUID: Int64]? {
        self.toMap(keyType: Foundation.UUID.self, valueType: Int64.self)
    }

    public var uuidFloat32Map: [Foundation.UUID: Float32]? {
        self.toMap(keyType: Foundation.UUID.self, valueType: Float32.self)
    }

    public var uuidDoubleMap: [Foundation.UUID: Double]? {
        self.toMap(keyType: Foundation.UUID.self, valueType: Double.self)
    }

    public var uuidBoolMap: [Foundation.UUID: Bool]? {
        self.toMap(keyType: Foundation.UUID.self, valueType: Bool.self)
    }

    public var uuidStringMap: [Foundation.UUID: String]? {
        self.toMap(keyType: Foundation.UUID.self, valueType: String.self)
    }

    public var uuidUUIDMap: [Foundation.UUID: Foundation.UUID]? {
        self.toMap(keyType: Foundation.UUID.self, valueType: Foundation.UUID.self)
    }

    public var timeuuidInt8Map: [TimeBasedUUID: Int8]? {
        self.toMap(keyType: TimeBasedUUID.self, valueType: Int8.self)
    }

    public var timeuuidInt16Map: [TimeBasedUUID: Int16]? {
        self.toMap(keyType: TimeBasedUUID.self, valueType: Int16.self)
    }

    public var timeuuidInt32Map: [TimeBasedUUID: Int32]? {
        self.toMap(keyType: TimeBasedUUID.self, valueType: Int32.self)
    }

    public var timeuuidInt64Map: [TimeBasedUUID: Int64]? {
        self.toMap(keyType: TimeBasedUUID.self, valueType: Int64.self)
    }

    public var timeuuidFloat32Map: [TimeBasedUUID: Float32]? {
        self.toMap(keyType: TimeBasedUUID.self, valueType: Float32.self)
    }

    public var timeuuidDoubleMap: [TimeBasedUUID: Double]? {
        self.toMap(keyType: TimeBasedUUID.self, valueType: Double.self)
    }

    public var timeuuidBoolMap: [TimeBasedUUID: Bool]? {
        self.toMap(keyType: TimeBasedUUID.self, valueType: Bool.self)
    }

    public var timeuuidStringMap: [TimeBasedUUID: String]? {
        self.toMap(keyType: TimeBasedUUID.self, valueType: String.self)
    }

    public var timeuuidUUIDMap: [TimeBasedUUID: Foundation.UUID]? {
        self.toMap(keyType: TimeBasedUUID.self, valueType: Foundation.UUID.self)
    }

    private func toMap<K: Hashable, V>(keyType _: K.Type, valueType _: V.Type) -> [K: V]? {
        guard cass_value_is_null(self.rawPointer) == cass_false else {
            return nil
        }

        var map: [K: V] = [:]

        let iterator = cass_iterator_from_map(self.rawPointer)
        defer { cass_iterator_free(iterator) }

        while cass_iterator_next(iterator) == cass_true {
            let keyPointer = cass_iterator_get_map_key(iterator)
            let valuePointer = cass_iterator_get_map_value(iterator)

            guard let key = self.extractValue(from: keyPointer, as: K.self),
                let value = self.extractValue(from: valuePointer, as: V.self)
            else {
                continue
            }

            map[key] = value
        }

        return map.isEmpty ? nil : map
    }

    private func extractValue<T>(from pointer: OpaquePointer?, as type: T.Type) -> T? {
        guard let pointer = pointer else {
            return nil
        }

        switch type {
        case is Int8.Type:
            var value: Int8 = 0
            let error = cass_value_get_int8(pointer, &value)
            return error == CASS_OK ? value as? T : nil
        case is Int16.Type:
            var value: Int16 = 0
            let error = cass_value_get_int16(pointer, &value)
            return error == CASS_OK ? value as? T : nil
        case is Int32.Type:
            var value: Int32 = 0
            let error = cass_value_get_int32(pointer, &value)
            return error == CASS_OK ? value as? T : nil
        case is Int64.Type:
            var value: Int64 = 0
            let error = cass_value_get_int64(pointer, &value)
            return error == CASS_OK ? value as? T : nil
        case is Float32.Type:
            var value: Float32 = 0
            let error = cass_value_get_float(pointer, &value)
            return error == CASS_OK ? value as? T : nil
        case is Double.Type:
            var value: Double = 0
            let error = cass_value_get_double(pointer, &value)
            return error == CASS_OK ? value as? T : nil
        case is Bool.Type:
            var value = cass_bool_t(0)
            let error = cass_value_get_bool(pointer, &value)
            return error == CASS_OK ? (value == cass_true) as? T : nil
        case is String.Type:
            var stringPtr: UnsafePointer<CChar>?
            var stringSize = 0
            let error = cass_value_get_string(pointer, &stringPtr, &stringSize)
            guard let ptr = stringPtr, error == CASS_OK else {
                return nil
            }
            let buffer = UnsafeBufferPointer(start: ptr, count: stringSize)
            return buffer.withMemoryRebound(to: UInt8.self) {
                String(decoding: $0, as: UTF8.self) as? T
            }
        case is Foundation.UUID.Type:
            var value = CassUuid()
            let error = cass_value_get_uuid(pointer, &value)
            return error == CASS_OK ? value.uuid() as? T : nil
        case is TimeBasedUUID.Type:
            var value = CassUuid()
            let error = cass_value_get_uuid(pointer, &value)
            return error == CASS_OK ? TimeBasedUUID(uuid: value) as? T : nil
        default:
            return nil
        }
    }
}

extension CassandraClient.Row {
    public func column(_ name: String) -> [Int8: Int8]? {
        self.column(name)?.int8Int8Map
    }

    public func column(_ index: Int) -> [Int8: Int8]? {
        self.column(index)?.int8Int8Map
    }

    public func column(_ name: String) -> [Int8: Int16]? {
        self.column(name)?.int8Int16Map
    }

    public func column(_ index: Int) -> [Int8: Int16]? {
        self.column(index)?.int8Int16Map
    }

    public func column(_ name: String) -> [Int8: Int32]? {
        self.column(name)?.int8Int32Map
    }

    public func column(_ index: Int) -> [Int8: Int32]? {
        self.column(index)?.int8Int32Map
    }

    public func column(_ name: String) -> [Int8: Int64]? {
        self.column(name)?.int8Int64Map
    }

    public func column(_ index: Int) -> [Int8: Int64]? {
        self.column(index)?.int8Int64Map
    }

    public func column(_ name: String) -> [Int8: Float32]? {
        self.column(name)?.int8Float32Map
    }

    public func column(_ index: Int) -> [Int8: Float32]? {
        self.column(index)?.int8Float32Map
    }

    public func column(_ name: String) -> [Int8: Double]? {
        self.column(name)?.int8DoubleMap
    }

    public func column(_ index: Int) -> [Int8: Double]? {
        self.column(index)?.int8DoubleMap
    }

    public func column(_ name: String) -> [Int8: Bool]? {
        self.column(name)?.int8BoolMap
    }

    public func column(_ index: Int) -> [Int8: Bool]? {
        self.column(index)?.int8BoolMap
    }

    public func column(_ name: String) -> [Int8: String]? {
        self.column(name)?.int8StringMap
    }

    public func column(_ index: Int) -> [Int8: String]? {
        self.column(index)?.int8StringMap
    }

    public func column(_ name: String) -> [Int8: Foundation.UUID]? {
        self.column(name)?.int8UUIDMap
    }

    public func column(_ index: Int) -> [Int8: Foundation.UUID]? {
        self.column(index)?.int8UUIDMap
    }

    public func column(_ name: String) -> [Int16: Int8]? {
        self.column(name)?.int16Int8Map
    }

    public func column(_ index: Int) -> [Int16: Int8]? {
        self.column(index)?.int16Int8Map
    }

    public func column(_ name: String) -> [Int16: Int16]? {
        self.column(name)?.int16Int16Map
    }

    public func column(_ index: Int) -> [Int16: Int16]? {
        self.column(index)?.int16Int16Map
    }

    public func column(_ name: String) -> [Int16: Int32]? {
        self.column(name)?.int16Int32Map
    }

    public func column(_ index: Int) -> [Int16: Int32]? {
        self.column(index)?.int16Int32Map
    }

    public func column(_ name: String) -> [Int16: Int64]? {
        self.column(name)?.int16Int64Map
    }

    public func column(_ index: Int) -> [Int16: Int64]? {
        self.column(index)?.int16Int64Map
    }

    public func column(_ name: String) -> [Int16: Float32]? {
        self.column(name)?.int16Float32Map
    }

    public func column(_ index: Int) -> [Int16: Float32]? {
        self.column(index)?.int16Float32Map
    }

    public func column(_ name: String) -> [Int16: Double]? {
        self.column(name)?.int16DoubleMap
    }

    public func column(_ index: Int) -> [Int16: Double]? {
        self.column(index)?.int16DoubleMap
    }

    public func column(_ name: String) -> [Int16: Bool]? {
        self.column(name)?.int16BoolMap
    }

    public func column(_ index: Int) -> [Int16: Bool]? {
        self.column(index)?.int16BoolMap
    }

    public func column(_ name: String) -> [Int16: String]? {
        self.column(name)?.int16StringMap
    }

    public func column(_ index: Int) -> [Int16: String]? {
        self.column(index)?.int16StringMap
    }

    public func column(_ name: String) -> [Int16: Foundation.UUID]? {
        self.column(name)?.int16UUIDMap
    }

    public func column(_ index: Int) -> [Int16: Foundation.UUID]? {
        self.column(index)?.int16UUIDMap
    }

    public func column(_ name: String) -> [Int32: Int8]? {
        self.column(name)?.int32Int8Map
    }

    public func column(_ index: Int) -> [Int32: Int8]? {
        self.column(index)?.int32Int8Map
    }

    public func column(_ name: String) -> [Int32: Int16]? {
        self.column(name)?.int32Int16Map
    }

    public func column(_ index: Int) -> [Int32: Int16]? {
        self.column(index)?.int32Int16Map
    }

    public func column(_ name: String) -> [Int32: Int32]? {
        self.column(name)?.int32Int32Map
    }

    public func column(_ index: Int) -> [Int32: Int32]? {
        self.column(index)?.int32Int32Map
    }

    public func column(_ name: String) -> [Int32: Int64]? {
        self.column(name)?.int32Int64Map
    }

    public func column(_ index: Int) -> [Int32: Int64]? {
        self.column(index)?.int32Int64Map
    }

    public func column(_ name: String) -> [Int32: Float32]? {
        self.column(name)?.int32Float32Map
    }

    public func column(_ index: Int) -> [Int32: Float32]? {
        self.column(index)?.int32Float32Map
    }

    public func column(_ name: String) -> [Int32: Double]? {
        self.column(name)?.int32DoubleMap
    }

    public func column(_ index: Int) -> [Int32: Double]? {
        self.column(index)?.int32DoubleMap
    }

    public func column(_ name: String) -> [Int32: Bool]? {
        self.column(name)?.int32BoolMap
    }

    public func column(_ index: Int) -> [Int32: Bool]? {
        self.column(index)?.int32BoolMap
    }

    public func column(_ name: String) -> [Int32: String]? {
        self.column(name)?.int32StringMap
    }

    public func column(_ index: Int) -> [Int32: String]? {
        self.column(index)?.int32StringMap
    }

    public func column(_ name: String) -> [Int32: Foundation.UUID]? {
        self.column(name)?.int32UUIDMap
    }

    public func column(_ index: Int) -> [Int32: Foundation.UUID]? {
        self.column(index)?.int32UUIDMap
    }

    public func column(_ name: String) -> [Int64: Int8]? {
        self.column(name)?.int64Int8Map
    }

    public func column(_ index: Int) -> [Int64: Int8]? {
        self.column(index)?.int64Int8Map
    }

    public func column(_ name: String) -> [Int64: Int16]? {
        self.column(name)?.int64Int16Map
    }

    public func column(_ index: Int) -> [Int64: Int16]? {
        self.column(index)?.int64Int16Map
    }

    public func column(_ name: String) -> [Int64: Int32]? {
        self.column(name)?.int64Int32Map
    }

    public func column(_ index: Int) -> [Int64: Int32]? {
        self.column(index)?.int64Int32Map
    }

    public func column(_ name: String) -> [Int64: Int64]? {
        self.column(name)?.int64Int64Map
    }

    public func column(_ index: Int) -> [Int64: Int64]? {
        self.column(index)?.int64Int64Map
    }

    public func column(_ name: String) -> [Int64: Float32]? {
        self.column(name)?.int64Float32Map
    }

    public func column(_ index: Int) -> [Int64: Float32]? {
        self.column(index)?.int64Float32Map
    }

    public func column(_ name: String) -> [Int64: Double]? {
        self.column(name)?.int64DoubleMap
    }

    public func column(_ index: Int) -> [Int64: Double]? {
        self.column(index)?.int64DoubleMap
    }

    public func column(_ name: String) -> [Int64: Bool]? {
        self.column(name)?.int64BoolMap
    }

    public func column(_ index: Int) -> [Int64: Bool]? {
        self.column(index)?.int64BoolMap
    }

    public func column(_ name: String) -> [Int64: String]? {
        self.column(name)?.int64StringMap
    }

    public func column(_ index: Int) -> [Int64: String]? {
        self.column(index)?.int64StringMap
    }

    public func column(_ name: String) -> [Int64: Foundation.UUID]? {
        self.column(name)?.int64UUIDMap
    }

    public func column(_ index: Int) -> [Int64: Foundation.UUID]? {
        self.column(index)?.int64UUIDMap
    }

    public func column(_ name: String) -> [String: Int8]? {
        self.column(name)?.stringInt8Map
    }

    public func column(_ index: Int) -> [String: Int8]? {
        self.column(index)?.stringInt8Map
    }

    public func column(_ name: String) -> [String: Int16]? {
        self.column(name)?.stringInt16Map
    }

    public func column(_ index: Int) -> [String: Int16]? {
        self.column(index)?.stringInt16Map
    }

    public func column(_ name: String) -> [String: Int32]? {
        self.column(name)?.stringInt32Map
    }

    public func column(_ index: Int) -> [String: Int32]? {
        self.column(index)?.stringInt32Map
    }

    public func column(_ name: String) -> [String: Int64]? {
        self.column(name)?.stringInt64Map
    }

    public func column(_ index: Int) -> [String: Int64]? {
        self.column(index)?.stringInt64Map
    }

    public func column(_ name: String) -> [String: Float32]? {
        self.column(name)?.stringFloat32Map
    }

    public func column(_ index: Int) -> [String: Float32]? {
        self.column(index)?.stringFloat32Map
    }

    public func column(_ name: String) -> [String: Double]? {
        self.column(name)?.stringDoubleMap
    }

    public func column(_ index: Int) -> [String: Double]? {
        self.column(index)?.stringDoubleMap
    }

    public func column(_ name: String) -> [String: Bool]? {
        self.column(name)?.stringBoolMap
    }

    public func column(_ index: Int) -> [String: Bool]? {
        self.column(index)?.stringBoolMap
    }

    public func column(_ name: String) -> [String: String]? {
        self.column(name)?.stringStringMap
    }

    public func column(_ index: Int) -> [String: String]? {
        self.column(index)?.stringStringMap
    }

    public func column(_ name: String) -> [String: Foundation.UUID]? {
        self.column(name)?.stringUUIDMap
    }

    public func column(_ index: Int) -> [String: Foundation.UUID]? {
        self.column(index)?.stringUUIDMap
    }

    public func column(_ name: String) -> [Foundation.UUID: Int8]? {
        self.column(name)?.uuidInt8Map
    }

    public func column(_ index: Int) -> [Foundation.UUID: Int8]? {
        self.column(index)?.uuidInt8Map
    }

    public func column(_ name: String) -> [Foundation.UUID: Int16]? {
        self.column(name)?.uuidInt16Map
    }

    public func column(_ index: Int) -> [Foundation.UUID: Int16]? {
        self.column(index)?.uuidInt16Map
    }

    public func column(_ name: String) -> [Foundation.UUID: Int32]? {
        self.column(name)?.uuidInt32Map
    }

    public func column(_ index: Int) -> [Foundation.UUID: Int32]? {
        self.column(index)?.uuidInt32Map
    }

    public func column(_ name: String) -> [Foundation.UUID: Int64]? {
        self.column(name)?.uuidInt64Map
    }

    public func column(_ index: Int) -> [Foundation.UUID: Int64]? {
        self.column(index)?.uuidInt64Map
    }

    public func column(_ name: String) -> [Foundation.UUID: Float32]? {
        self.column(name)?.uuidFloat32Map
    }

    public func column(_ index: Int) -> [Foundation.UUID: Float32]? {
        self.column(index)?.uuidFloat32Map
    }

    public func column(_ name: String) -> [Foundation.UUID: Double]? {
        self.column(name)?.uuidDoubleMap
    }

    public func column(_ index: Int) -> [Foundation.UUID: Double]? {
        self.column(index)?.uuidDoubleMap
    }

    public func column(_ name: String) -> [Foundation.UUID: Bool]? {
        self.column(name)?.uuidBoolMap
    }

    public func column(_ index: Int) -> [Foundation.UUID: Bool]? {
        self.column(index)?.uuidBoolMap
    }

    public func column(_ name: String) -> [Foundation.UUID: String]? {
        self.column(name)?.uuidStringMap
    }

    public func column(_ index: Int) -> [Foundation.UUID: String]? {
        self.column(index)?.uuidStringMap
    }

    public func column(_ name: String) -> [Foundation.UUID: Foundation.UUID]? {
        self.column(name)?.uuidUUIDMap
    }

    public func column(_ index: Int) -> [Foundation.UUID: Foundation.UUID]? {
        self.column(index)?.uuidUUIDMap
    }

    public func column(_ name: String) -> [TimeBasedUUID: Int8]? {
        self.column(name)?.timeuuidInt8Map
    }

    public func column(_ index: Int) -> [TimeBasedUUID: Int8]? {
        self.column(index)?.timeuuidInt8Map
    }

    public func column(_ name: String) -> [TimeBasedUUID: Int16]? {
        self.column(name)?.timeuuidInt16Map
    }

    public func column(_ index: Int) -> [TimeBasedUUID: Int16]? {
        self.column(index)?.timeuuidInt16Map
    }

    public func column(_ name: String) -> [TimeBasedUUID: Int32]? {
        self.column(name)?.timeuuidInt32Map
    }

    public func column(_ index: Int) -> [TimeBasedUUID: Int32]? {
        self.column(index)?.timeuuidInt32Map
    }

    public func column(_ name: String) -> [TimeBasedUUID: Int64]? {
        self.column(name)?.timeuuidInt64Map
    }

    public func column(_ index: Int) -> [TimeBasedUUID: Int64]? {
        self.column(index)?.timeuuidInt64Map
    }

    public func column(_ name: String) -> [TimeBasedUUID: Float32]? {
        self.column(name)?.timeuuidFloat32Map
    }

    public func column(_ index: Int) -> [TimeBasedUUID: Float32]? {
        self.column(index)?.timeuuidFloat32Map
    }

    public func column(_ name: String) -> [TimeBasedUUID: Double]? {
        self.column(name)?.timeuuidDoubleMap
    }

    public func column(_ index: Int) -> [TimeBasedUUID: Double]? {
        self.column(index)?.timeuuidDoubleMap
    }

    public func column(_ name: String) -> [TimeBasedUUID: Bool]? {
        self.column(name)?.timeuuidBoolMap
    }

    public func column(_ index: Int) -> [TimeBasedUUID: Bool]? {
        self.column(index)?.timeuuidBoolMap
    }

    public func column(_ name: String) -> [TimeBasedUUID: String]? {
        self.column(name)?.timeuuidStringMap
    }

    public func column(_ index: Int) -> [TimeBasedUUID: String]? {
        self.column(index)?.timeuuidStringMap
    }

    public func column(_ name: String) -> [TimeBasedUUID: Foundation.UUID]? {
        self.column(name)?.timeuuidUUIDMap
    }

    public func column(_ index: Int) -> [TimeBasedUUID: Foundation.UUID]? {
        self.column(index)?.timeuuidUUIDMap
    }
}
