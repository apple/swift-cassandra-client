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

extension CassandraClient.Statement {
    func bindMapCases(_ parameter: Value, _ index: Int) throws -> CassError {
        switch parameter {
        case .int8Int8Map(let map):
            return try self.bindMap(map, at: index)
        case .int8Int16Map(let map):
            return try self.bindMap(map, at: index)
        case .int8Int32Map(let map):
            return try self.bindMap(map, at: index)
        case .int8Int64Map(let map):
            return try self.bindMap(map, at: index)
        case .int8Float32Map(let map):
            return try self.bindMap(map, at: index)
        case .int8DoubleMap(let map):
            return try self.bindMap(map, at: index)
        case .int8BoolMap(let map):
            return try self.bindMap(map, at: index)
        case .int8StringMap(let map):
            return try self.bindMap(map, at: index)
        case .int8UUIDMap(let map):
            return try self.bindMap(map, at: index)

        case .int16Int8Map(let map):
            return try self.bindMap(map, at: index)
        case .int16Int16Map(let map):
            return try self.bindMap(map, at: index)
        case .int16Int32Map(let map):
            return try self.bindMap(map, at: index)
        case .int16Int64Map(let map):
            return try self.bindMap(map, at: index)
        case .int16Float32Map(let map):
            return try self.bindMap(map, at: index)
        case .int16DoubleMap(let map):
            return try self.bindMap(map, at: index)
        case .int16BoolMap(let map):
            return try self.bindMap(map, at: index)
        case .int16StringMap(let map):
            return try self.bindMap(map, at: index)
        case .int16UUIDMap(let map):
            return try self.bindMap(map, at: index)

        case .int32Int8Map(let map):
            return try self.bindMap(map, at: index)
        case .int32Int16Map(let map):
            return try self.bindMap(map, at: index)
        case .int32Int32Map(let map):
            return try self.bindMap(map, at: index)
        case .int32Int64Map(let map):
            return try self.bindMap(map, at: index)
        case .int32Float32Map(let map):
            return try self.bindMap(map, at: index)
        case .int32DoubleMap(let map):
            return try self.bindMap(map, at: index)
        case .int32BoolMap(let map):
            return try self.bindMap(map, at: index)
        case .int32StringMap(let map):
            return try self.bindMap(map, at: index)
        case .int32UUIDMap(let map):
            return try self.bindMap(map, at: index)

        case .int64Int8Map(let map):
            return try self.bindMap(map, at: index)
        case .int64Int16Map(let map):
            return try self.bindMap(map, at: index)
        case .int64Int32Map(let map):
            return try self.bindMap(map, at: index)
        case .int64Int64Map(let map):
            return try self.bindMap(map, at: index)
        case .int64Float32Map(let map):
            return try self.bindMap(map, at: index)
        case .int64DoubleMap(let map):
            return try self.bindMap(map, at: index)
        case .int64BoolMap(let map):
            return try self.bindMap(map, at: index)
        case .int64StringMap(let map):
            return try self.bindMap(map, at: index)
        case .int64UUIDMap(let map):
            return try self.bindMap(map, at: index)

        case .stringInt8Map(let map):
            return try self.bindMap(map, at: index)
        case .stringInt16Map(let map):
            return try self.bindMap(map, at: index)
        case .stringInt32Map(let map):
            return try self.bindMap(map, at: index)
        case .stringInt64Map(let map):
            return try self.bindMap(map, at: index)
        case .stringFloat32Map(let map):
            return try self.bindMap(map, at: index)
        case .stringDoubleMap(let map):
            return try self.bindMap(map, at: index)
        case .stringBoolMap(let map):
            return try self.bindMap(map, at: index)
        case .stringStringMap(let map):
            return try self.bindMap(map, at: index)
        case .stringUUIDMap(let map):
            return try self.bindMap(map, at: index)

        case .uuidInt8Map(let map):
            return try self.bindMap(map, at: index)
        case .uuidInt16Map(let map):
            return try self.bindMap(map, at: index)
        case .uuidInt32Map(let map):
            return try self.bindMap(map, at: index)
        case .uuidInt64Map(let map):
            return try self.bindMap(map, at: index)
        case .uuidFloat32Map(let map):
            return try self.bindMap(map, at: index)
        case .uuidDoubleMap(let map):
            return try self.bindMap(map, at: index)
        case .uuidBoolMap(let map):
            return try self.bindMap(map, at: index)
        case .uuidStringMap(let map):
            return try self.bindMap(map, at: index)
        case .uuidUUIDMap(let map):
            return try self.bindMap(map, at: index)

        case .timeuuidInt8Map(let map):
            return try self.bindMap(map, at: index)
        case .timeuuidInt16Map(let map):
            return try self.bindMap(map, at: index)
        case .timeuuidInt32Map(let map):
            return try self.bindMap(map, at: index)
        case .timeuuidInt64Map(let map):
            return try self.bindMap(map, at: index)
        case .timeuuidFloat32Map(let map):
            return try self.bindMap(map, at: index)
        case .timeuuidDoubleMap(let map):
            return try self.bindMap(map, at: index)
        case .timeuuidBoolMap(let map):
            return try self.bindMap(map, at: index)
        case .timeuuidStringMap(let map):
            return try self.bindMap(map, at: index)
        case .timeuuidUUIDMap(let map):
            return try self.bindMap(map, at: index)

        default:
            fatalError("Not a map case")
        }
    }

    private func bindMap<K, V>(_ map: [K: V], at index: Int) throws -> CassError {
        let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, map.count * 2)
        defer { cass_collection_free(collection) }

        for (key, value) in map {
            let keyResult = try appendToCollection(collection, element: key)
            guard keyResult == CASS_OK else {
                throw CassandraClient.Error(keyResult)
            }

            let valueResult = try appendToCollection(collection, element: value)
            guard valueResult == CASS_OK else {
                throw CassandraClient.Error(valueResult)
            }
        }

        return cass_statement_bind_collection(self.rawPointer, index, collection)
    }

    private func appendToCollection<T>(_ collection: OpaquePointer?, element: T) throws -> CassError {
        switch element {
        case let value as Int8:
            return cass_collection_append_int8(collection, value)
        case let value as Int16:
            return cass_collection_append_int16(collection, value)
        case let value as Int32:
            return cass_collection_append_int32(collection, value)
        case let value as Int64:
            return cass_collection_append_int64(collection, value)
        case let value as Float32:
            return cass_collection_append_float(collection, value)
        case let value as Double:
            return cass_collection_append_double(collection, value)
        case let value as Bool:
            return cass_collection_append_bool(collection, value ? cass_true : cass_false)
        case let value as String:
            return cass_collection_append_string(collection, value)
        case let value as Foundation.UUID:
            let uuid = CassUuid(value.uuid)
            return cass_collection_append_uuid(collection, uuid)
        case let value as TimeBasedUUID:
            let timeuuid = CassUuid(value.uuid)
            return cass_collection_append_uuid(collection, timeuuid)
        default:
            throw CassandraClient.Error.badParams("Map element of type \(T.self) is not supported")
        }
    }
}
