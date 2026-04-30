//===----------------------------------------------------------------------===//
//
// This source file is part of the Swift Cassandra Client open source project
//
// Copyright (c) 2022-2025 Apple Inc. and the Swift Cassandra Client project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of Swift Cassandra Client project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import Foundation

// MARK: - Encryption helpers for CassandraSession

extension CassandraSession {
    /// Resolve "table" or "keyspace.table" into the registry lookup key and schema.
    @available(macOS 15.0, iOS 18.0, visionOS 2.0, *)
    internal func resolveSchema(
        tableName: String
    ) throws -> CassandraClient.EncryptionSchema {
        let keyspace: String
        let table: String
        if let dotIndex = tableName.firstIndex(of: ".") {
            keyspace = String(tableName[tableName.startIndex..<dotIndex])
            table = String(tableName[tableName.index(after: dotIndex)...])
        } else {
            guard let sessionKeyspace = self.keyspace else {
                throw CassandraClient.Error.encryptionConfigError(
                    "encryptionTable '\(tableName)' has no keyspace qualifier and session has no default keyspace"
                )
            }
            keyspace = sessionKeyspace
            table = tableName
        }
        let registryKey = "\(keyspace).\(table)"
        guard let schema = self.encryptionSchemas[registryKey] else {
            throw CassandraClient.Error.encryptionConfigError(
                "No EncryptionSchema registered for '\(registryKey)'"
            )
        }
        return schema
    }

    /// Validate that encrypted parameter values are bound to columns registered as encrypted.
    ///
    /// For each `.encryptedXxx` value, extracts the column name from its `EncryptionContext`
    /// and checks that column is in the schema's `encryptedColumns` set.
    ///
    /// This catches: encrypted values bound to wrong/unregistered columns (typos, copy-paste errors).
    /// It does NOT catch: plaintext values bound to encrypted columns — that requires prepared statement
    /// metadata to map positional parameters to column names.
    @available(macOS 15.0, iOS 18.0, visionOS 2.0, *)
    func validateEncryptionBindings(
        parameters: [CassandraClient.Statement.Value],
        options: CassandraClient.Statement.Options
    ) throws {
        guard let tableName = options.encryptionTable else { return }
        let schema = try self.resolveSchema(tableName: tableName)

        for value in parameters {
            guard value.isEncrypted, let columnName = value.encryptedColumnName else { continue }
            guard schema.encryptedColumns.contains(columnName) else {
                throw CassandraClient.Error.encryptionConfigError(
                    "Column '\(columnName)' is not registered as encrypted but received an encrypted value"
                )
            }
        }
    }

    /// Build an EncryptionContext.Base from a row using the registered schema.
    /// Reads PK and CK columns as-is from the row, then constructs the full primaryKey.
    @available(macOS 15.0, iOS 18.0, visionOS 2.0, *)
    internal func buildEncryptionContext(
        row: CassandraClient.Row,
        tableName: String,
        encryptor: CassandraClient.Encryptor
    ) throws -> CassandraClient.EncryptionContext.Base {
        let schema = try self.resolveSchema(tableName: tableName)

        var components: [CassandraClient.KeyComponent] = []
        for col in schema.keyColumns {
            let component = try Self.readKeyComponent(from: row, name: col.name, type: col.type)
            components.append(component)
        }

        let primaryKey = CassandraClient.PrimaryKey(from: components)

        return CassandraClient.EncryptionContext.Base(
            keyspace: schema.keyspace,
            table: schema.table,
            primaryKey: primaryKey
        )
    }

    /// Read a cleartext column value from a row and return it as a KeyComponent.
    @available(macOS 15.0, iOS 18.0, visionOS 2.0, *)
    private static func readKeyComponent(
        from row: CassandraClient.Row,
        name: String,
        type: CassandraClient.KeyColumnType
    ) throws -> CassandraClient.KeyComponent {
        if type == .string {
            guard let value: String = row.column(name) else {
                throw CassandraClient.Error.encryptionConfigError(
                    "Column '\(name)' not found or not a string in row"
                )
            }
            return .string(value)
        } else if type == .uuid {
            guard let value: Foundation.UUID = row.column(name) else {
                throw CassandraClient.Error.encryptionConfigError(
                    "Column '\(name)' not found or not a UUID in row"
                )
            }
            return .uuid(value)
        } else if type == .int32 {
            guard let value: Int32 = row.column(name) else {
                throw CassandraClient.Error.encryptionConfigError(
                    "Column '\(name)' not found or not an Int32 in row"
                )
            }
            return .int32(value)
        } else if type == .int64 {
            guard let value: Int64 = row.column(name) else {
                throw CassandraClient.Error.encryptionConfigError(
                    "Column '\(name)' not found or not an Int64 in row"
                )
            }
            return .int64(value)
        } else if type == .data {
            guard let value: [UInt8] = row.column(name) else {
                throw CassandraClient.Error.encryptionConfigError(
                    "Column '\(name)' not found or not bytes in row"
                )
            }
            return .data(Data(value))
        } else if type == .date {
            guard let value: Int64 = row.column(name) else {
                throw CassandraClient.Error.encryptionConfigError(
                    "Column '\(name)' not found or not a timestamp in row"
                )
            }
            return .date(Date(timeIntervalSince1970: Double(value) / 1000.0))
        } else {
            throw CassandraClient.Error.encryptionConfigError(
                "Unsupported key column type '\(type.rawValue)' for column '\(name)'"
            )
        }
    }
}
