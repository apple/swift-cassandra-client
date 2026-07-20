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

// MARK: - Centralised log-metadata-key constants

extension CassandraClient {
    /// Structured-log metadata key names for request logging. Follows the repo's `<domain>.<thing>`
    /// convention (matching ``EncryptionLogKey``).
    internal enum LogKey {
        static let query = "request.query"
        static let consistency = "request.consistency"
        static let errorCategory = "request.errorCategory"
        static let latencyMs = "request.latencyMs"
        static let boundValues = "request.boundValues"
    }

    /// Structured-log metadata key names for encryption. Values are unchanged from their original
    /// definition (shipped keys — consumers may filter on them), just relocated here (by-kind).
    internal enum EncryptionLogKey {
        static let keyName = "encryption.keyName"
        static let column = "encryption.column"
        static let keyRotationFrom = "encryption.keyRotation.from"
        static let keyRotationTo = "encryption.keyRotation.to"
        static let keysLoaded = "encryption.keysLoaded"
        static let rowsDecrypted = "encryption.rowsDecrypted"
    }
}
