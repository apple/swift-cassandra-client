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

// MARK: - Centralised encryption string constants

extension CassandraClient {
    internal enum EncryptionLogKey {
        static let keyName = "encryption.keyName"
        static let column = "encryption.column"
        static let keyRotationFrom = "encryption.keyRotation.from"
        static let keyRotationTo = "encryption.keyRotation.to"
        static let keysLoaded = "encryption.keysLoaded"
        static let rowsDecrypted = "encryption.rowsDecrypted"
    }

    internal enum EncryptionMetric {
        static let encryptTotal = "cassandra.encryption.encrypt.total"
        static let encryptDuration = "cassandra.encryption.encrypt.duration"
        static let decryptTotal = "cassandra.encryption.decrypt.total"
        static let decryptDuration = "cassandra.encryption.decrypt.duration"
        static let decryptFailures = "cassandra.encryption.decrypt.failures"
        static let keyRotationTotal = "cassandra.encryption.key_rotation.total"
        static let dimensionKeyName = "keyName"
    }
}
