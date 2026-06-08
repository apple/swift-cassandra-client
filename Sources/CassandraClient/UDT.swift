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

import Foundation

extension CassandraClient {
    /// A value for a Cassandra user-defined type (UDT) column.
    ///
    /// Field values reuse ``CassandraClient/Statement/Value``, so scalars, collections, and
    /// nested UDTs all compose. The `typeName` must match a UDT registered in the session's
    /// keyspace; its data type is resolved from schema metadata at execution time.
    public struct UDT {
        public let typeName: String
        public let fields: [(name: String, value: Statement.Value)]

        public init(typeName: String, fields: [(String, Statement.Value)]) {
            self.typeName = typeName
            self.fields = fields.map { (name: $0.0, value: $0.1) }
        }
    }
}
