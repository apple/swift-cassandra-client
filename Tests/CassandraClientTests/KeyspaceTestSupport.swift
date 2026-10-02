//===----------------------------------------------------------------------===//
//
// This source file is part of the Swift Cassandra Client open source project
//
// Copyright (c) 2026 Apple Inc. and the Swift Cassandra Client project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of Swift Cassandra Client project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

@testable import CassandraClient

/// Create `keyspace` if it does not exist. Connects with no keyspace set, since connecting to a missing
/// keyspace fails.
func createKeyspace(_ keyspace: String, configuration: CassandraClient.Configuration) async throws {
    var configuration = configuration
    configuration.keyspace = nil
    try await CassandraClient.withClient(configuration: configuration) { client in
        try await client.execute(
            "create keyspace if not exists \(keyspace) with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"
        )
    }
}
