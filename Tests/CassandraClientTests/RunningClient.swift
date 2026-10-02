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

import CassandraClient
import NIO

/// A client with `run()` started in a task, for tests that hold a client across `setUp` and `tearDown`,
/// which `withClient` cannot scope. Requests wait until `run()` has connected.
struct RunningClient: Sendable {
    let client: CassandraClient
    private let running: Task<Void, Error>

    init(
        eventLoopGroup: EventLoopGroup = MultiThreadedEventLoopGroup.singleton,
        configuration: CassandraClient.Configuration
    ) {
        let client = CassandraClient(eventLoopGroup: eventLoopGroup, configuration: configuration)
        self.client = client
        self.running = Task { try await client.run() }
    }

    /// Cancel `run()`, which shuts the client down, and wait for it. Rethrows `run()`'s error, such as a
    /// failed connect.
    func stop() async throws {
        self.running.cancel()
        try await self.running.value
    }
}
