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
import XCTest

/// `XCTAssertThrowsError` for an `async` expression, which its autoclosure cannot await.
func assertThrowsErrorAsync<T>(
    _ expression: @autoclosure () async throws -> T,
    _ message: @autoclosure () -> String = "",
    file: StaticString = #filePath,
    line: UInt = #line,
    _ errorHandler: (_ error: any Error) -> Void = { _ in }
) async {
    do {
        _ = try await expression()
        let message = message()
        XCTFail(message.isEmpty ? "expected an error" : "expected an error: \(message)", file: file, line: line)
    } catch {
        errorHandler(error)
    }
}

/// Waits until `client` rejects requests with `.disconnected`, which is how a stop request ends `run()`.
func waitUntilDisconnected(
    _ client: CassandraClient,
    timeout: Duration = .seconds(10),
    file: StaticString = #filePath,
    line: UInt = #line
) async throws {
    let deadline = ContinuousClock.now + timeout
    while ContinuousClock.now < deadline {
        do {
            _ = try await client.query("select release_version from system.local")
        } catch let error as CassandraClient.Error where error == .disconnected {
            return
        }
        try await Task.sleep(for: .milliseconds(20))
    }
    XCTFail("the client was still connected after \(timeout)", file: file, line: line)
}
