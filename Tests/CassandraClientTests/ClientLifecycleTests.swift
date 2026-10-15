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
import NIOConcurrencyHelpers
import XCTest

/// Unit tests for ``CassandraClient/withClient(eventLoopGroup:configuration:_:)`` and ``CassandraClient/run()``.
/// No cluster required: the contact points provider fails, so `run()`'s connect fails without reaching the
/// network, and a request after the failed connect fails with `.disconnected`. The paths that need a connected
/// client (cancellation, graceful shutdown, the metrics poller) are covered by the integration suite.
final class ClientLifecycleTests: XCTestCase {
    private struct BodyError: Error, Equatable {}
    private struct ConnectError: Error, Equatable {}

    private typealias ContactPointsCallback =
        @Sendable (Result<CassandraClient.Configuration.ContactPoints, Swift.Error>) -> Void

    /// Fails the connect once released: until `release()`, the provider holds the callback, so a test can act
    /// while the connect is known to be in flight.
    private final class ConnectGate: Sendable {
        private enum State {
            case waiting(ContactPointsCallback?)
            case released
        }

        private let state = NIOLockedValueBox(State.waiting(nil))

        func provide(_ callback: @escaping ContactPointsCallback) {
            let failNow: Bool = self.state.withLockedValue { state in
                guard case .waiting = state else { return true }
                state = .waiting(callback)
                return false
            }
            if failNow {
                callback(.failure(ConnectError()))
            }
        }

        func release() {
            let callback: ContactPointsCallback? = self.state.withLockedValue { state in
                defer { state = .released }
                guard case .waiting(let callback) = state else { return nil }
                return callback
            }
            callback?(.failure(ConnectError()))
        }
    }

    private static func makeConfiguration(
        contactPointsProvider: @escaping @Sendable (@escaping ContactPointsCallback) -> Void = { callback in
            callback(.failure(ConnectError()))
        }
    ) -> CassandraClient.Configuration {
        CassandraClient.Configuration(
            contactPointsProvider: contactPointsProvider,
            port: 9042,
            protocolVersion: .v3
        )
    }

    private static func assertDisconnected(
        _ client: CassandraClient,
        file: StaticString = #filePath,
        line: UInt = #line
    ) async {
        await assertThrowsErrorAsync(
            try await client.query("select release_version from system.local"),
            file: file,
            line: line
        ) { error in
            XCTAssertEqual(error as? CassandraClient.Error, .disconnected, file: file, line: line)
        }
    }

    private static func assertConnectError(_ error: any Error, file: StaticString = #filePath, line: UInt = #line) {
        XCTAssertEqual(
            error as? ConnectError,
            ConnectError(),
            "expected the connect error, got \(error)",
            file: file,
            line: line
        )
    }

    func testRunThrowsConnectErrorAndShutsClientDown() async {
        let client = CassandraClient(configuration: Self.makeConfiguration())
        await assertThrowsErrorAsync(try await client.run()) { error in
            Self.assertConnectError(error)
        }
        await Self.assertDisconnected(client)
        await assertThrowsErrorAsync(try await client.run()) { error in
            XCTAssertEqual(error as? CassandraClient.Error, .disconnected)
        }
    }

    // A request that is waiting for the connect fails once the connect does, rather than waiting forever.
    func testRequestWaitingForConnectFailsWhenConnectFails() async {
        let gate = ConnectGate()
        let client = CassandraClient(configuration: Self.makeConfiguration(contactPointsProvider: gate.provide))
        let running = Task { try await client.run() }
        let request = Task { try await client.query("select release_version from system.local") }
        gate.release()
        await assertThrowsErrorAsync(try await running.value) { error in
            Self.assertConnectError(error)
        }
        // The request either waited and gets the connect error, or started after the failed connect and gets
        // `.disconnected`; which one depends on scheduling.
        await assertThrowsErrorAsync(try await request.value) { error in
            XCTAssertTrue(
                error as? ConnectError == ConnectError() || error as? CassandraClient.Error == .disconnected,
                "expected the connect error or .disconnected, got \(error)"
            )
        }
    }

    func testWithClientThrowsConnectErrorAfterBodyReturns() async {
        var bodyRan = false
        await assertThrowsErrorAsync(
            try await CassandraClient.withClient(configuration: Self.makeConfiguration()) { _ in
                bodyRan = true
            }
        ) { error in
            Self.assertConnectError(error)
        }
        XCTAssertTrue(bodyRan)
    }

    // An error from the body takes precedence over the connect error from `run()`.
    func testWithClientRethrowsBodyError() async {
        await assertThrowsErrorAsync(
            try await CassandraClient.withClient(configuration: Self.makeConfiguration()) { _ in
                throw BodyError()
            }
        ) { error in
            XCTAssertEqual(error as? BodyError, BodyError())
        }
    }

    func testWithClientShutsClientDown() async {
        var escaped: CassandraClient?
        await assertThrowsErrorAsync(
            try await CassandraClient.withClient(configuration: Self.makeConfiguration()) { client in
                escaped = client
            }
        )
        guard let client = escaped else {
            return XCTFail("body did not run")
        }
        await Self.assertDisconnected(client)
    }

    func testWithClientShutsClientDownAfterBodyThrows() async {
        var escaped: CassandraClient?
        await assertThrowsErrorAsync(
            try await CassandraClient.withClient(configuration: Self.makeConfiguration()) { client in
                escaped = client
                throw BodyError()
            }
        )
        guard let client = escaped else {
            return XCTFail("body did not run")
        }
        await Self.assertDisconnected(client)
    }

    func testRunAfterShutdownThrowsDisconnected() async throws {
        let client = CassandraClient(configuration: Self.makeConfiguration())
        try await client.shutdownAsync()
        await assertThrowsErrorAsync(try await client.run()) { error in
            XCTAssertEqual(error as? CassandraClient.Error, .disconnected)
        }
    }

    // A second `run()` would otherwise wait for a cancellation that, inside `withClient`, never comes. The gate
    // holds `withClient`'s own connect in flight, so the second `run()` finds the run slot taken rather than the
    // client already shut down by a failed connect.
    func testSecondRunThrows() async {
        let gate = ConnectGate()
        let configuration = Self.makeConfiguration(contactPointsProvider: gate.provide)
        await assertThrowsErrorAsync(
            try await CassandraClient.withClient(configuration: configuration) { client in
                await assertThrowsErrorAsync(try await client.run()) { error in
                    XCTAssertEqual(
                        error as? CassandraClient.Error,
                        .invalidState("run() has already been called on this client")
                    )
                }
                gate.release()
            }
        ) { error in
            Self.assertConnectError(error)
        }
    }
}
