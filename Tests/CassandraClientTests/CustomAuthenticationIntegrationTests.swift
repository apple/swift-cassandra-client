//===----------------------------------------------------------------------===//
//
// This source file is part of the Swift Cassandra Client open source project
//
// Copyright (c) 2022 Apple Inc. and the Swift Cassandra Client project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of Swift Cassandra Client project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import Foundation
import Logging
import NIO
import NIOConcurrencyHelpers
import XCTest

@testable import CassandraClient

/// Custom-authenticator INTEGRATION tests — need a live Cassandra that enforces `PasswordAuthenticator`:
///
///     CASSANDRA_REQUIRE_AUTH=1 CASSANDRA_HOST=<host> swift test --filter CustomAuthenticationIntegrationTests
///
/// Credentials come from `CASSANDRA_USER`/`CASSANDRA_PASSWORD` (default `cassandra`/`cassandra`). Against a
/// default `AllowAllAuthenticator` cluster the callbacks never fire, so all but the teardown test are gated
/// behind `CASSANDRA_REQUIRE_AUTH` and `XCTSkip` when it is unset. The flag is set out-of-band, not probed,
/// because `testClusterEnforcesAuthentication` is itself the probe.
final class CustomAuthenticationIntegrationTests: XCTestCase {
    private var environment: [String: String] { ProcessInfo.processInfo.environment }
    private var validUsername: String { self.environment["CASSANDRA_USER"] ?? "cassandra" }
    private var validPassword: String { self.environment["CASSANDRA_PASSWORD"] ?? "cassandra" }

    /// Skips a test unless `CASSANDRA_REQUIRE_AUTH` is set to a non-empty value, i.e. the caller asserts the
    /// target cluster enforces `PasswordAuthenticator`. See the type doc for why this is opt-in, not probed.
    private func requireAuthEnforcement() throws {
        guard let value = self.environment["CASSANDRA_REQUIRE_AUTH"], !value.isEmpty else {
            throw XCTSkip(
                "set CASSANDRA_REQUIRE_AUTH=1 against an auth-enforcing cluster to run this test"
            )
        }
    }

    /// A config that authenticates only via the custom-authenticator path: no `username`/`password` and no
    /// keyspace (tests query `system.local`). Callers set `.authenticator` (or leave it nil for the
    /// enforcement-guard test).
    private func makeConfiguration() -> CassandraClient.Configuration {
        var configuration = CassandraClient.Configuration(
            contactPointsProvider: { callback in
                callback(.success([self.environment["CASSANDRA_HOST"] ?? "127.0.0.1"]))
            },
            port: self.environment["CASSANDRA_CQL_PORT"].flatMap(Int32.init) ?? 9042,
            protocolVersion: .v3
        )
        configuration.connectTimeoutMillis = 10_000
        configuration.requestTimeoutMillis = 24_000
        return configuration
    }

    private func makeClient(_ configuration: CassandraClient.Configuration) -> CassandraClient {
        CassandraClient(configuration: configuration, logger: Logger(label: "test.custom-auth"))
    }

    /// A failed handshake surfaces as `Error.badCredentials`. Compares `shortDescription` since the driver's
    /// message text is not fixed.
    private func assertAuthFailure(_ error: Swift.Error, file: StaticString = #filePath, line: UInt = #line) {
        guard let cassError = error as? CassandraClient.Error else {
            return XCTFail("expected CassandraClient.Error, got \(error)", file: file, line: line)
        }
        XCTAssertEqual(
            cassError.shortDescription,
            "Bad credentials",
            "expected an authentication failure, got \(cassError)",
            file: file,
            line: line
        )
    }

    /// A valid authenticator connects, `SELECT` returns, and `onSuccess` fired (the success callback).
    func testAuthenticatorConnectsAndSucceeds() throws {
        try self.requireAuthEnforcement()
        let authenticator = RecordingPlaintextAuthenticator(
            username: self.validUsername,
            password: self.validPassword
        )
        var configuration = self.makeConfiguration()
        configuration.authenticator = authenticator
        let client = self.makeClient(configuration)
        defer { XCTAssertNoThrow(try client.shutdown()) }

        let rows = try client.query("select release_version from system.local").wait()
        XCTAssertEqual(Array(rows).count, 1, "system.local returns exactly one row")
        XCTAssertTrue(authenticator.onSuccessFired, "onSuccess must fire once the server reports success")
    }

    /// Wrong credentials fail with an auth error rather than stalling or crashing.
    func testWrongCredentialsFailWithAuthError() throws {
        try self.requireAuthEnforcement()
        var configuration = self.makeConfiguration()
        configuration.authenticator = PlaintextAuthenticator(
            username: self.validUsername,
            password: "wrong-\(UUID().uuidString)"
        )
        let client = self.makeClient(configuration)
        defer { XCTAssertNoThrow(try client.shutdown()) }

        XCTAssertThrowsError(try client.query("select release_version from system.local").wait()) { error in
            self.assertAuthFailure(error)
        }
    }

    /// A valid authenticator plus bogus `username`/`password` still connects — the authenticator takes
    /// precedence over credentials in `makeCluster` (bogus credentials would otherwise fail the connect).
    func testCustomAuthenticatorTakesPrecedenceOverCredentials() throws {
        try self.requireAuthEnforcement()
        var configuration = self.makeConfiguration()
        configuration.authenticator = PlaintextAuthenticator(
            username: self.validUsername,
            password: self.validPassword
        )
        configuration.username = "bogus-\(UUID().uuidString)"
        configuration.password = "bogus-\(UUID().uuidString)"
        let client = self.makeClient(configuration)
        defer { XCTAssertNoThrow(try client.shutdown()) }

        let rows = try client.query("select release_version from system.local").wait()
        XCTAssertEqual(Array(rows).count, 1)
    }

    /// One shared authenticator instance under concurrent fan-out: all queries succeed and its shared,
    /// lock-protected counters advance (a stateless authenticator would leave them at 0). Exercises the
    /// shared-instance path under load; does not prove race-freedom.
    func testConcurrentSharedAuthenticator() throws {
        try self.requireAuthEnforcement()
        let authenticator = RecordingPlaintextAuthenticator(
            username: self.validUsername,
            password: self.validPassword
        )
        var configuration = self.makeConfiguration()
        configuration.authenticator = authenticator
        configuration.numIOThreads = 4
        let client = self.makeClient(configuration)
        defer { XCTAssertNoThrow(try client.shutdown()) }

        let iterations = 50
        let group = DispatchGroup()
        let lock = NSLock()
        var errors = [Swift.Error]()
        var rowCounts = [Int]()

        for _ in 0..<iterations {
            group.enter()
            DispatchQueue.global().async {
                defer { group.leave() }
                do {
                    let count = Array(try client.query("select release_version from system.local").wait()).count
                    lock.lock()
                    rowCounts.append(count)
                    lock.unlock()
                } catch {
                    lock.lock()
                    errors.append(error)
                    lock.unlock()
                }
            }
        }
        group.wait()

        XCTAssertEqual(errors.count, 0, "concurrent queries through the shared authenticator: \(errors)")
        XCTAssertEqual(rowCounts.count, iterations)
        XCTAssertTrue(rowCounts.allSatisfy { $0 == 1 }, "every query returns exactly one row")
        // The shared instance was actually driven under load (exact counts are not deterministic — the
        // driver decides how many connections to open — so assert only that the handshake ran).
        XCTAssertGreaterThan(authenticator.initialResponseCount, 0)
        XCTAssertGreaterThan(authenticator.onSuccessCount, 0)
    }

    /// An authenticator that throws from `initialResponse()` fails the connect cleanly (the trampoline's
    /// `do/catch` → `set_error_n` path).
    func testThrowingAuthenticatorFailsConnectCleanly() throws {
        try self.requireAuthEnforcement()
        var configuration = self.makeConfiguration()
        configuration.authenticator = ThrowingAuthenticator()
        let client = self.makeClient(configuration)
        defer { XCTAssertNoThrow(try client.shutdown()) }

        XCTAssertThrowsError(try client.query("select release_version from system.local").wait()) { error in
            self.assertAuthFailure(error)
        }
    }

    /// The enforcement guard: a no-authenticator, no-credential connect must be rejected. If it connects,
    /// auth is silently off and this fails the build, so the other gated tests can't pass vacuously. This
    /// test is the probe, which is why the suite is gated by an out-of-band flag rather than by probing.
    func testClusterEnforcesAuthentication() throws {
        try self.requireAuthEnforcement()
        var configuration = self.makeConfiguration()
        configuration.authenticator = nil
        configuration.username = nil
        configuration.password = nil
        let client = self.makeClient(configuration)
        defer { XCTAssertNoThrow(try client.shutdown()) }

        XCTAssertThrowsError(
            try client.query("select release_version from system.local").wait(),
            "CASSANDRA_REQUIRE_AUTH is set but the cluster accepted a no-credential connect — auth is not enforced, so the auth tests cannot be trusted"
        ) { error in
            self.assertAuthFailure(error)
        }
    }

    /// After a connect/query/close, the retained authenticator box is released — its `deinit` runs — showing
    /// the driver's data-cleanup fired on teardown. Proves release on normal teardown only.
    func testBoxReleasedOnSessionTeardown() throws {
        let deinitCounter = NIOLockedValueBox<Int>(0)

        func connectQueryAndShutdown() throws {
            let authenticator = DeinitCountingAuthenticator(
                username: self.validUsername,
                password: self.validPassword,
                deinitCounter: deinitCounter
            )
            var configuration = self.makeConfiguration()
            configuration.authenticator = authenticator
            let client = self.makeClient(configuration)
            defer { try? client.shutdown() }
            let rows = try client.query("select release_version from system.local").wait()
            XCTAssertEqual(Array(rows).count, 1)
        }
        try connectQueryAndShutdown()

        // The data-cleanup trampoline may run on a driver thread during teardown, so poll briefly.
        let deadline = Date().addingTimeInterval(5)
        while deinitCounter.withLockedValue({ $0 }) == 0, Date() < deadline {
            Thread.sleep(forTimeInterval: 0.05)
        }
        XCTAssertEqual(
            deinitCounter.withLockedValue { $0 },
            1,
            "the authenticator box must be released exactly once when the driver destroys its provider"
        )
    }
}
