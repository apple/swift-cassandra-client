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
///
/// The environment accessors are `static` so the `contactPointsProvider` closure never captures `self`:
/// a test case is not `Sendable`, and they read only the process environment.
final class CustomAuthenticationIntegrationTests: XCTestCase {
    private static var environment: [String: String] { ProcessInfo.processInfo.environment }
    private static var validUsername: String { Self.environment["CASSANDRA_USER"] ?? "cassandra" }
    private static var validPassword: String { Self.environment["CASSANDRA_PASSWORD"] ?? "cassandra" }

    /// Skips a test unless `CASSANDRA_REQUIRE_AUTH` is set to a non-empty value, i.e. the caller asserts the
    /// target cluster enforces `PasswordAuthenticator`. See the type doc for why this is opt-in, not probed.
    private func requireAuthEnforcement() throws {
        guard let value = Self.environment["CASSANDRA_REQUIRE_AUTH"], !value.isEmpty else {
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
                callback(.success([Self.environment["CASSANDRA_HOST"] ?? "127.0.0.1"]))
            },
            port: Self.environment["CASSANDRA_CQL_PORT"].flatMap(Int32.init) ?? 9042,
            protocolVersion: .v3
        )
        configuration.connectTimeout = .milliseconds(10_000)
        configuration.requestTimeout = .milliseconds(24_000)
        return configuration
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

    /// Runs a client whose connect must fail authentication: a request waiting on the connect fails, and
    /// `withClient` rethrows the connect error from `run()`.
    private func assertConnectFailsAuthentication(
        _ configuration: CassandraClient.Configuration,
        _ message: @autoclosure () -> String = "",
        file: StaticString = #filePath,
        line: UInt = #line
    ) async {
        await assertThrowsErrorAsync(
            try await CassandraClient.withClient(configuration: configuration) { client in
                await assertThrowsErrorAsync(
                    try await client.query("select release_version from system.local"),
                    file: file,
                    line: line
                )
            },
            message(),
            file: file,
            line: line
        ) { error in
            self.assertAuthFailure(error, file: file, line: line)
        }
    }

    /// A valid authenticator connects, `SELECT` returns, and `onSuccess` fired (the success callback).
    func testAuthenticatorConnectsAndSucceeds() async throws {
        try self.requireAuthEnforcement()
        let authenticator = RecordingPlaintextAuthenticator(
            username: Self.validUsername,
            password: Self.validPassword
        )
        var configuration = self.makeConfiguration()
        configuration.authenticator = authenticator
        try await CassandraClient.withClient(configuration: configuration) { client in
            let rows = try await client.query("select release_version from system.local")
            XCTAssertEqual(Array(rows).count, 1, "system.local returns exactly one row")
        }
        XCTAssertTrue(authenticator.onSuccessFired, "onSuccess must fire once the server reports success")
    }

    /// Wrong credentials fail with an auth error rather than stalling or crashing.
    func testWrongCredentialsFailWithAuthError() async throws {
        try self.requireAuthEnforcement()
        var configuration = self.makeConfiguration()
        configuration.authenticator = PlaintextAuthenticator(
            username: Self.validUsername,
            password: "wrong-\(UUID().uuidString)"
        )
        await self.assertConnectFailsAuthentication(configuration)
    }

    /// The built-in password authenticator connects with valid credentials and is rejected with bogus
    /// ones. It goes through the driver's native credentials path rather than the SASL callbacks.
    func testPasswordAuthenticator() async throws {
        try self.requireAuthEnforcement()
        var configuration = self.makeConfiguration()
        configuration.authenticator = CassandraClient.PasswordAuthenticator(
            username: Self.validUsername,
            password: Self.validPassword
        )
        try await CassandraClient.withClient(configuration: configuration) { client in
            let rows = try await client.query("select release_version from system.local")
            XCTAssertEqual(Array(rows).count, 1)
        }

        configuration.authenticator = CassandraClient.PasswordAuthenticator(
            username: "bogus-\(UUID().uuidString)",
            password: "bogus-\(UUID().uuidString)"
        )
        await self.assertConnectFailsAuthentication(configuration)
    }

    /// One shared authenticator instance under concurrent fan-out: all queries succeed and its shared,
    /// lock-protected counters advance (a stateless authenticator would leave them at 0). Exercises the
    /// shared-instance path under load; does not prove race-freedom.
    func testConcurrentSharedAuthenticator() async throws {
        try self.requireAuthEnforcement()
        let authenticator = RecordingPlaintextAuthenticator(
            username: Self.validUsername,
            password: Self.validPassword
        )
        var configuration = self.makeConfiguration()
        configuration.authenticator = authenticator
        configuration.numIOThreads = 4
        let iterations = 50
        let outcomes = try await CassandraClient.withClient(configuration: configuration) { client in
            await withTaskGroup(of: Result<Int, Swift.Error>.self) { group in
                for _ in 0..<iterations {
                    group.addTask {
                        do {
                            return .success(
                                Array(try await client.query("select release_version from system.local")).count
                            )
                        } catch {
                            return .failure(error)
                        }
                    }
                }
                return await group.reduce(into: []) { $0.append($1) }
            }
        }

        let rowCounts = outcomes.compactMap { try? $0.get() }
        let errors = outcomes.compactMap { outcome -> Swift.Error? in
            if case .failure(let error) = outcome { return error }
            return nil
        }
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
    func testThrowingAuthenticatorFailsConnectCleanly() async throws {
        try self.requireAuthEnforcement()
        var configuration = self.makeConfiguration()
        configuration.authenticator = ThrowingAuthenticator()
        await self.assertConnectFailsAuthentication(configuration)
    }

    /// The enforcement guard: a no-authenticator, no-credential connect must be rejected. If it connects,
    /// auth is silently off and this fails the build, so the other gated tests can't pass vacuously. This
    /// test is the probe, which is why the suite is gated by an out-of-band flag rather than by probing.
    func testClusterEnforcesAuthentication() async throws {
        try self.requireAuthEnforcement()
        var configuration = self.makeConfiguration()
        configuration.authenticator = nil
        await self.assertConnectFailsAuthentication(
            configuration,
            "CASSANDRA_REQUIRE_AUTH is set but the cluster accepted a no-credential connect — auth is not enforced, so the auth tests cannot be trusted"
        )
    }

    /// After a connect/query/close, the retained authenticator box is released — its `deinit` runs — showing
    /// the driver's data-cleanup fired on teardown. Proves release on normal teardown only.
    func testBoxReleasedOnSessionTeardown() async throws {
        let deinitCounter = NIOLockedValueBox<Int>(0)

        func connectQueryAndShutdown() async throws {
            let authenticator = DeinitCountingAuthenticator(
                username: Self.validUsername,
                password: Self.validPassword,
                deinitCounter: deinitCounter
            )
            var configuration = self.makeConfiguration()
            configuration.authenticator = authenticator
            let rowCount = try await CassandraClient.withClient(configuration: configuration) { client in
                Array(try await client.query("select release_version from system.local")).count
            }
            XCTAssertEqual(rowCount, 1)
        }
        try await connectQueryAndShutdown()

        // The data-cleanup trampoline may run on a driver thread during teardown, so poll briefly.
        let deadline = Date().addingTimeInterval(5)
        while deinitCounter.withLockedValue({ $0 }) == 0, Date() < deadline {
            try await Task.sleep(for: .milliseconds(50))
        }
        XCTAssertEqual(
            deinitCounter.withLockedValue { $0 },
            1,
            "the authenticator box must be released exactly once when the driver destroys its provider"
        )
    }
}
