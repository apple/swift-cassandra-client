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

import NIOConcurrencyHelpers

@testable import CassandraClient

// Test authenticators shared by the custom-authenticator unit and integration suites. Kept `internal`
// (not `fileprivate`) so both files reuse one definition. Stateful ones lock their own state to satisfy
// the `Sendable` protocol under the driver's concurrent invocation.

/// SASL PLAIN (`\0username\0password`) — the single-token mechanism `PasswordAuthenticator` accepts.
/// Doubles as the doc-comment example on ``CassandraClient/Authenticator``.
struct PlaintextAuthenticator: CassandraClient.Authenticator {
    let username: String
    let password: String

    func initialResponse() throws -> [UInt8]? {
        [0x00] + Array(self.username.utf8) + [0x00] + Array(self.password.utf8)
    }
}

/// Implements only `initialResponse()`; leaves `evaluateChallenge`/`onSuccess` to the protocol defaults.
struct MinimalAuthenticator: CassandraClient.Authenticator {
    let response: [UInt8]?
    func initialResponse() throws -> [UInt8]? { self.response }
}

/// Thrown by ``ThrowingAuthenticator`` to drive the trampoline's `do/catch` → `set_error` path.
struct AuthenticatorTestError: Swift.Error {}

/// `initialResponse()` throws, so the exchange aborts before any token is produced.
struct ThrowingAuthenticator: CassandraClient.Authenticator {
    func initialResponse() throws -> [UInt8]? { throw AuthenticatorTestError() }
}

/// Byte-reverses each challenge and records challenges and the success token, for the challenge/success
/// round-trip that no live `PasswordAuthenticator` exchange reaches.
final class RecordingChallengeAuthenticator: CassandraClient.Authenticator {
    private let state = NIOLockedValueBox<(challenges: [[UInt8]], successToken: [UInt8]?)>(([], nil))

    func initialResponse() throws -> [UInt8]? { [] }

    func evaluateChallenge(_ challenge: [UInt8]) throws -> [UInt8]? {
        self.state.withLockedValue { $0.challenges.append(challenge) }
        return Array(challenge.reversed())
    }

    func onSuccess(_ token: [UInt8]) throws {
        self.state.withLockedValue { $0.successToken = token }
    }

    var recordedChallenges: [[UInt8]] { self.state.withLockedValue { $0.challenges } }
    var recordedSuccessToken: [UInt8]? { self.state.withLockedValue { $0.successToken } }
}

/// Plaintext credentials plus lock-protected invocation counts. Serves the success-observability test
/// (`onSuccessFired`) and the shared-instance concurrency test (counts under fan-out).
final class RecordingPlaintextAuthenticator: CassandraClient.Authenticator {
    let username: String
    let password: String
    private let counts = NIOLockedValueBox<(initial: Int, success: Int)>((0, 0))

    init(username: String, password: String) {
        self.username = username
        self.password = password
    }

    func initialResponse() throws -> [UInt8]? {
        self.counts.withLockedValue { $0.initial += 1 }
        return [0x00] + Array(self.username.utf8) + [0x00] + Array(self.password.utf8)
    }

    func onSuccess(_ token: [UInt8]) throws {
        self.counts.withLockedValue { $0.success += 1 }
    }

    var initialResponseCount: Int { self.counts.withLockedValue { $0.initial } }
    var onSuccessCount: Int { self.counts.withLockedValue { $0.success } }
    var onSuccessFired: Bool { self.onSuccessCount > 0 }
}

/// Increments an external, box-outliving counter in `deinit`, to witness the driver's data-cleanup
/// releasing the retained box on session teardown.
final class DeinitCountingAuthenticator: CassandraClient.Authenticator {
    let username: String
    let password: String
    private let deinitCounter: NIOLockedValueBox<Int>

    init(username: String, password: String, deinitCounter: NIOLockedValueBox<Int>) {
        self.username = username
        self.password = password
        self.deinitCounter = deinitCounter
    }

    deinit {
        self.deinitCounter.withLockedValue { $0 += 1 }
    }

    func initialResponse() throws -> [UInt8]? {
        [0x00] + Array(self.username.utf8) + [0x00] + Array(self.password.utf8)
    }
}
