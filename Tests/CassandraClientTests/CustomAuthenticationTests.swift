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
import XCTest

@testable import CassandraClient

/// Custom-authenticator UNIT tests. No cluster required — cover the authenticator protocol and the pure
/// marshaling helpers in ``AuthBridge`` (the trampolines and C setter are exercised by the integration
/// suite, since a `CassAuthenticator*` cannot be fabricated outside a live exchange).
final class CustomAuthenticationTests: XCTestCase {

    private func makeConfiguration() -> CassandraClient.Configuration {
        CassandraClient.Configuration(
            contactPointsProvider: { callback in callback(.success(["127.0.0.1"])) },
            port: 9042,
            protocolVersion: .v3
        )
    }

    // MARK: - Authenticator byte output

    /// Plaintext `initialResponse()` is exactly the SASL PLAIN encoding `\0user\0pass`.
    func testPlaintextInitialResponseEncoding() throws {
        let authenticator = PlaintextAuthenticator(username: "u", password: "p")
        XCTAssertEqual(
            try authenticator.initialResponse(),
            [0x00] + Array("u".utf8) + [0x00] + Array("p".utf8)
        )
    }

    /// Empty credentials still frame the two NUL separators.
    func testPlaintextEmptyCredentials() throws {
        let authenticator = PlaintextAuthenticator(username: "", password: "")
        XCTAssertEqual(try authenticator.initialResponse(), [0x00, 0x00])
    }

    /// Multi-byte credentials are encoded as their UTF-8 bytes, not scalars.
    func testPlaintextUnicodeCredentials() throws {
        let username = "usér"
        let password = "pä🔑"
        let authenticator = PlaintextAuthenticator(username: username, password: password)
        XCTAssertEqual(
            try authenticator.initialResponse(),
            [0x00] + Array(username.utf8) + [0x00] + Array(password.utf8)
        )
    }

    /// A challenge is transformed and returned; the success token is recorded verbatim. Byte content
    /// (including an embedded NUL) survives the `[UInt8]` round-trips.
    func testChallengeTransformAndSuccessRecording() throws {
        let authenticator = RecordingChallengeAuthenticator()

        let challenge: [UInt8] = [0x01, 0x02, 0x00, 0x03]  // embedded NUL — tokens are binary, not C strings
        XCTAssertEqual(try authenticator.evaluateChallenge(challenge), [0x03, 0x00, 0x02, 0x01])
        XCTAssertEqual(authenticator.recordedChallenges, [challenge])

        let successToken: [UInt8] = [0xAA, 0x00, 0xBB]
        try authenticator.onSuccess(successToken)
        XCTAssertEqual(authenticator.recordedSuccessToken, successToken)
    }

    /// A mechanism that implements only `initialResponse()` gets `nil` from `evaluateChallenge`
    /// and a no-op `onSuccess` from the protocol extension.
    func testDefaultProtocolMethods() throws {
        let authenticator = MinimalAuthenticator(response: [0x2A])
        XCTAssertEqual(try authenticator.initialResponse(), [0x2A])
        XCTAssertNil(try authenticator.evaluateChallenge([0x01, 0x02]))
        XCTAssertNoThrow(try authenticator.onSuccess([0x03]))
    }

    // MARK: - readToken

    /// A NULL token reads as empty — never a crash. The NULL input is synthetic; the driver passes
    /// `std::string::data()` (never NULL) on the challenge/success paths, so this covers only the guard.
    func testReadTokenNilYieldsEmpty() {
        XCTAssertEqual(AuthBridge.readToken(nil, 0), [])
        XCTAssertEqual(AuthBridge.readToken(nil, 8), [])  // NULL with a positive size the driver never sends
    }

    /// A non-NULL token with size 0 reads as empty (an absent server token arrives as empty, not NULL).
    func testReadTokenZeroSizeYieldsEmpty() {
        let bytes: [UInt8] = [0x01, 0x02]
        let result = bytes.withUnsafeBytes { raw in
            AuthBridge.readToken(raw.baseAddress?.assumingMemoryBound(to: CChar.self), 0)
        }
        XCTAssertEqual(result, [])
    }

    /// A non-empty token is copied byte-for-byte, embedded NULs and high bytes included.
    func testReadTokenPreservesBytes() {
        let input: [UInt8] = [0x00, 0x75, 0x00, 0x70, 0xFF, 0x80]
        let result = input.withUnsafeBytes { raw in
            AuthBridge.readToken(raw.baseAddress?.assumingMemoryBound(to: CChar.self), input.count)
        }
        XCTAssertEqual(result, input)
    }

    // MARK: - truncatedAuthError

    /// A message within the cap is returned unchanged.
    func testTruncatedAuthErrorShortMessageUnchanged() {
        let message = "authentication failed"
        XCTAssertEqual(AuthBridge.truncatedAuthError(message), message)
    }

    /// An empty message is returned unchanged.
    func testTruncatedAuthErrorEmptyMessage() {
        XCTAssertEqual(AuthBridge.truncatedAuthError(""), "")
    }

    /// A message of exactly `maxAuthErrorLength` bytes is at the boundary (cap is `>`, not `>=`) — unchanged.
    func testTruncatedAuthErrorAtBoundaryUnchanged() {
        let message = String(repeating: "a", count: AuthBridge.maxAuthErrorLength)
        let result = AuthBridge.truncatedAuthError(message)
        XCTAssertEqual(result, message)
        XCTAssertEqual(result.utf8.count, AuthBridge.maxAuthErrorLength)
    }

    /// An over-long message is capped at `maxAuthErrorLength` UTF-8 bytes (the length `writeError` sends),
    /// ending in an ellipsis whose room is reserved within the cap.
    func testTruncatedAuthErrorCapsLongMessage() {
        let message = String(repeating: "a", count: AuthBridge.maxAuthErrorLength + 500)
        let result = AuthBridge.truncatedAuthError(message)
        XCTAssertLessThanOrEqual(result.utf8.count, AuthBridge.maxAuthErrorLength)
        XCTAssertTrue(result.hasSuffix("…"))
        XCTAssertTrue(message.hasPrefix(String(result.dropLast())))  // kept content is a prefix of the original
    }

    /// Truncation backs off to a scalar boundary: a run of 4-byte scalars is never split into a
    /// replacement character, and the byte cap still holds.
    func testTruncatedAuthErrorDoesNotSplitScalar() {
        let message = String(repeating: "🔑", count: 1000)  // 4 UTF-8 bytes each, far over the cap
        let result = AuthBridge.truncatedAuthError(message)
        XCTAssertLessThanOrEqual(result.utf8.count, AuthBridge.maxAuthErrorLength)
        XCTAssertEqual(result.unicodeScalars.last, "…")
        XCTAssertTrue(
            result.unicodeScalars.dropLast().allSatisfy { $0 == "🔑" },
            "no scalar should be split into U+FFFD"
        )
    }

    // MARK: - Configuration.description redaction

    /// With an authenticator set, `description` shows `authenticator: custom` and none of the
    /// authenticator's internals (potential secrets).
    func testDescriptionRedactsAuthenticatorInternals() {
        let secret = "TOP_SECRET_TOKEN_\(UUID().uuidString)"
        var configuration = self.makeConfiguration()
        configuration.authenticator = PlaintextAuthenticator(username: "u", password: secret)

        let description = configuration.description
        XCTAssertTrue(description.contains("authenticator: custom"))
        XCTAssertFalse(description.contains(secret), "authenticator internals must not appear in description")
    }

    /// With no authenticator, `description` shows `authenticator: none`.
    func testDescriptionShowsNoneWithoutAuthenticator() {
        let configuration = self.makeConfiguration()
        XCTAssertTrue(configuration.description.contains("authenticator: none"))
    }
}
