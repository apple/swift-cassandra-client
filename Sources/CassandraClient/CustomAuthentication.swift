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

internal import CDataStaxDriver

extension CassandraClient {
    /// A custom SASL authenticator, for mechanisms beyond username/password (e.g. AWS SigV4 for
    /// Amazon Keyspaces, or Kerberos).
    ///
    /// The client drives a SASL (Simple Authentication and Security Layer) handshake: it produces an
    /// initial response, answers any server challenges, then observes success. Set an instance on
    /// ``Configuration/authenticator`` to use it; when set, it takes precedence over
    /// ``Configuration/username``/``Configuration/password``.
    ///
    /// Tokens are opaque bytes. Returned responses are `[UInt8]?` where `nil` sends an empty response;
    /// received challenge/success tokens are always a (possibly empty) `[UInt8]`.
    ///
    /// - Important: A single authenticator instance is shared across every connection the driver opens —
    ///   the initial connection and every reconnect over the session's lifetime — and its methods are
    ///   invoked **concurrently** by the driver's I/O threads. Conforming types must therefore be
    ///   `Sendable` and safe under concurrent invocation. `Sendable` governs transfer between concurrency
    ///   domains, not concurrent entry into these synchronous methods, so **the compiler enforces none of
    ///   this**: an `@unchecked Sendable` conformer over mutable state can compile and still race. A
    ///   mechanism carrying mutable state must serialize its own access.
    ///
    /// A single-token mechanism such as SASL PLAIN implements only ``initialResponse()``:
    ///
    /// ```swift
    /// struct PlaintextAuthenticator: CassandraClient.Authenticator {
    ///     let username: String
    ///     let password: String
    ///     func initialResponse() throws -> [UInt8]? {
    ///         [0x00] + Array(username.utf8) + [0x00] + Array(password.utf8)
    ///     }
    /// }
    /// ```
    public protocol Authenticator: Sendable {
        /// The initial response that begins the SASL handshake. `nil` sends an empty response.
        func initialResponse() throws -> [UInt8]?

        /// Answer a server challenge. The challenge is always present (possibly empty). Return `nil` when
        /// the client has nothing further to send.
        func evaluateChallenge(_ challenge: [UInt8]) throws -> [UInt8]?

        /// Called once the server reports success, with any final token it sent (always present, may be empty).
        func onSuccess(_ token: [UInt8]) throws
    }
}

extension CassandraClient.Authenticator {
    /// Default: a single-token mechanism (e.g. SASL PLAIN) sends nothing further after its initial response.
    public func evaluateChallenge(_ challenge: [UInt8]) throws -> [UInt8]? { nil }

    /// Default: nothing to do on success.
    public func onSuccess(_ token: [UInt8]) throws {}
}

// MARK: - Bridging to CassAuthenticatorCallbacks

/// Retained payload carried through the driver's `void* data` slot. Recovered (unretained) in each
/// exchange trampoline and released once by ``authDataCleanup`` when the driver destroys its provider.
private final class AuthenticatorBox {
    let authenticator: any CassandraClient.Authenticator

    init(_ authenticator: any CassandraClient.Authenticator) {
        self.authenticator = authenticator
    }
}

/// Namespaces the callback struct and the marshaling helpers. `internal` (not `private`) so the pure
/// helpers below are reachable from the unit tests via `@testable import`.
internal enum AuthBridge {
    /// Maximum UTF-8 byte length of a client-produced auth error message before it crosses the C boundary.
    /// Unrelated to the log-truncation caps on ``CassandraClient/Configuration``.
    static let maxAuthErrorLength = 1024

    /// The four `@convention(c)` callbacks the driver invokes during a SASL exchange. Copied by value into
    /// the driver's refcounted provider at registration, so it only needs to outlive that one call — but it
    /// is `static` (file-scope) because the transient `Cluster` cannot own it.
    static let authCallbacks = CassAuthenticatorCallbacks(
        initial_callback: authInitial,
        challenge_callback: authChallenge,
        success_callback: authSuccess,
        cleanup_callback: authExchangeCleanup
    )

    /// Reads a driver token into an owned `[UInt8]`, empty for a NULL-or-empty token. The driver never
    /// passes NULL on the challenge/success paths (`std::string::data()`); the guard is defensive.
    static func readToken(_ token: UnsafePointer<CChar>?, _ size: Int) -> [UInt8] {
        guard let token, size > 0 else { return [] }
        return [UInt8](UnsafeRawBufferPointer(start: token, count: size))
    }

    /// Caps an auth error message at ``maxAuthErrorLength`` UTF-8 bytes — the length `writeError` sends —
    /// backing off to a scalar boundary so a multi-byte scalar is never split, with room reserved for the
    /// appended ellipsis. Pure, so it unit-tests without a live `CassAuthenticator*`.
    static func truncatedAuthError(_ message: String) -> String {
        guard message.utf8.count > maxAuthErrorLength else { return message }
        let budget = maxAuthErrorLength - "…".utf8.count
        var truncated = ""
        var bytes = 0
        for scalar in message.unicodeScalars {
            let width = String(scalar).utf8.count
            if bytes + width > budget { break }
            truncated.unicodeScalars.append(scalar)
            bytes += width
        }
        return truncated + "…"
    }

    /// Sets the response token on the exchange; a `nil` (or empty) response sends empty bytes.
    static func writeResponse(_ auth: OpaquePointer, _ bytes: [UInt8]?) {
        let bytes = bytes ?? []
        bytes.withUnsafeBytes { raw in
            cass_authenticator_set_response(
                auth,
                raw.baseAddress?.assumingMemoryBound(to: CChar.self),
                bytes.count
            )
        }
    }

    /// Reports a failure on the exchange so the driver aborts it; the message is expected pre-truncated.
    static func writeError(_ auth: OpaquePointer, _ message: String) {
        cass_authenticator_set_error_n(auth, message, message.utf8.count)
    }
}

// File-scope, non-capturing trampolines (implicitly bridged to `@convention(c)` in `authCallbacks`). Each
// recovers the authenticator via `takeUnretainedValue()` — the driver's retained `data` owns the box. A
// throw is capped and reported via `set_error` so the driver aborts the exchange, not crossing the C boundary.

private func authInitial(_ auth: OpaquePointer?, _ data: UnsafeMutableRawPointer?) {
    guard let auth, let data else { return }
    let box = Unmanaged<AuthenticatorBox>.fromOpaque(data).takeUnretainedValue()
    do {
        AuthBridge.writeResponse(auth, try box.authenticator.initialResponse())
    } catch {
        AuthBridge.writeError(auth, AuthBridge.truncatedAuthError(String(describing: error)))
    }
}

private func authChallenge(
    _ auth: OpaquePointer?,
    _ data: UnsafeMutableRawPointer?,
    _ token: UnsafePointer<CChar>?,
    _ size: Int
) {
    guard let auth, let data else { return }
    let box = Unmanaged<AuthenticatorBox>.fromOpaque(data).takeUnretainedValue()
    do {
        let response = try box.authenticator.evaluateChallenge(AuthBridge.readToken(token, size))
        AuthBridge.writeResponse(auth, response)
    } catch {
        AuthBridge.writeError(auth, AuthBridge.truncatedAuthError(String(describing: error)))
    }
}

private func authSuccess(
    _ auth: OpaquePointer?,
    _ data: UnsafeMutableRawPointer?,
    _ token: UnsafePointer<CChar>?,
    _ size: Int
) {
    guard let auth, let data else { return }
    let box = Unmanaged<AuthenticatorBox>.fromOpaque(data).takeUnretainedValue()
    do {
        try box.authenticator.onSuccess(AuthBridge.readToken(token, size))
    } catch {
        AuthBridge.writeError(auth, AuthBridge.truncatedAuthError(String(describing: error)))
    }
}

/// Per-exchange cleanup: no per-exchange scratch state to release. This fires once per connection; the box
/// is freed by ``authDataCleanup``, not here (releasing here would over-release).
private func authExchangeCleanup(_ auth: OpaquePointer?, _ data: UnsafeMutableRawPointer?) {}

/// Data-cleanup: releases the box's single retain when the driver destroys its provider (once, at refcount
/// zero — after `cass_cluster_free` and every connection is gone).
private func authDataCleanup(_ data: UnsafeMutableRawPointer?) {
    guard let data else { return }
    Unmanaged<AuthenticatorBox>.fromOpaque(data).release()
}

// MARK: - Cluster wiring

// Declared here (not in Configuration.swift, where `Cluster` lives) to reach the file-scope bridge symbols;
// `Cluster.rawPointer` is module-internal, so the extension can still register the callbacks.
extension Cluster {
    func setAuthenticator(_ authenticator: any CassandraClient.Authenticator) throws {
        let box = AuthenticatorBox(authenticator)
        let result = withUnsafePointer(to: AuthBridge.authCallbacks) { callbacks in
            cass_cluster_set_authenticator_callbacks(
                self.rawPointer,
                callbacks,
                authDataCleanup,
                Unmanaged.passRetained(box).toOpaque()
            )
        }
        // Ownership transfers to the driver at the call above (it builds its provider unconditionally), so
        // no release-on-throw — the driver's data-cleanup frees the box; a release here would double-free.
        // The setter always returns CASS_OK; this guard is a formality against the CassError return type.
        guard result == CASS_OK else {
            throw CassandraClient.Error(result, message: "Failed to configure cluster")
        }
    }
}
