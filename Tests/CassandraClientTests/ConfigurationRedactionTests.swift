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

import Testing

@testable import CassandraClient

struct ConfigurationRedactionTests {
    private static let secrets = ["user-password", "PRIVATE KEY PEM", "key-password"]

    private func makeConfiguration() -> CassandraClient.Configuration {
        var configuration = CassandraClient.Configuration(
            contactPointsProvider: { $0(.success(["localhost"])) },
            port: 9042,
            protocolVersion: .v4
        )
        configuration.authenticator = CassandraClient.PasswordAuthenticator(
            username: "user",
            password: "user-password"
        )
        var tls = CassandraClient.Configuration.TLS()
        tls.clientCredentials = .init(
            certificate: "CERTIFICATE PEM",
            privateKey: (key: "PRIVATE KEY PEM", password: "key-password")
        )
        configuration.transportSecurity = .tls(tls)
        return configuration
    }

    /// Every printing path — `description`, `debugDescription` and the `Mirror` that `dump(_:)` walks —
    /// leaves out the passwords and the private key.
    @Test
    func configurationRedactsSecrets() {
        let configuration = self.makeConfiguration()
        var dumped = ""
        dump(configuration, to: &dumped)
        for rendered in [String(describing: configuration), String(reflecting: configuration), dumped] {
            for secret in Self.secrets {
                #expect(!rendered.contains(secret), "\(secret) in \(rendered)")
            }
        }
    }

    @Test
    func tlsRedactsSecrets() throws {
        let tls = try #require(self.makeConfiguration().transportSecurity.tls)
        var dumped = ""
        dump(tls, to: &dumped)
        for rendered in [String(describing: tls), String(reflecting: tls), dumped] {
            for secret in Self.secrets {
                #expect(!rendered.contains(secret), "\(secret) in \(rendered)")
            }
        }
    }

    @Test
    func passwordAuthenticatorRedactsPassword() {
        let authenticator = CassandraClient.PasswordAuthenticator(username: "user", password: "user-password")
        var dumped = ""
        dump(authenticator, to: &dumped)
        for rendered in [String(describing: authenticator), String(reflecting: authenticator), dumped] {
            #expect(!rendered.contains("user-password"), "password in \(rendered)")
            #expect(rendered.contains("user"))
        }
    }

    /// The built-in authenticator still answers the SASL PLAIN handshake if driven through the callbacks.
    @Test
    func passwordAuthenticatorInitialResponseIsSASLPlain() throws {
        let authenticator = CassandraClient.PasswordAuthenticator(username: "u", password: "p")
        #expect(try authenticator.initialResponse() == [0x00, UInt8(ascii: "u"), 0x00, UInt8(ascii: "p")])
    }
}
