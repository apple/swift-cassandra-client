//===----------------------------------------------------------------------===//
//
// This source file is part of the Swift Cassandra Client open source project
//
// Copyright (c) 2022-2025 Apple Inc. and the Swift Cassandra Client project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of Swift Cassandra Client project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import CDataStaxDriver
import NIO
import XCTest

@testable import CassandraClient

/// Unit tests for the SSL verify-flag mapping and the configuration checks around it. No cluster
/// required — the flags are asserted as the bitmask handed to the driver, and building a cluster
/// opens no connections.
final class SSLConfigurationTests: XCTestCase {
    private static let peerCert = Int32(CASS_SSL_VERIFY_PEER_CERT.rawValue)
    private static let peerIdentity = Int32(CASS_SSL_VERIFY_PEER_IDENTITY.rawValue)
    private static let peerIdentityDNS = Int32(CASS_SSL_VERIFY_PEER_IDENTITY_DNS.rawValue)

    private func makeConfiguration() -> CassandraClient.Configuration {
        CassandraClient.Configuration(
            contactPointsProvider: { callback in callback(.success(["127.0.0.1"])) },
            port: 9042,
            protocolVersion: .v3
        )
    }

    // MARK: - Default

    /// The default verifies the peer's identity, so a certificate that merely chains to a trusted
    /// issuer is not accepted for an address it does not name.
    func testDefaultVerifiesPeerIdentity() {
        XCTAssertEqual(CassandraClient.Configuration.SSL().verifyFlag, .peerIdentity)
    }

    // MARK: - Flag mapping

    /// `.none` disables verification entirely, which the driver spells as an empty mask.
    func testNoneMapsToEmptyMask() {
        XCTAssertEqual(self.flags(for: .none), Int32(CASS_SSL_VERIFY_NONE.rawValue))
    }

    /// `.peerCert` validates the chain and nothing else.
    func testPeerCertMapsToChainValidationOnly() {
        XCTAssertEqual(self.flags(for: .peerCert), Self.peerCert)
    }

    /// `.peerIdentity` requests the IP subject match *and* chain validation. The driver gates
    /// `SSL_get_verify_result` on the peer-cert bit, so omitting it would match the subject of a
    /// certificate whose chain was never validated.
    func testPeerIdentityAlsoRequestsChainValidation() {
        let flags = self.flags(for: .peerIdentity)
        XCTAssertEqual(flags, Self.peerCert | Self.peerIdentity)
    }

    /// `.peerIdentityDNS` requests the hostname subject match *and* chain validation, for the same
    /// reason as ``testPeerIdentityAlsoRequestsChainValidation``.
    func testPeerIdentityDNSAlsoRequestsChainValidation() {
        let flags = self.flags(for: .peerIdentityDNS)
        XCTAssertEqual(flags, Self.peerCert | Self.peerIdentityDNS)
    }

    /// The mask actually reaches the context the driver is handed. Asserting ``cassVerifyFlags`` alone
    /// would stay green if `makeSSLContext()` stopped applying it.
    func testMakeSSLContextAppliesTheMask() throws {
        for verifyFlag in CassandraClient.Configuration.SSL.VerifyFlag.allCases {
            let sslContext = try self.makeSSL(verifyFlag: verifyFlag).makeSSLContext()
            XCTAssertEqual(
                sslContext.verifyFlags,
                self.flags(for: verifyFlag),
                "verifyFlag: \(verifyFlag)"
            )
        }
    }

    // MARK: - Hostname resolution requirement

    /// `.peerIdentityDNS` without hostname resolution is rejected up front rather than failing every
    /// handshake against an unresolved hostname.
    func testPeerIdentityDNSRequiresHostnameResolution() {
        for hostnameResolution in [nil, false] as [Bool?] {
            var configuration = self.makeConfiguration()
            configuration.ssl = self.makeSSL(verifyFlag: .peerIdentityDNS)
            configuration.hostnameResolution = hostnameResolution

            XCTAssertThrowsError(try self.makeCluster(configuration)) { error in
                XCTAssertEqual(
                    error as? CassandraClient.Error,
                    .badParams(
                        "SSL verifyFlag .peerIdentityDNS requires hostnameResolution to be true"
                    ),
                    "hostnameResolution: \(String(describing: hostnameResolution))"
                )
            }
        }
    }

    /// With hostname resolution enabled the driver has a hostname to match, so the pairing is allowed.
    func testPeerIdentityDNSWithHostnameResolutionIsAccepted() {
        var configuration = self.makeConfiguration()
        configuration.ssl = self.makeSSL(verifyFlag: .peerIdentityDNS)
        configuration.hostnameResolution = true

        XCTAssertNoThrow(try self.makeCluster(configuration))
    }

    /// The requirement is specific to DNS matching; the other options resolve no hostname and so are
    /// accepted whether hostname resolution is unset or explicitly off.
    func testOtherVerifyFlagsDoNotRequireHostnameResolution() {
        for verifyFlag in CassandraClient.Configuration.SSL.VerifyFlag.allCases where verifyFlag != .peerIdentityDNS {
            for hostnameResolution in [nil, false] as [Bool?] {
                var configuration = self.makeConfiguration()
                configuration.ssl = self.makeSSL(verifyFlag: verifyFlag)
                configuration.hostnameResolution = hostnameResolution

                XCTAssertNoThrow(
                    try self.makeCluster(configuration),
                    "verifyFlag: \(verifyFlag), "
                        + "hostnameResolution: \(String(describing: hostnameResolution))"
                )
            }
        }
    }

    /// A configuration with no SSL at all is unaffected by the requirement.
    func testNoSSLIsUnaffected() {
        var configuration = self.makeConfiguration()
        configuration.hostnameResolution = false

        XCTAssertNoThrow(try self.makeCluster(configuration))
    }

    // MARK: - Insecure-configuration warning

    /// Exactly the options that verify no identity are warned about. Asserted as a partition over
    /// `allCases` rather than two hardcoded lists, so a new case is covered without being named here.
    func testWarningCoversExactlyTheFlagsThatVerifyNoIdentity() {
        let expectedToWarn: [CassandraClient.Configuration.SSL.VerifyFlag] = [.none, .peerCert]

        for verifyFlag in CassandraClient.Configuration.SSL.VerifyFlag.allCases {
            var configuration = self.makeConfiguration()
            configuration.ssl = self.makeSSL(verifyFlag: verifyFlag)

            if expectedToWarn.contains(verifyFlag) {
                XCTAssertNotNil(configuration.insecureSSLWarning, "verifyFlag: \(verifyFlag)")
            } else {
                XCTAssertNil(configuration.insecureSSLWarning, "verifyFlag: \(verifyFlag)")
            }
        }
    }

    /// A configuration without SSL is not warned about.
    func testNoSSLIsNotWarnedAbout() {
        XCTAssertNil(self.makeConfiguration().insecureSSLWarning)
    }

    // MARK: - Description

    /// SSL disabled and SSL enabled without verification are distinguishable in the connect log.
    /// Both once rendered as `none`, which is the one line meant to diagnose this.
    func testDescriptionDistinguishesDisabledFromUnverified() {
        var unverified = self.makeConfiguration()
        unverified.ssl = self.makeSSL(verifyFlag: .none)

        XCTAssertNotEqual(unverified.description, self.makeConfiguration().description)
        XCTAssertTrue(self.makeConfiguration().description.contains("ssl: disabled"))
    }

    /// The verify mode reaches the description, so a handshake that starts failing after an upgrade
    /// can be diagnosed from the existing connect log.
    func testDescriptionCarriesTheVerifyMode() {
        var configuration = self.makeConfiguration()
        configuration.ssl = self.makeSSL(verifyFlag: .peerIdentityDNS)

        XCTAssertTrue(configuration.description.contains("peerIdentityDNS"))
    }

    // MARK: - Helpers

    private func flags(for verifyFlag: CassandraClient.Configuration.SSL.VerifyFlag) -> Int32 {
        self.makeSSL(verifyFlag: verifyFlag).cassVerifyFlags
    }

    private func makeSSL(
        verifyFlag: CassandraClient.Configuration.SSL.VerifyFlag
    ) -> CassandraClient.Configuration.SSL {
        var ssl = CassandraClient.Configuration.SSL()
        ssl.verifyFlag = verifyFlag
        return ssl
    }

    /// Builds the cluster and discards it. `Cluster` is not `Sendable` — the library builds it on the event
    /// loop for that reason — so the result is dropped there rather than carried back by `wait()`. The tests
    /// assert on whether building throws; none of them use the cluster.
    private func makeCluster(_ configuration: CassandraClient.Configuration) throws {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { try? eventLoopGroup.syncShutdownGracefully() }
        try configuration.makeCluster(on: eventLoopGroup.next()).map { _ in }.wait()
    }
}
