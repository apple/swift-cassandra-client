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
import XCTest

@testable import CassandraClient

/// Unit tests for the SSL verify-flag mapping and the configuration checks around it. No cluster
/// required — the flags are asserted as the bitmask handed to the driver, and building a cluster
/// opens no connections.
final class SSLConfigurationTests: XCTestCase {
    private static let peerCertBit = Int32(CASS_SSL_VERIFY_PEER_CERT.rawValue)
    private static let peerIdentityBit = Int32(CASS_SSL_VERIFY_PEER_IDENTITY.rawValue)
    private static let peerIdentityDNSBit = Int32(CASS_SSL_VERIFY_PEER_IDENTITY_DNS.rawValue)

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
        XCTAssertEqual(CassandraClient.Configuration.SSL().certificateVerification, .ipAddressVerification)
    }

    // MARK: - Flag mapping

    /// `.none` disables verification entirely, which the driver spells as an empty mask.
    func testNoneMapsToEmptyMask() {
        XCTAssertEqual(self.flags(for: .none), Int32(CASS_SSL_VERIFY_NONE.rawValue))
    }

    /// `.noHostnameVerification` validates the chain and nothing else.
    func testPeerCertMapsToChainValidationOnly() {
        XCTAssertEqual(self.flags(for: .noHostnameVerification), Self.peerCertBit)
    }

    /// `.ipAddressVerification` requests the IP subject match *and* chain validation. The driver gates
    /// `SSL_get_verify_result` on the peer-cert bit, so omitting it would match the subject of a
    /// certificate whose chain was never validated.
    func testPeerIdentityAlsoRequestsChainValidation() {
        let flags = self.flags(for: .ipAddressVerification)
        XCTAssertEqual(flags, Self.peerCertBit | Self.peerIdentityBit)
    }

    /// `.fullVerification` requests the hostname subject match *and* chain validation, for the same
    /// reason as ``testPeerIdentityAlsoRequestsChainValidation``.
    func testPeerIdentityDNSAlsoRequestsChainValidation() {
        let flags = self.flags(for: .fullVerification)
        XCTAssertEqual(flags, Self.peerCertBit | Self.peerIdentityDNSBit)
    }

    /// The mask actually reaches the context the driver is handed. Asserting ``cassVerifyFlags`` alone
    /// would stay green if `makeSSLContext()` stopped applying it.
    func testMakeSSLContextAppliesTheMask() throws {
        for verification in CassandraClient.Configuration.SSL.CertificateVerification.allCases {
            let sslContext = try self.makeSSL(verification: verification).makeSSLContext()
            XCTAssertEqual(
                sslContext.verifyFlags,
                self.flags(for: verification),
                "verification: \(verification)"
            )
        }
    }

    // MARK: - Hostname resolution requirement

    /// `.fullVerification` without hostname resolution is rejected up front rather than failing every
    /// handshake against an unresolved hostname.
    func testPeerIdentityDNSRequiresHostnameResolution() async {
        for hostnameResolution in [false] {
            var configuration = self.makeConfiguration()
            configuration.ssl = self.makeSSL(verification: .fullVerification)
            configuration.hostnameResolution = hostnameResolution

            await assertThrowsErrorAsync(try await self.makeCluster(configuration)) { error in
                XCTAssertEqual(
                    error as? CassandraClient.Error,
                    .badParams(
                        "SSL certificateVerification .fullVerification requires hostnameResolution to be true"
                    ),
                    "hostnameResolution: \(String(describing: hostnameResolution))"
                )
            }
        }
    }

    /// With hostname resolution enabled the driver has a hostname to match, so the pairing is allowed.
    func testPeerIdentityDNSWithHostnameResolutionIsAccepted() async throws {
        var configuration = self.makeConfiguration()
        configuration.ssl = self.makeSSL(verification: .fullVerification)
        configuration.hostnameResolution = true

        try await self.makeCluster(configuration)
    }

    /// The requirement is specific to DNS matching; the other options resolve no hostname and so are
    /// accepted with hostname resolution off.
    func testOtherCertificateVerificationsDoNotRequireHostnameResolution() async {
        for verification in CassandraClient.Configuration.SSL.CertificateVerification.allCases
        where verification != .fullVerification {
            for hostnameResolution in [false] {
                var configuration = self.makeConfiguration()
                configuration.ssl = self.makeSSL(verification: verification)
                configuration.hostnameResolution = hostnameResolution

                do {
                    try await self.makeCluster(configuration)
                } catch {
                    XCTFail(
                        "verification: \(verification), "
                            + "hostnameResolution: \(String(describing: hostnameResolution)): \(error)"
                    )
                }
            }
        }
    }

    /// A configuration with no SSL at all is unaffected by the requirement.
    func testNoSSLIsUnaffected() async throws {
        var configuration = self.makeConfiguration()
        configuration.hostnameResolution = false

        try await self.makeCluster(configuration)
    }

    // MARK: - Insecure-configuration warning

    /// Exactly the options that verify no identity are warned about. Asserted as a partition over
    /// `allCases` rather than two hardcoded lists, so a new case is covered without being named here.
    func testWarningCoversExactlyTheFlagsThatVerifyNoIdentity() {
        let expectedToWarn: [CassandraClient.Configuration.SSL.CertificateVerification] = [
            .none, .noHostnameVerification,
        ]

        for verification in CassandraClient.Configuration.SSL.CertificateVerification.allCases {
            var configuration = self.makeConfiguration()
            configuration.ssl = self.makeSSL(verification: verification)

            if expectedToWarn.contains(verification) {
                XCTAssertNotNil(configuration.insecureSSLWarning, "verification: \(verification)")
            } else {
                XCTAssertNil(configuration.insecureSSLWarning, "verification: \(verification)")
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
        unverified.ssl = self.makeSSL(verification: .none)

        XCTAssertNotEqual(unverified.description, self.makeConfiguration().description)
        XCTAssertTrue(self.makeConfiguration().description.contains("ssl: disabled"))
    }

    /// The verify mode reaches the description, so a handshake that starts failing after an upgrade
    /// can be diagnosed from the existing connect log.
    func testDescriptionCarriesTheVerifyMode() {
        var configuration = self.makeConfiguration()
        configuration.ssl = self.makeSSL(verification: .fullVerification)

        XCTAssertTrue(configuration.description.contains("fullVerification"))
    }

    // MARK: - Helpers

    private func flags(for verification: CassandraClient.Configuration.SSL.CertificateVerification) -> Int32 {
        self.makeSSL(verification: verification).cassVerifyFlags
    }

    private func makeSSL(
        verification: CassandraClient.Configuration.SSL.CertificateVerification
    ) -> CassandraClient.Configuration.SSL {
        var ssl = CassandraClient.Configuration.SSL()
        ssl.certificateVerification = verification
        return ssl
    }

    /// Builds the cluster and discards it. The tests assert on whether building throws; none of them use
    /// the cluster.
    private func makeCluster(_ configuration: CassandraClient.Configuration) async throws {
        _ = try await configuration.makeCluster()
    }
}
