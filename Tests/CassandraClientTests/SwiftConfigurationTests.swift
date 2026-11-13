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

import Configuration
import Logging
import NIOConcurrencyHelpers
import Testing

@testable import CassandraClient

struct SwiftConfigurationTests {
    private func makeConfiguration(
        _ values: [AbsoluteConfigKey: ConfigValue]
    ) throws -> CassandraClient.Configuration {
        try CassandraClient.Configuration(
            configReader: ConfigReader(provider: InMemoryProvider(values: values)),
            logger: .init(label: "test")
        )
    }

    /// A configuration with only the required keys set, for tests that add one key at a time.
    private func makeConfiguration(
        adding values: [AbsoluteConfigKey: ConfigValue]
    ) throws -> CassandraClient.Configuration {
        var all: [AbsoluteConfigKey: ConfigValue] = [
            "contactPoints": .init(.stringArray(["localhost"]), isSecret: false)
        ]
        all.merge(values) { _, new in new }
        return try self.makeConfiguration(all)
    }

    /// As ``makeConfiguration(adding:)``, but captures what the initializer logs.
    private func makeConfigurationCapturingLogs(
        adding values: [AbsoluteConfigKey: ConfigValue]
    ) throws -> (CassandraClient.Configuration, TestLogCapture) {
        var all: [AbsoluteConfigKey: ConfigValue] = [
            "contactPoints": .init(.stringArray(["localhost"]), isSecret: false)
        ]
        all.merge(values) { _, new in new }
        let (logger, capture) = makeCapturingLogger()
        let configuration = try CassandraClient.Configuration(
            configReader: ConfigReader(provider: InMemoryProvider(values: all)),
            logger: logger
        )
        return (configuration, capture)
    }

    /// Resolves the contact points the configuration was built with. The provider synthesised from
    /// configuration is synchronous, so the result is available as soon as it returns.
    private func contactPoints(of configuration: CassandraClient.Configuration) throws -> [String] {
        let result = NIOLockedValueBox<Result<CassandraClient.Configuration.ContactPoints, Swift.Error>?>(nil)
        configuration.contactPointsProvider { outcome in
            result.withLockedValue { $0 = outcome }
        }
        return try #require(result.withLockedValue { $0 }).get()
    }

    /// As ``makeConfiguration(_:)``, but with the contact points supplied in code.
    private func makeConfiguration(
        _ values: [AbsoluteConfigKey: ConfigValue],
        contactPointsProvider:
            @escaping @Sendable (
                @escaping @Sendable (Result<CassandraClient.Configuration.ContactPoints, Swift.Error>) -> Void
            ) -> Void
    ) throws -> CassandraClient.Configuration {
        try CassandraClient.Configuration(
            configReader: ConfigReader(provider: InMemoryProvider(values: values)),
            contactPointsProvider: contactPointsProvider,
            logger: .init(label: "test")
        )
    }

    /// As ``makeConfiguration(_:contactPointsProvider:)``, but captures what the initializer logs.
    private func makeConfigurationCapturingLogs(
        _ values: [AbsoluteConfigKey: ConfigValue],
        contactPointsProvider:
            @escaping @Sendable (
                @escaping @Sendable (Result<CassandraClient.Configuration.ContactPoints, Swift.Error>) -> Void
            ) -> Void
    ) throws -> (CassandraClient.Configuration, TestLogCapture) {
        let (logger, capture) = makeCapturingLogger()
        let configuration = try CassandraClient.Configuration(
            configReader: ConfigReader(provider: InMemoryProvider(values: values)),
            contactPointsProvider: contactPointsProvider,
            logger: logger
        )
        return (configuration, capture)
    }

    /// A provider that yields a fixed set of contact points
    private static func staticProvider(
        _ contactPoints: CassandraClient.Configuration.ContactPoints
    )
        -> @Sendable (@escaping @Sendable (Result<CassandraClient.Configuration.ContactPoints, Swift.Error>) -> Void)
        -> Void
    {
        { callback in callback(.success(contactPoints)) }
    }

    private struct DiscoveryFailure: Swift.Error {}

    @Test
    func allPropertiesAreSetFromConfig() throws {
        let config = try self.makeConfiguration([
            "contactPoints": .init(.stringArray(["localhost", "192.168.1.1"]), isSecret: false),
            "port": 9043,
            "protocolVersion": 3,
            "username": "cassandra",
            "password": "secret",
            "keyspace": "test",

            "numIOThreads": 4,
            "connectTimeoutMillis": 5000,
            "requestTimeoutMillis": 30000,
            "resolveTimeoutMillis": 2000,

            "slowQueryThresholdMillis": 250,
            "logBoundValues": true,

            "coreConnectionsPerHost": 2,
            "tcpNodelay": false,
            "tcpKeepalive": true,
            "tcpKeepaliveDelaySeconds": 30,
            "connectionHeartbeatIntervalSeconds": 45,
            "connectionIdleTimeoutSeconds": 120,

            "isSchemaMetadataEnabled": false,
            "hostnameResolution": true,
            "randomizedContactPoints": false,
            "isNoCompactEnabled": true,

            "consistency": "localQuorum",
            "serialConsistency": "localSerial",
            "prepareStrategy": "allHosts",

            "metricsEnabled": true,
            "metricsPollIntervalMillis": 5000,
            "metricsSessionName": "primary",

            "tls.enabled": true,
            "tls.trustedCertificates": .init(.stringArray(["cert-one", "cert-two"]), isSecret: false),
            "tls.certificateVerification": "fullVerification",
            "tls.certificate": "client-cert",
            "tls.privateKey": "client-key",
            "tls.privateKeyPassword": "key-password",

            "loadBalancingStrategy.strategy": "dataCenterAware",
            "loadBalancingStrategy.localDataCenter": "dc1",

            "speculativeExecutionPolicy.policy": "constant",
            "speculativeExecutionPolicy.delayMillis": 100,
            "speculativeExecutionPolicy.maxExecutions": 3,
        ])

        #expect(try self.contactPoints(of: config) == ["localhost", "192.168.1.1"])
        #expect(config.port == 9043)
        #expect(config.protocolVersion == .v3)
        let authenticator = try #require(config.authenticator as? CassandraClient.PasswordAuthenticator)
        #expect(authenticator.username == "cassandra")
        #expect(authenticator.password == "secret")
        #expect(config.keyspace == "test")

        #expect(config.numIOThreads == 4)
        #expect(config.connectTimeout == .milliseconds(5000))
        #expect(config.requestTimeout == .milliseconds(30000))
        #expect(config.resolveTimeout == .milliseconds(2000))

        #expect(config.slowQueryThreshold == .milliseconds(250))
        #expect(config.logBoundValues)

        #expect(config.coreConnectionsPerHost == 2)
        #expect(!config.tcpNodelay)
        #expect(config.tcpKeepalive)
        #expect(config.tcpKeepaliveDelay == .seconds(30))
        #expect(config.connectionHeartbeatInterval == .seconds(45))
        #expect(config.connectionIdleTimeout == .seconds(120))

        #expect(!config.isSchemaMetadataEnabled)
        #expect(config.hostnameResolution)
        #expect(!config.randomizedContactPoints)
        #expect(config.isNoCompactEnabled)

        #expect(config.consistency == .localQuorum)
        #expect(config.serialConsistency == .localSerial)
        #expect(config.prepareStrategy == .allHosts)

        #expect(config.metricsEnabled)
        #expect(config.metricsPollInterval == .milliseconds(5000))
        #expect(config.metricsSessionName == "primary")

        let tls = try #require(config.transportSecurity.tls)
        #expect(tls.trustedCertificates == ["cert-one", "cert-two"])
        #expect(tls.certificateVerification == .fullVerification)
        #expect(tls.clientCredentials?.certificate == "client-cert")
        #expect(tls.clientCredentials?.privateKey.key == "client-key")
        #expect(tls.clientCredentials?.privateKey.password == "key-password")

        #expect(config.loadBalancingStrategy == .dataCenterAware(.init(localDataCenter: "dc1")))
        #expect(config.speculativeExecutionPolicy == .constant(delay: .milliseconds(100), maxExecutions: 3))
    }

    @Test
    func defaultsAreUsedWhenOnlyContactPointsAreSet() throws {
        let config = try self.makeConfiguration(adding: [:])

        #expect(try self.contactPoints(of: config) == ["localhost"])
        #expect(config.port == 9042)
        #expect(config.protocolVersion == .v4)
        #expect(config.authenticator == nil)
        #expect(config.keyspace == nil)

        #expect(config.numIOThreads == nil)
        #expect(config.connectTimeout == nil)
        #expect(config.requestTimeout == nil)
        #expect(config.resolveTimeout == nil)

        #expect(config.slowQueryThreshold == nil)
        #expect(!config.logBoundValues)

        #expect(config.coreConnectionsPerHost == nil)
        #expect(config.tcpNodelay)
        #expect(!config.tcpKeepalive)
        #expect(config.tcpKeepaliveDelay == .zero)
        #expect(config.connectionHeartbeatInterval == nil)
        #expect(config.connectionIdleTimeout == nil)

        #expect(config.isSchemaMetadataEnabled)
        #expect(!config.hostnameResolution)
        #expect(config.randomizedContactPoints)
        #expect(!config.isNoCompactEnabled)

        #expect(config.consistency == nil)
        #expect(config.serialConsistency == nil)
        #expect(config.prepareStrategy == nil)

        #expect(!config.metricsEnabled)
        #expect(config.metricsPollInterval == .seconds(10))
        #expect(config.metricsSessionName == nil)

        #expect(config.transportSecurity.tls == nil)
        #expect(config.loadBalancingStrategy == nil)
        #expect(config.speculativeExecutionPolicy == nil)
    }

    // MARK: - Contact points

    @Test
    func contactPointsAreRereadOnEachClusterCreation() throws {
        let provider = MutableInMemoryProvider(
            initialValues: ["contactPoints": .init(.stringArray(["seed-one"]), isSecret: false)]
        )
        let config = try CassandraClient.Configuration(
            configReader: ConfigReader(provider: provider),
            logger: .init(label: "test")
        )
        #expect(try self.contactPoints(of: config) == ["seed-one"])

        provider.setValue(
            ConfigValue(.stringArray(["seed-two", "seed-three"]), isSecret: false),
            forKey: "contactPoints"
        )
        #expect(try self.contactPoints(of: config) == ["seed-two", "seed-three"])
    }

    @Test
    func contactPointsRereadFailureIsSurfacedToTheCallback() throws {
        let provider = MutableInMemoryProvider(
            initialValues: ["contactPoints": .init(.stringArray(["seed-one"]), isSecret: false)]
        )
        let config = try CassandraClient.Configuration(
            configReader: ConfigReader(provider: provider),
            logger: .init(label: "test")
        )
        #expect(try self.contactPoints(of: config) == ["seed-one"])

        // Reloaded into an invalid state: the connection must fail rather than reuse "seed-one".
        provider.setValue(ConfigValue(.stringArray([]), isSecret: false), forKey: "contactPoints")
        #expect(throws: CassandraClient.ConfigurationError.self) {
            try self.contactPoints(of: config)
        }
    }

    @Test(
        arguments: [
            nil,
            .init(.stringArray([]), isSecret: false),
            .init(.stringArray([""]), isSecret: false),
            .init(.stringArray(["localhost", " "]), isSecret: false),
            .init(.stringArray(["\t"]), isSecret: false),
        ] as [ConfigValue?]
    )
    func invalidContactPointsAreRejected(contactPoints: ConfigValue?) {
        var values: [AbsoluteConfigKey: ConfigValue] = [:]
        if let contactPoints {
            values["contactPoints"] = contactPoints
        }
        #expect(throws: (any Error).self) {
            try self.makeConfiguration(values)
        }
    }

    // MARK: - Contact points supplied in code

    @Test
    func contactPointsProviderSuppliesTheContactPoints() throws {
        let (config, logs) = try self.makeConfigurationCapturingLogs(
            [:],
            contactPointsProvider: Self.staticProvider(["discovered-one", "discovered-two"])
        )
        #expect(try self.contactPoints(of: config) == ["discovered-one", "discovered-two"])
        #expect(logs.all.filter { $0.level >= .warning }.isEmpty)
    }

    @Test
    func contactPointsProviderFailureIsSurfacedToTheCallback() throws {
        // Discovery failing is the normal transient state for a provider supplied in code, so the error must
        // reach the caller rather than be swallowed into an empty contact point list.
        let config = try self.makeConfiguration(
            [:],
            contactPointsProvider: { callback in callback(.failure(DiscoveryFailure())) }
        )
        #expect(throws: DiscoveryFailure.self) {
            try self.contactPoints(of: config)
        }
    }

    @Test
    func otherPropertiesAreStillReadWhenContactPointsAreSuppliedInCode() throws {
        let config = try self.makeConfiguration(
            [
                "port": 9043,
                "protocolVersion": 3,
                "keyspace": "test",
                "consistency": "localQuorum",
                "tls.enabled": true,
                "tls.certificateVerification": "noHostnameVerification",
                "loadBalancingStrategy.strategy": "dataCenterAware",
                "loadBalancingStrategy.localDataCenter": "dc1",
            ],
            contactPointsProvider: Self.staticProvider(["discovered"])
        )

        #expect(config.port == 9043)
        #expect(config.protocolVersion == .v3)
        #expect(config.keyspace == "test")
        #expect(config.consistency == .localQuorum)
        #expect(config.transportSecurity.tls?.certificateVerification == .noHostnameVerification)
        #expect(config.loadBalancingStrategy == .dataCenterAware(.init(localDataCenter: "dc1")))
    }

    @Test
    func configuredContactPointsAreIgnoredAndWarnedAboutWhenSuppliedInCode() throws {
        let (config, logs) = try self.makeConfigurationCapturingLogs(
            ["contactPoints": .init(.stringArray(["from-config"]), isSecret: false)],
            contactPointsProvider: Self.staticProvider(["from-provider"])
        )

        #expect(try self.contactPoints(of: config) == ["from-provider"])
        let warning = try #require(logs.all.first { $0.level == .warning })
        #expect(logs.all.filter { $0.level == .warning }.count == 1)
        #expect(
            warning.metadata[CassandraClient.ConfigurationLogKey.ignoredKeys]?.description == "contactPoints"
        )
    }

    // MARK: - Port and protocol version

    @Test(arguments: [1, 65535])
    func portBoundsAreAccepted(port: Int) throws {
        let config = try self.makeConfiguration(adding: ["port": .init(.int(port), isSecret: false)])
        #expect(config.port == Int32(port))
    }

    @Test(arguments: [0, -1, 65536, Int.max])
    func outOfRangePortThrows(port: Int) {
        #expect(throws: CassandraClient.ConfigurationError.self) {
            try self.makeConfiguration(adding: ["port": .init(.int(port), isSecret: false)])
        }
    }

    @Test(arguments: [0, -1, 6, Int.max])
    func invalidProtocolVersionThrows(version: Int) {
        #expect(throws: CassandraClient.ConfigurationError.self) {
            try self.makeConfiguration(adding: ["protocolVersion": .init(.int(version), isSecret: false)])
        }
    }

    @Test(arguments: [1, 2, 5])
    func protocolVersionTheDriverDoesNotSupportThrows(version: Int) {
        // These are all ProtocolVersion cases, but the driver rejects them, so they are caught here
        // rather than at connect time.
        #expect(throws: CassandraClient.ConfigurationError.self) {
            try self.makeConfiguration(adding: ["protocolVersion": .init(.int(version), isSecret: false)])
        }
    }

    @Test(arguments: [3, 4])
    func supportedProtocolVersionsAreAccepted(version: Int) throws {
        let config = try self.makeConfiguration(
            adding: ["protocolVersion": .init(.int(version), isSecret: false)]
        )
        #expect(config.protocolVersion.rawValue == Int32(version))
    }

    @Test
    func outOfRangeUInt32Throws() {
        #expect(throws: CassandraClient.ConfigurationError.self) {
            try self.makeConfiguration(adding: ["connectTimeoutMillis": -1])
        }
    }

    // MARK: - Replaced keys

    /// A replaced key throws, naming its replacement, rather than being read as unset — an old
    /// `compact` value would otherwise silently mean the opposite under `isNoCompactEnabled`.
    @Test(
        arguments: [
            ("schema", .init(.bool(true), isSecret: false), "isSchemaMetadataEnabled"),
            ("compact", .init(.bool(true), isSecret: false), "isNoCompactEnabled"),
            ("connectionHeartbeatInterval", .init(.int(45), isSecret: false), "connectionHeartbeatIntervalSeconds"),
            ("connectionIdleTimeout", .init(.int(120), isSecret: false), "connectionIdleTimeoutSeconds"),
            ("ssl.enabled", .init(.bool(true), isSecret: false), "tls.enabled"),
            ("ssl.trustedCertificates", .init(.stringArray(["cert"]), isSecret: false), "tls.trustedCertificates"),
            (
                "ssl.certificateVerification", .init(.string("fullVerification"), isSecret: false),
                "tls.certificateVerification"
            ),
            ("ssl.certificate", .init(.string("client-cert"), isSecret: false), "tls.certificate"),
            ("ssl.privateKey", .init(.string("client-key"), isSecret: true), "tls.privateKey"),
            ("ssl.privateKeyPassword", .init(.string("key-password"), isSecret: true), "tls.privateKeyPassword"),
            ("ssl.verifyFlag", .init(.string("peerCert"), isSecret: false), "tls.certificateVerification"),
            ("ssl.cert", .init(.string("client-cert"), isSecret: false), "tls.certificate"),
        ] as [(AbsoluteConfigKey, ConfigValue, String)]
    )
    func replacedKeyThrows(key: AbsoluteConfigKey, value: ConfigValue, replacement: String) throws {
        let error = #expect(throws: CassandraClient.ConfigurationError.self) {
            try self.makeConfiguration(adding: [key: value])
        }
        let message = try #require(error).message
        #expect(message.contains(replacement))
        #expect(message.contains("'\(key)'"))
        // The message names keys, never their values.
        #expect(!message.contains("client-key"))
        #expect(!message.contains("key-password"))
    }

    // MARK: - Credentials

    @Test(arguments: ["username", "password"] as [AbsoluteConfigKey])
    func credentialWithoutItsPairThrows(key: AbsoluteConfigKey) {
        #expect(throws: CassandraClient.ConfigurationError.self) {
            try self.makeConfiguration(adding: [key: "value"])
        }
    }

    // MARK: - Enumerated string values

    @Test(
        arguments: [
            "consistency",
            "serialConsistency",
            "prepareStrategy",
            "tls.certificateVerification",
        ] as [AbsoluteConfigKey]
    )
    func unrecognizedEnumeratedValueThrows(key: AbsoluteConfigKey) throws {
        let error = #expect(throws: CassandraClient.ConfigurationError.self) {
            // 'tls.enabled' so that the TLS scope, and with it 'tls.certificateVerification', is read at all.
            try self.makeConfiguration(adding: ["tls.enabled": true, key: "notAValidValue"])
        }
        // The offending key is named, so which of several enumerated keys was wrong is unambiguous.
        // Scoped keys are reported relative to their scope, hence the last component only.
        let message = try #require(error).message
        #expect(message.contains(try #require(key.components.last)))
        #expect(message.contains("notAValidValue"))
    }

    @Test
    func serialConsistencyRejectsANonSerialLevel() {
        // "quorum" is a valid 'consistency' but not a valid 'serialConsistency'.
        #expect(throws: CassandraClient.ConfigurationError.self) {
            try self.makeConfiguration(adding: ["serialConsistency": "quorum"])
        }
    }

    // MARK: - TLS

    @Test
    func tlsIsIgnoredWhenNotEnabled() throws {
        let config = try self.makeConfiguration(adding: ["tls.certificate": "client-cert"])
        #expect(config.transportSecurity.tls == nil)
    }

    @Test
    func tlsDefaults() throws {
        let config = try self.makeConfiguration(adding: ["tls.enabled": true])
        let tls = try #require(config.transportSecurity.tls)
        #expect(tls.trustedCertificates == nil)
        #expect(tls.certificateVerification == .ipAddressVerification)
        #expect(tls.clientCredentials == nil)
    }

    @Test
    func tlsPrivateKeyWithoutPasswordThrows() {
        #expect(throws: (any Error).self) {
            try self.makeConfiguration(adding: ["tls.enabled": true, "tls.privateKey": "client-key"])
        }
    }

    /// A certificate without a key, or a key without a certificate, cannot authenticate the client.
    @Test(
        arguments: [
            ["tls.certificate": "client-cert"],
            ["tls.privateKey": "client-key", "tls.privateKeyPassword": "key-password"],
        ] as [[AbsoluteConfigKey: ConfigValue]]
    )
    func tlsClientCredentialsNeedBothHalves(extra: [AbsoluteConfigKey: ConfigValue]) {
        #expect(throws: CassandraClient.ConfigurationError.self) {
            try self.makeConfiguration(adding: extra.merging(["tls.enabled": true]) { $1 })
        }
    }

    @Test(arguments: [nil, false] as [Bool?])
    func tlsPropertiesSetWhileDisabledWarns(enabled: Bool?) throws {
        var values: [AbsoluteConfigKey: ConfigValue] = [
            "tls.trustedCertificates": .init(.stringArray(["cert-one"]), isSecret: false),
            "tls.certificateVerification": "fullVerification",
            "tls.certificate": "client-cert",
            "tls.privateKey": .init(.string("client-key"), isSecret: true),
            "tls.privateKeyPassword": .init(.string("key-password"), isSecret: true),
        ]
        if let enabled {
            values["tls.enabled"] = .init(.bool(enabled), isSecret: false)
        }
        let (config, logs) = try self.makeConfigurationCapturingLogs(adding: values)

        #expect(config.transportSecurity.tls == nil)
        let warning = try #require(logs.all.first { $0.level == .warning })
        #expect(logs.all.filter { $0.level == .warning }.count == 1)
        #expect(
            warning.metadata[CassandraClient.ConfigurationLogKey.ignoredKeys]?.description
                == "tls.trustedCertificates, tls.certificateVerification, tls.certificate, tls.privateKey, tls.privateKeyPassword"
        )
    }

    @Test
    func tlsDisabledWithNoTLSPropertiesDoesNotWarn() throws {
        let (config, logs) = try self.makeConfigurationCapturingLogs(adding: ["tls.enabled": false])
        #expect(config.transportSecurity.tls == nil)
        #expect(logs.all.filter { $0.level >= .warning }.isEmpty)
    }

    // MARK: - Load balancing
    @Test
    func loadBalancingRoundRobinWithLocalDataCenterThrows() {
        #expect(throws: CassandraClient.ConfigurationError.self) {
            try self.makeConfiguration(
                adding: [
                    "loadBalancingStrategy.strategy": "roundRobin",
                    "loadBalancingStrategy.localDataCenter": "dc1",
                ]
            )
        }
    }

    @Test
    func loadBalancingDataCenterAwareWithoutLocalDataCenter() throws {
        let config = try self.makeConfiguration(adding: ["loadBalancingStrategy.strategy": "dataCenterAware"])
        #expect(config.loadBalancingStrategy == .dataCenterAware(.init()))
    }

    @Test
    func invalidLoadBalancingStrategyThrows() {
        #expect(throws: CassandraClient.ConfigurationError.self) {
            try self.makeConfiguration(adding: ["loadBalancingStrategy.strategy": "closestHost"])
        }
    }

    // MARK: - Speculative execution

    @Test
    func speculativeExecutionConstantMissingKeysThrows() {
        #expect(throws: (any Error).self) {
            try self.makeConfiguration(adding: ["speculativeExecutionPolicy.policy": "constant"])
        }
    }

    @Test
    func speculativeExecutionConstantZeroIsAccepted() throws {
        let config = try self.makeConfiguration(
            adding: [
                "speculativeExecutionPolicy.policy": "constant",
                "speculativeExecutionPolicy.delayMillis": 0,
                "speculativeExecutionPolicy.maxExecutions": 0,
            ]
        )
        #expect(config.speculativeExecutionPolicy == .constant(delay: .milliseconds(0), maxExecutions: 0))
    }

    @Test(arguments: [(-1, 3), (100, -1), (-1, -1)])
    func negativeSpeculativeExecutionValuesThrow(delay: Int, maxExecutions: Int) {
        #expect(throws: CassandraClient.ConfigurationError.self) {
            try self.makeConfiguration(
                adding: [
                    "speculativeExecutionPolicy.policy": "constant",
                    "speculativeExecutionPolicy.delayMillis": .init(.int(delay), isSecret: false),
                    "speculativeExecutionPolicy.maxExecutions": .init(.int(maxExecutions), isSecret: false),
                ]
            )
        }
    }

    @Test
    func invalidSpeculativeExecutionPolicyThrows() {
        #expect(throws: CassandraClient.ConfigurationError.self) {
            try self.makeConfiguration(adding: ["speculativeExecutionPolicy.policy": "exponential"])
        }
    }
}
