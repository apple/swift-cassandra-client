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

#if compiler(>=6.2)
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

            "ssl.enabled": true,
            "ssl.trustedCertificates": .init(.stringArray(["cert-one", "cert-two"]), isSecret: false),
            "ssl.certificateVerification": "fullVerification",
            "ssl.cert": "client-cert",
            "ssl.privateKey": "client-key",
            "ssl.privateKeyPassword": "key-password",

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

        let ssl = try #require(config.ssl)
        #expect(ssl.trustedCertificates == ["cert-one", "cert-two"])
        #expect(ssl.certificateVerification == .fullVerification)
        #expect(ssl.cert == "client-cert")
        #expect(ssl.privateKey?.key == "client-key")
        #expect(ssl.privateKey?.password == "key-password")

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

        #expect(config.ssl == nil)
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
                "ssl.enabled": true,
                "ssl.certificateVerification": "noHostnameVerification",
                "loadBalancingStrategy.strategy": "dataCenterAware",
                "loadBalancingStrategy.localDataCenter": "dc1",
            ],
            contactPointsProvider: Self.staticProvider(["discovered"])
        )

        #expect(config.port == 9043)
        #expect(config.protocolVersion == .v3)
        #expect(config.keyspace == "test")
        #expect(config.consistency == .localQuorum)
        #expect(config.ssl?.certificateVerification == .noHostnameVerification)
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

    /// A key replaced before 1.0 throws, naming its replacement, rather than being read as unset — an old
    /// `compact` value would otherwise silently mean the opposite under `isNoCompactEnabled`.
    @Test(
        arguments: [
            ("schema", .init(.bool(true), isSecret: false), "isSchemaMetadataEnabled"),
            ("compact", .init(.bool(true), isSecret: false), "isNoCompactEnabled"),
            ("connectionHeartbeatInterval", .init(.int(45), isSecret: false), "connectionHeartbeatIntervalSeconds"),
            ("connectionIdleTimeout", .init(.int(120), isSecret: false), "connectionIdleTimeoutSeconds"),
            ("ssl.verifyFlag", .init(.string("peerCert"), isSecret: false), "certificateVerification"),
        ] as [(AbsoluteConfigKey, ConfigValue, String)]
    )
    func replacedKeyThrows(key: AbsoluteConfigKey, value: ConfigValue, replacement: String) throws {
        let error = #expect(throws: CassandraClient.ConfigurationError.self) {
            try self.makeConfiguration(adding: [key: value])
        }
        #expect(try #require(error).message.contains(replacement))
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
            "ssl.certificateVerification",
        ] as [AbsoluteConfigKey]
    )
    func unrecognizedEnumeratedValueThrows(key: AbsoluteConfigKey) throws {
        let error = #expect(throws: CassandraClient.ConfigurationError.self) {
            // 'ssl.enabled' so that the SSL scope, and with it 'ssl.certificateVerification', is read at all.
            try self.makeConfiguration(adding: ["ssl.enabled": true, key: "notAValidValue"])
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

    // MARK: - SSL

    @Test
    func sslIsIgnoredWhenNotEnabled() throws {
        let config = try self.makeConfiguration(adding: ["ssl.cert": "client-cert"])
        #expect(config.ssl == nil)
    }

    @Test
    func sslDefaults() throws {
        let config = try self.makeConfiguration(adding: ["ssl.enabled": true])
        let ssl = try #require(config.ssl)
        #expect(ssl.trustedCertificates == nil)
        #expect(ssl.certificateVerification == .ipAddressVerification)
        #expect(ssl.cert == nil)
        #expect(ssl.privateKey == nil)
    }

    @Test
    func sslPrivateKeyWithoutPasswordThrows() {
        #expect(throws: (any Error).self) {
            try self.makeConfiguration(adding: ["ssl.enabled": true, "ssl.privateKey": "client-key"])
        }
    }

    @Test(arguments: [nil, false] as [Bool?])
    func sslPropertiesSetWhileDisabledWarns(enabled: Bool?) throws {
        var values: [AbsoluteConfigKey: ConfigValue] = [
            "ssl.trustedCertificates": .init(.stringArray(["cert-one"]), isSecret: false),
            "ssl.certificateVerification": "fullVerification",
            "ssl.cert": "client-cert",
            "ssl.privateKey": .init(.string("client-key"), isSecret: true),
            "ssl.privateKeyPassword": .init(.string("key-password"), isSecret: true),
        ]
        if let enabled {
            values["ssl.enabled"] = .init(.bool(enabled), isSecret: false)
        }
        let (config, logs) = try self.makeConfigurationCapturingLogs(adding: values)

        #expect(config.ssl == nil)
        let warning = try #require(logs.all.first { $0.level == .warning })
        #expect(logs.all.filter { $0.level == .warning }.count == 1)
        #expect(
            warning.metadata[CassandraClient.ConfigurationLogKey.ignoredKeys]?.description
                == "ssl.trustedCertificates, ssl.certificateVerification, ssl.cert, ssl.privateKey, ssl.privateKeyPassword"
        )
    }

    @Test
    func sslDisabledWithNoSSLPropertiesDoesNotWarn() throws {
        let (config, logs) = try self.makeConfigurationCapturingLogs(adding: ["ssl.enabled": false])
        #expect(config.ssl == nil)
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
#endif
