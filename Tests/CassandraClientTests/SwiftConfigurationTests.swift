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
    @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
    private func makeConfiguration(
        _ values: [AbsoluteConfigKey: ConfigValue]
    ) throws -> CassandraClient.Configuration {
        try CassandraClient.Configuration(
            configReader: ConfigReader(provider: InMemoryProvider(values: values)),
            logger: .init(label: "test")
        )
    }

    /// A configuration with only the required keys set, for tests that add one key at a time.
    @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
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
    @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
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
    @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
    private func contactPoints(of configuration: CassandraClient.Configuration) throws -> [String] {
        let result = NIOLockedValueBox<Result<CassandraClient.Configuration.ContactPoints, Swift.Error>?>(nil)
        configuration.contactPointsProvider { outcome in
            result.withLockedValue { $0 = outcome }
        }
        return try #require(result.withLockedValue { $0 }).get()
    }

    @Test
    @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
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
            "tcpNodelay": true,
            "tcpKeepalive": true,
            "tcpKeepaliveDelaySeconds": 30,
            "connectionHeartbeatInterval": 45,
            "connectionIdleTimeout": 120,

            "schema": true,
            "hostnameResolution": true,
            "randomizedContactPoints": true,
            "compact": true,

            "consistency": "localQuorum",
            "serialConsistency": "localSerial",
            "prepareStrategy": "allHosts",

            "metricsEnabled": true,
            "metricsPollIntervalMillis": 5000,
            "metricsSessionName": "primary",

            "ssl.enabled": true,
            "ssl.trustedCertificates": .init(.stringArray(["cert-one", "cert-two"]), isSecret: false),
            "ssl.verifyFlag": "peerIdentityDNS",
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
        #expect(config.username == "cassandra")
        #expect(config.password == "secret")
        #expect(config.keyspace == "test")

        #expect(config.numIOThreads == 4)
        #expect(config.connectTimeoutMillis == 5000)
        #expect(config.requestTimeoutMillis == 30000)
        #expect(config.resolveTimeoutMillis == 2000)

        #expect(config.slowQueryThresholdMillis == 250)
        #expect(config.logBoundValues)

        #expect(config.coreConnectionsPerHost == 2)
        #expect(config.tcpNodelay == true)
        #expect(config.tcpKeepalive == true)
        #expect(config.tcpKeepaliveDelaySeconds == 30)
        #expect(config.connectionHeartbeatInterval == 45)
        #expect(config.connectionIdleTimeout == 120)

        #expect(config.schema == true)
        #expect(config.hostnameResolution == true)
        #expect(config.randomizedContactPoints == true)
        #expect(config.compact == true)

        #expect(config.consistency == .localQuorum)
        #expect(config.serialConsistency == .localSerial)
        #expect(config.prepareStrategy == .allHosts)

        #expect(config.metricsEnabled)
        #expect(config.metricsPollIntervalMillis == 5000)
        #expect(config.metricsSessionName == "primary")

        let ssl = try #require(config.ssl)
        #expect(ssl.trustedCertificates == ["cert-one", "cert-two"])
        #expect(ssl.verifyFlag == .peerIdentityDNS)
        #expect(ssl.cert == "client-cert")
        #expect(ssl.privateKey?.key == "client-key")
        #expect(ssl.privateKey?.password == "key-password")

        #expect(config.loadBalancingStrategy == .dataCenterAware(.init(localDataCenter: "dc1")))
        #expect(config.speculativeExecutionPolicy == .constant(delayInMillseconds: 100, maxExecutions: 3))
    }

    @Test
    @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
    func defaultsAreUsedWhenOnlyContactPointsAreSet() throws {
        let config = try self.makeConfiguration(adding: [:])

        #expect(try self.contactPoints(of: config) == ["localhost"])
        #expect(config.port == 9042)
        #expect(config.protocolVersion == .v4)
        #expect(config.username == nil)
        #expect(config.password == nil)
        #expect(config.keyspace == nil)

        #expect(config.numIOThreads == nil)
        #expect(config.connectTimeoutMillis == nil)
        #expect(config.requestTimeoutMillis == nil)
        #expect(config.resolveTimeoutMillis == nil)

        #expect(config.slowQueryThresholdMillis == nil)
        #expect(!config.logBoundValues)

        #expect(config.coreConnectionsPerHost == nil)
        #expect(config.tcpNodelay == nil)
        #expect(config.tcpKeepalive == nil)
        #expect(config.tcpKeepaliveDelaySeconds == 0)
        #expect(config.connectionHeartbeatInterval == nil)
        #expect(config.connectionIdleTimeout == nil)

        #expect(config.schema == nil)
        #expect(config.hostnameResolution == nil)
        #expect(config.randomizedContactPoints == nil)
        #expect(config.compact == nil)

        #expect(config.consistency == nil)
        #expect(config.serialConsistency == nil)
        #expect(config.prepareStrategy == nil)

        #expect(!config.metricsEnabled)
        #expect(config.metricsPollIntervalMillis == 10000)
        #expect(config.metricsSessionName == nil)

        #expect(config.ssl == nil)
        #expect(config.loadBalancingStrategy == nil)
        #expect(config.speculativeExecutionPolicy == nil)
    }

    // MARK: - Contact points

    @Test
    @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
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
    @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
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
    @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
    func invalidContactPointsAreRejected(contactPoints: ConfigValue?) {
        var values: [AbsoluteConfigKey: ConfigValue] = [:]
        if let contactPoints {
            values["contactPoints"] = contactPoints
        }
        #expect(throws: (any Error).self) {
            try self.makeConfiguration(values)
        }
    }

    // MARK: - Port and protocol version

    @Test(arguments: [1, 65535])
    @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
    func portBoundsAreAccepted(port: Int) throws {
        let config = try self.makeConfiguration(adding: ["port": .init(.int(port), isSecret: false)])
        #expect(config.port == Int32(port))
    }

    @Test(arguments: [0, -1, 65536, Int.max])
    @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
    func outOfRangePortThrows(port: Int) {
        #expect(throws: CassandraClient.ConfigurationError.self) {
            try self.makeConfiguration(adding: ["port": .init(.int(port), isSecret: false)])
        }
    }

    @Test(arguments: [0, -1, 6, Int.max])
    @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
    func invalidProtocolVersionThrows(version: Int) {
        #expect(throws: CassandraClient.ConfigurationError.self) {
            try self.makeConfiguration(adding: ["protocolVersion": .init(.int(version), isSecret: false)])
        }
    }

    @Test(arguments: [1, 2, 5])
    @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
    func protocolVersionTheDriverDoesNotSupportThrows(version: Int) {
        // These are all ProtocolVersion cases, but the driver rejects them, so they are caught here
        // rather than at connect time.
        #expect(throws: CassandraClient.ConfigurationError.self) {
            try self.makeConfiguration(adding: ["protocolVersion": .init(.int(version), isSecret: false)])
        }
    }

    @Test(arguments: [3, 4])
    @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
    func supportedProtocolVersionsAreAccepted(version: Int) throws {
        let config = try self.makeConfiguration(
            adding: ["protocolVersion": .init(.int(version), isSecret: false)]
        )
        #expect(config.protocolVersion.rawValue == Int32(version))
    }

    @Test
    @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
    func outOfRangeUInt32Throws() {
        #expect(throws: CassandraClient.ConfigurationError.self) {
            try self.makeConfiguration(adding: ["connectTimeoutMillis": -1])
        }
    }

    // MARK: - Enumerated string values

    @Test(
        arguments: [
            "consistency",
            "serialConsistency",
            "prepareStrategy",
            "ssl.verifyFlag",
        ] as [AbsoluteConfigKey]
    )
    @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
    func unrecognizedEnumeratedValueThrows(key: AbsoluteConfigKey) throws {
        let error = #expect(throws: CassandraClient.ConfigurationError.self) {
            // 'ssl.enabled' so that the SSL scope, and with it 'ssl.verifyFlag', is read at all.
            try self.makeConfiguration(adding: ["ssl.enabled": true, key: "notAValidValue"])
        }
        // The offending key is named, so which of several enumerated keys was wrong is unambiguous.
        // Scoped keys are reported relative to their scope, hence the last component only.
        let message = try #require(error).message
        #expect(message.contains(try #require(key.components.last)))
        #expect(message.contains("notAValidValue"))
    }

    @Test
    @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
    func serialConsistencyRejectsANonSerialLevel() {
        // "quorum" is a valid 'consistency' but not a valid 'serialConsistency'.
        #expect(throws: CassandraClient.ConfigurationError.self) {
            try self.makeConfiguration(adding: ["serialConsistency": "quorum"])
        }
    }

    // MARK: - SSL

    @Test
    @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
    func sslIsIgnoredWhenNotEnabled() throws {
        let config = try self.makeConfiguration(adding: ["ssl.cert": "client-cert"])
        #expect(config.ssl == nil)
    }

    @Test
    @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
    func sslDefaults() throws {
        let config = try self.makeConfiguration(adding: ["ssl.enabled": true])
        let ssl = try #require(config.ssl)
        #expect(ssl.trustedCertificates == nil)
        #expect(ssl.verifyFlag == .peerIdentity)
        #expect(ssl.cert == nil)
        #expect(ssl.privateKey == nil)
    }

    @Test
    @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
    func sslPrivateKeyWithoutPasswordThrows() {
        #expect(throws: (any Error).self) {
            try self.makeConfiguration(adding: ["ssl.enabled": true, "ssl.privateKey": "client-key"])
        }
    }

    @Test(arguments: [nil, false] as [Bool?])
    @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
    func sslPropertiesSetWhileDisabledWarns(enabled: Bool?) throws {
        var values: [AbsoluteConfigKey: ConfigValue] = [
            "ssl.trustedCertificates": .init(.stringArray(["cert-one"]), isSecret: false),
            "ssl.verifyFlag": "peerIdentityDNS",
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
                == "ssl.trustedCertificates, ssl.verifyFlag, ssl.cert, ssl.privateKey, ssl.privateKeyPassword"
        )
    }

    @Test
    @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
    func sslDisabledWithNoSSLPropertiesDoesNotWarn() throws {
        let (config, logs) = try self.makeConfigurationCapturingLogs(adding: ["ssl.enabled": false])
        #expect(config.ssl == nil)
        #expect(logs.all.filter { $0.level >= .warning }.isEmpty)
    }

    // MARK: - Load balancing
    @Test
    @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
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
    @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
    func loadBalancingDataCenterAwareWithoutLocalDataCenter() throws {
        let config = try self.makeConfiguration(adding: ["loadBalancingStrategy.strategy": "dataCenterAware"])
        #expect(config.loadBalancingStrategy == .dataCenterAware(.init()))
    }

    @Test
    @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
    func invalidLoadBalancingStrategyThrows() {
        #expect(throws: CassandraClient.ConfigurationError.self) {
            try self.makeConfiguration(adding: ["loadBalancingStrategy.strategy": "closestHost"])
        }
    }

    // MARK: - Speculative execution

    @Test
    @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
    func speculativeExecutionConstantMissingKeysThrows() {
        #expect(throws: (any Error).self) {
            try self.makeConfiguration(adding: ["speculativeExecutionPolicy.policy": "constant"])
        }
    }

    @Test
    @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
    func speculativeExecutionConstantZeroIsAccepted() throws {
        let config = try self.makeConfiguration(
            adding: [
                "speculativeExecutionPolicy.policy": "constant",
                "speculativeExecutionPolicy.delayMillis": 0,
                "speculativeExecutionPolicy.maxExecutions": 0,
            ]
        )
        #expect(config.speculativeExecutionPolicy == .constant(delayInMillseconds: 0, maxExecutions: 0))
    }

    @Test(arguments: [(-1, 3), (100, -1), (-1, -1)])
    @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
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
    @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
    func invalidSpeculativeExecutionPolicyThrows() {
        #expect(throws: CassandraClient.ConfigurationError.self) {
            try self.makeConfiguration(adding: ["speculativeExecutionPolicy.policy": "exponential"])
        }
    }
}
#endif
