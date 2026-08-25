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

@available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
extension CassandraClient.Configuration {
    /// Initializes ``CassandraClient/Configuration`` from a `ConfigReader`.
    ///
    /// Unless noted otherwise, each key maps onto the property of the same name, an absent key leaves that
    /// property at its default, and every value is read once here — later provider updates do not affect the
    /// returned configuration. `contactPoints` is the only exception.
    ///
    /// ## Configuration keys:
    /// - `contactPoints` (string array, required): Initial contact points of the Cassandra cluster. Must hold
    ///   at least one entry, none of them blank. Unlike every other key, this one is re-read from
    ///   `configReader` on each cluster creation rather than captured, so a reloading provider's new seeds
    ///   apply to subsequent connections. A re-read that fails validation fails that connection.
    /// - `port` (int, optional, default: 9042): Port the cluster listens on, 1 through 65535.
    /// - `protocolVersion` (int, optional, default: 4): Native protocol version, either 3 or 4.
    /// - `username` (string, optional): Username for plain text authentication. Unused when ``authenticator`` is set in code.
    /// - `password` (string, optional, secret): Password for plain text authentication. Unused when ``authenticator`` is set in code.
    /// - `keyspace` (string, optional): Keyspace the session connects to.
    /// - `numIOThreads` (int, optional): Number of driver IO threads.
    /// - `connectTimeoutMillis` (int, optional): Connection timeout in milliseconds.
    /// - `requestTimeoutMillis` (int, optional): Request timeout in milliseconds.
    /// - `resolveTimeoutMillis` (int, optional): Host resolution timeout in milliseconds.
    /// - `slowQueryThresholdMillis` (int, optional): Latency at or above which a successful query is logged.
    /// - `logBoundValues` (bool, optional): Include bound parameter values in request logs.
    /// - `coreConnectionsPerHost` (int, optional): Number of connections kept open per host.
    /// - `tcpNodelay` (bool, optional): Whether to set tcp no delay on the socket.
    /// - `tcpKeepalive` (bool, optional): Whether to enable TCP keepalive.
    /// - `tcpKeepaliveDelaySeconds` (int, optional): Delay before the first keepalive probe, in seconds.
    /// - `connectionHeartbeatIntervalSeconds` (int, optional): Connection heartbeat interval, in seconds.
    /// - `connectionIdleTimeoutSeconds` (int, optional): Connection idle timeout, in seconds.
    /// - `schema` (bool, optional): Whether the driver maintains schema metadata.
    /// - `hostnameResolution` (bool, optional): Whether to perform reverse DNS lookups on cluster hosts.
    /// - `randomizedContactPoints` (bool, optional): Whether to shuffle the resolved contact points.
    /// - `compact` (bool, optional): Whether to connect in compact mode.
    /// - `consistency` (string, optional): Consistency level, one of the cases from ``CassandraClient/Consistency``.
    /// - `serialConsistency` (string, optional): Serial consistency level for LWT operations, one of the cases from ``CassandraClient/SerialConsistency``.
    /// - `prepareStrategy` (string, optional): When to prepare statements, one of the cases from ``CassandraClient/Configuration/PrepareStrategy``.
    /// - `metricsEnabled` (bool, optional): Whether driver metrics are emitted.
    /// - `metricsPollIntervalMillis` (int, optional): Metrics poller cadence in milliseconds. `0` leaves ``metricsEnabled`` on but stops the poller.
    /// - `metricsSessionName` (string, optional): Value of the `session` dimension on emitted metrics.
    /// - `ssl` (scoped, optional): SSL configuration read by ``CassandraClient/Configuration/SSL/init(configReader:)``.
    ///   Only applied if `ssl.enabled` is `true`. If it is not, but other `ssl` keys are set, those keys are
    ///   ignored and a warning is logged to `logger`.
    /// - `loadBalancingStrategy` (scoped, optional): Load balancing strategy read by
    ///   ``CassandraClient/Configuration/LoadBalancingStrategy/init(configReader:)``. Only applied if
    ///   `loadBalancingStrategy.strategy` is present.
    /// - `speculativeExecutionPolicy` (scoped, optional): Speculative execution policy, one of the cases from ``CassandraClient/Configuration/SpeculativeExecutionPolicy``.
    ///
    /// The ``authenticator``, ``encryptor`` and ``encryptionSchemas`` properties cannot be expressed in
    /// configuration and must be set in code. Setting ``authenticator`` takes precedence over the `username` and `password` read here.
    ///
    /// - Throws: If a value is out of range or is not one of the accepted values for its key, or a required key is missing. `contactPoints` is
    ///   validated here too, but because it is re-read it can also fail later, via the callback passed to ``contactPointsProvider``.
    ///
    /// - Parameters:
    ///   - configReader: The reader to read configuration from.
    ///   - logger: Logger for configuration warnings, such as SSL properties set while SSL is disabled
    public init(configReader: ConfigReader, logger: Logger) throws {
        // Read once here so a malformed list is a configuration error at init rather than a connect-time
        // failure. The value is deliberately discarded: the provider below re-reads on every cluster
        // creation so that a reloading provider's new seeds take effect without a process restart.
        _ = try Self.contactPoints(from: configReader)

        let rawPort = configReader.int(forKey: "port", default: 9042)
        guard let port = Int32(exactly: rawPort), (1...65535).contains(port) else {
            throw CassandraClient.ConfigurationError(
                message: "'port' must be between 1 and 65535, got \(rawPort)"
            )
        }

        // The driver accepts only these two: 1 and 2 are below its lowest supported version and 5 is
        // beta-only, so all three would fail at cluster creation.
        let supportedProtocolVersions: [ProtocolVersion] = [.v3, .v4]
        let rawProtocolVersion = configReader.int(forKey: "protocolVersion", default: 4)
        guard let protocolVersion = Int32(exactly: rawProtocolVersion).flatMap(ProtocolVersion.init(rawValue:)),
            supportedProtocolVersions.contains(protocolVersion)
        else {
            let valids = supportedProtocolVersions.map(\.rawValue.description).joined(separator: ", ")
            throw CassandraClient.ConfigurationError(
                message: "'protocolVersion' \(rawProtocolVersion) is invalid. Valid values: \(valids)"
            )
        }

        self.init(
            contactPointsProvider: { callback in
                // A re-read that no longer validates fails the connection rather than falling back to the
                // seeds from init: silently connecting with stale seeds is far harder to diagnose.
                callback(Result { try Self.contactPoints(from: configReader) })
            },
            port: port,
            protocolVersion: protocolVersion
        )

        self.username = configReader.string(forKey: "username")
        self.password = configReader.string(forKey: "password", isSecret: true)
        self.keyspace = configReader.string(forKey: "keyspace")

        self.numIOThreads = try configReader.uint32(forKey: "numIOThreads")
        self.connectTimeoutMillis = try configReader.uint32(forKey: "connectTimeoutMillis")
        self.requestTimeoutMillis = try configReader.uint32(forKey: "requestTimeoutMillis")
        self.resolveTimeoutMillis = try configReader.uint32(forKey: "resolveTimeoutMillis")

        self.slowQueryThresholdMillis = try configReader.uint32(forKey: "slowQueryThresholdMillis")
        if let value = configReader.bool(forKey: "logBoundValues") {
            self.logBoundValues = value
        }

        self.coreConnectionsPerHost = try configReader.uint32(forKey: "coreConnectionsPerHost")
        self.tcpNodelay = configReader.bool(forKey: "tcpNodelay")
        self.tcpKeepalive = configReader.bool(forKey: "tcpKeepalive")
        if let value = try configReader.uint32(forKey: "tcpKeepaliveDelaySeconds") {
            self.tcpKeepaliveDelaySeconds = value
        }
        self.connectionHeartbeatInterval = try configReader.uint32(forKey: "connectionHeartbeatInterval")
        self.connectionIdleTimeout = try configReader.uint32(forKey: "connectionIdleTimeout")

        self.schema = configReader.bool(forKey: "schema")
        self.hostnameResolution = configReader.bool(forKey: "hostnameResolution")
        self.randomizedContactPoints = configReader.bool(forKey: "randomizedContactPoints")
        self.compact = configReader.bool(forKey: "compact")

        self.consistency = try configReader.string(forKey: "consistency")
        self.serialConsistency = try configReader.string(forKey: "serialConsistency")
        self.prepareStrategy = try configReader.string(forKey: "prepareStrategy")

        if let value = configReader.bool(forKey: "metricsEnabled") {
            self.metricsEnabled = value
        }
        if let value = try configReader.uint32(forKey: "metricsPollIntervalMillis") {
            self.metricsPollIntervalMillis = value
        }
        self.metricsSessionName = configReader.string(forKey: "metricsSessionName")

        let sslConfigReader = configReader.scoped(to: "ssl")
        if let ssl = try SSL(configReader: sslConfigReader) {
            self.ssl = ssl
        } else {
            SSL.warnAboutIgnoredKeys(
                configReader: sslConfigReader,
                logger: logger
            )
        }
        if let strategy = try LoadBalancingStrategy(
            configReader: configReader.scoped(to: "loadBalancingStrategy")
        ) {
            self.loadBalancingStrategy = strategy
        }
        if let policy = try SpeculativeExecutionPolicy(
            configReader: configReader.scoped(to: "speculativeExecutionPolicy")
        ) {
            self.speculativeExecutionPolicy = policy
        }
    }

    /// Reads and validates `contactPoints`.
    ///
    /// - Throws: If the list is empty or holds a blank entry, or the key is missing.
    private static func contactPoints(from configReader: ConfigReader) throws -> ContactPoints {
        let contactPoints = try configReader.requiredStringArray(forKey: "contactPoints")
        guard !contactPoints.isEmpty else {
            throw CassandraClient.ConfigurationError(message: "'contactPoints' must not be empty")
        }
        // A blank contact point is how the driver is told to *clear* its contact points, so it would
        // silently discard the rest of the list rather than fail.
        if let index = contactPoints.firstIndex(where: { $0.allSatisfy(\.isWhitespace) }) {
            throw CassandraClient.ConfigurationError(
                message: "'contactPoints' entry at index \(index) must not be blank"
            )
        }
        return contactPoints
    }
}

@available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
extension CassandraClient.Configuration.SSL {
    /// Initializes SSL configuration from a `ConfigReader`.
    ///
    /// ## Configuration keys:
    /// - `enabled` (bool, optional, default: false): Whether SSL is enabled. If `false`, the initializer
    ///   returns `nil`.
    /// - `trustedCertificates` (string array, optional): PEM encoded certificates used to verify the peer.
    /// - `verifyFlag` (string, optional, default: "default"): Verification performed on the peer's
    ///   certificate, one of "default", "none", "peerCert", "peerIdentity" or "peerIdentityDNS".
    /// - `cert` (string, optional): PEM encoded client certificate chain.
    /// - `privateKey` (string, optional, secret): PEM encoded client private key.
    /// - `privateKeyPassword` (string, secret): Password for `privateKey`. Required when `privateKey` is set.
    ///
    /// - Throws: If `verifyFlag` is not one of the accepted values, or `privateKey` is set without `privateKeyPassword`.
    public init?(configReader: ConfigReader) throws {
        guard configReader.bool(forKey: "enabled", default: false) else {
            return nil
        }
        self.init()

        self.trustedCertificates = configReader.stringArray(forKey: "trustedCertificates")
        if let verifyFlag = try configReader.string(forKey: "verifyFlag", asOrThrow: VerifyFlag.self) {
            self.verifyFlag = verifyFlag
        }

        self.cert = configReader.string(forKey: "cert")
        if let privateKey = configReader.string(forKey: "privateKey", isSecret: true) {
            let password = try configReader.requiredString(forKey: "privateKeyPassword", isSecret: true)
            self.privateKey = (key: privateKey, password: password)
        }
    }

    /// Warns about SSL keys that are set but ignored because SSL is not enabled.
    ///
    /// Configuring certificates and then connecting in plain text is potentially a mistake, so we should make the user aware
    internal static func warnAboutIgnoredKeys(configReader: ConfigReader, logger: Logger) {
        var ignoredKeys: [String] = []
        if configReader.stringArray(forKey: "trustedCertificates") != nil {
            ignoredKeys.append("trustedCertificates")
        }
        if configReader.string(forKey: "verifyFlag") != nil {
            ignoredKeys.append("verifyFlag")
        }
        if configReader.string(forKey: "cert") != nil {
            ignoredKeys.append("cert")
        }
        if configReader.string(forKey: "privateKey", isSecret: true) != nil {
            ignoredKeys.append("privateKey")
        }
        if configReader.string(forKey: "privateKeyPassword", isSecret: true) != nil {
            ignoredKeys.append("privateKeyPassword")
        }
        guard !ignoredKeys.isEmpty else {
            return
        }
        logger.warning(
            "SSL properties are set but 'ssl.enabled' is not true, so they are ignored.",
            metadata: [
                CassandraClient.ConfigurationLogKey.ignoredKeys: .string(
                    ignoredKeys.map { "ssl.\($0)" }.joined(separator: ", ")
                )
            ]
        )
    }
}

@available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
extension CassandraClient.Configuration.LoadBalancingStrategy {
    /// Initializes a load balancing strategy from a `ConfigReader`.
    ///
    /// ## Configuration keys:
    /// - `strategy` (string, optional): The strategy to use, either "roundRobin" or "dataCenterAware". If
    ///   absent, the initializer returns `nil`.
    /// - `localDataCenter` (string, optional): Local data center name. Only supported by the "dataCenterAware" strategy.
    ///
    /// - Throws: If `strategy` is not one of the accepted values, or `localDataCenter` is set for the "roundRobin" strategy.
    public init?(configReader: ConfigReader) throws {
        guard let strategy = configReader.string(forKey: "strategy") else {
            return nil
        }
        let localDataCenter = configReader.string(forKey: "localDataCenter")
        switch strategy {
        case "roundRobin":
            guard localDataCenter == nil else {
                throw CassandraClient.ConfigurationError(
                    message:
                        "'loadBalancingStrategy.localDataCenter' is not supported by the roundRobin strategy"
                )
            }
            self = .roundRobin()
        case "dataCenterAware":
            self = .dataCenterAware(.init(localDataCenter: localDataCenter))
        default:
            throw CassandraClient.ConfigurationError(
                message: "'loadBalancingStrategy.strategy' is not a valid strategy: \(strategy)"
            )
        }
    }
}

@available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
extension CassandraClient.Configuration.SpeculativeExecutionPolicy {
    /// Initializes a speculative execution policy from a `ConfigReader`.
    ///
    /// ## Configuration keys:
    /// - `policy` (string, optional): The policy to use, either "constant" or "disabled". If absent, the
    ///   initializer returns `nil`.
    /// - `delayMillis` (int): Delay before each speculative execution, in milliseconds. Required when
    ///   `policy` is "constant".
    /// - `maxExecutions` (int): Maximum number of speculative executions. Required when `policy` is
    ///   "constant". `0` permits no speculative executions.
    ///
    /// - Throws: If `policy` is not one of the accepted values or `delayMillis` / `maxExecutions` is negative or out of range, or a key required by "constant" is missing.
    public init?(configReader: ConfigReader) throws {
        guard let policy = configReader.string(forKey: "policy") else {
            return nil
        }
        switch policy {
        case "constant":
            let delayInMillseconds = try configReader.requiredInt(forKey: "delayMillis")
            guard delayInMillseconds >= 0 else {
                throw CassandraClient.ConfigurationError(
                    message:
                        "'speculativeExecutionPolicy.delayMillis' must not be negative, got \(delayInMillseconds)"
                )
            }
            let maxExecutions = try configReader.requiredInt32(forKey: "maxExecutions")
            guard maxExecutions >= 0 else {
                throw CassandraClient.ConfigurationError(
                    message: "'speculativeExecutionPolicy.maxExecutions' must not be negative, got \(maxExecutions)"
                )
            }
            self = .constant(delayInMillseconds: Int64(delayInMillseconds), maxExecutions: maxExecutions)
        case "disabled":
            self = .disabled
        default:
            throw CassandraClient.ConfigurationError(
                message: "'speculativeExecutionPolicy.policy' is not a valid policy: \(policy)"
            )
        }
    }
}

@available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
extension ConfigReader {
    fileprivate func uint32(forKey key: ConfigKey) throws -> UInt32? {
        guard let value = self.int(forKey: key) else {
            return nil
        }
        return try Self.narrow(value, to: UInt32.self, forKey: key)
    }

    fileprivate func requiredInt32(forKey key: ConfigKey) throws -> Int32 {
        try Self.narrow(self.requiredInt(forKey: key), to: Int32.self, forKey: key)
    }

    private static func narrow<Value: FixedWidthInteger>(
        _ value: Int,
        to: Value.Type,
        forKey key: ConfigKey
    ) throws -> Value {
        guard let narrowed = Value(exactly: value) else {
            throw CassandraClient.ConfigurationError(
                message: "'\(key)' must be between \(Value.min) and \(Value.max), got \(value)"
            )
        }
        return narrowed
    }

    fileprivate func string<T: RawRepresentable>(
        forKey key: ConfigKey,
        asOrThrow: T.Type = T.self
    ) throws -> T? where T.RawValue == String {
        guard let string = self.string(forKey: key) else {
            return nil  // Value doesn't exist, fine
        }
        // Value does exist, must be valid
        guard let value = T(rawValue: string) else {
            throw CassandraClient.ConfigurationError(
                message: "'\(key)' is not a valid \(T.self): \(string)"
            )
        }
        return value
    }
}
#endif
