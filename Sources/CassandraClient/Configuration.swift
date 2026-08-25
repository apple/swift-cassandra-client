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
import NIO

// TODO: add more config option per C++ cluster impl
extension CassandraClient {
    /// Configuration for the ``CassandraClient``.
    public struct Configuration: Sendable, CustomStringConvertible {
        public typealias ContactPoints = [String]

        /// Provides the initial `ContactPoints` of the Cassandra cluster.
        /// This can be a subset since each Cassandra instance is capable of discovering its peers.
        public var contactPointsProvider:
            @Sendable (@escaping @Sendable (Result<ContactPoints, Swift.Error>) -> Void) -> Void

        public var port: Int32
        public var protocolVersion: ProtocolVersion
        public var username: String?
        public var password: String?

        /// A custom SASL authenticator. When set, it takes precedence over ``username``/``password``.
        /// The instance is shared across all connections and invoked concurrently; see
        /// ``CassandraClient/Authenticator``.
        public var authenticator: (any CassandraClient.Authenticator)? = nil

        public var ssl: SSL?
        public var keyspace: String?
        public var numIOThreads: UInt32?
        public var connectTimeoutMillis: UInt32?
        public var requestTimeoutMillis: UInt32?
        public var resolveTimeoutMillis: UInt32?

        /// Logs a successful query at `.debug` when its latency reaches this threshold (ms). `nil` disables
        /// the check; `0` logs every success.
        public var slowQueryThresholdMillis: UInt32? = nil

        /// Includes bound parameter values in request logs when `true`. Off by default — values are potential PII.
        public var logBoundValues: Bool = false

        /// Maximum length of query text in a log record; longer text is truncated.
        internal static let maxLoggedQueryLength = 500
        /// Maximum length of each bound value in a log record when ``logBoundValues`` is set.
        internal static let maxLoggedValueLength = 50

        public var coreConnectionsPerHost: UInt32?
        public var tcpNodelay: Bool?
        public var tcpKeepalive: Bool?
        public var tcpKeepaliveDelaySeconds: UInt32 = 0
        public var connectionHeartbeatInterval: UInt32?
        public var connectionIdleTimeout: UInt32?
        public var schema: Bool?
        public var hostnameResolution: Bool?
        public var randomizedContactPoints: Bool?
        public var speculativeExecutionPolicy: SpeculativeExecutionPolicy?
        public var prepareStrategy: PrepareStrategy?
        public var compact: Bool?

        /// Enables driver metrics emission. Default `false` (off).
        /// When enabled, the session polls the driver's snapshot and pushes gauges to swift-metrics.
        public var metricsEnabled: Bool = false

        /// Poller cadence in milliseconds. Default `10000` (10s). `nil` or `0` disables the poller
        /// while leaving ``metricsEnabled`` on; a `0` interval would busy-loop the poller.
        /// Requires macOS 12 / iOS 15 or newer; on older platforms the poller does not start.
        public var metricsPollIntervalMillis: UInt32? = 10000

        /// Optional session name attached as a `session` dimension on every emitted metric.
        /// Set this to disambiguate metrics when more than one metrics-enabled session runs in a
        /// process, otherwise their identically-named gauges overwrite each other. `nil` = no dimension.
        public var metricsSessionName: String? = nil

        /// Encryptor for transparent column encryption.
        @available(macOS 15.0, iOS 18.0, visionOS 2.0, *)
        public var encryptor: Encryptor? {
            get { self._encryptor as? Encryptor }
            set { self._encryptor = newValue }
        }

        private var _encryptor: (any Sendable)?

        /// Registered encryption schemas.
        @available(macOS 15.0, iOS 18.0, visionOS 2.0, *)
        public var encryptionSchemas: [String: EncryptionSchema] {
            get { self._encryptionSchemas as! [String: EncryptionSchema] }
            set { self._encryptionSchemas = newValue }
        }

        private var _encryptionSchemas: any Sendable = [String: EncryptionSchema]()

        /// Register an encryption schema for automatic context building during decoding.
        @available(macOS 15.0, iOS 18.0, visionOS 2.0, *)
        public mutating func registerEncryptionSchema(_ schema: EncryptionSchema) {
            var schemas = self.encryptionSchemas
            schemas[schema.registryKey] = schema
            self.encryptionSchemas = schemas
        }

        /// Sets the cluster's consistency level. Default is `.localOne`.
        public var consistency: CassandraClient.Consistency?

        /// Sets the cluster's serial consistency level for LWT operations.
        /// Default is `.serial`.
        public var serialConsistency: CassandraClient.SerialConsistency?

        /// The load balancing strategy to use. Default is `nil` which uses ``LoadBalancingStrategy/dataCenterAware(_:)``.
        public var loadBalancingStrategy: LoadBalancingStrategy?

        /// A struct representing the load balancing strategy.
        public struct LoadBalancingStrategy: Sendable, Hashable {
            enum Backing: Hashable {
                case roundRobin(RoundRobin)
                case dataCenterAware(DataCenterAware)
            }
            public struct RoundRobin: Sendable, Hashable {
                public init() {}
            }
            public struct DataCenterAware: Sendable, Hashable {
                /// Sets the local data center name for DC-aware routing policy.
                /// When set, a DC-aware load balancing policy will be used that prioritizes hosts from this data center.
                public var localDataCenter: String?

                /// Creates a new data center aware load balancing strategy.
                ///
                /// - Parameters:
                ///   - localDataCenter: Sets the local data center name for DC-aware routing policy.
                public init(
                    localDataCenter: String? = nil
                ) {
                    self.localDataCenter = localDataCenter
                }
            }

            var backing: Backing

            /// Returns a new round robin load balancing strategy.
            public static func roundRobin(_ roundRobin: RoundRobin = .init()) -> Self {
                .init(backing: .roundRobin(roundRobin))
            }

            /// Returns a new data center aware load balancing strategy.
            public static func dataCenterAware(_ dataCenterAware: DataCenterAware = .init()) -> Self {
                .init(backing: .dataCenterAware(dataCenterAware))
            }

        }

        public enum SpeculativeExecutionPolicy: Sendable, Hashable {
            case constant(delayInMillseconds: Int64, maxExecutions: Int32)
            case disabled
        }

        public enum PrepareStrategy: String, Sendable, Hashable {
            case allHosts
            case upOrAddHost
        }

        public enum ProtocolVersion: Int32, Sendable, CaseIterable {
            case v1 = 1
            case v2 = 2
            case v3 = 3
            case v4 = 4
            case v5 = 5
        }

        @preconcurrency public init(
            contactPointsProvider:
                @escaping @Sendable (@escaping @Sendable (Result<ContactPoints, Swift.Error>) -> Void) ->
                Void,
            port: Int32,
            protocolVersion: ProtocolVersion
        ) {
            self.contactPointsProvider = contactPointsProvider
            self.port = port
            self.protocolVersion = protocolVersion
        }

        internal func makeCluster(on eventLoop: EventLoop) -> EventLoopFuture<Cluster> {
            let clusterPromise = eventLoop.makePromise(of: Cluster.self)
            self.contactPointsProvider { result in
                switch result {
                case .success(let contactPoints):
                    // cluster is not Sendable, so it needs to be created on the eventloop
                    eventLoop.execute {
                        do {
                            let cluster = try self.makeCluster(contactPoints: contactPoints)
                            clusterPromise.assumeIsolated().succeed(cluster)
                        } catch {
                            clusterPromise.fail(error)
                        }
                    }
                case .failure(let error):
                    clusterPromise.fail(error)
                }
            }
            return clusterPromise.futureResult
        }

        @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
        internal func makeCluster() async throws -> Cluster {
            try await withCheckedThrowingContinuation { continuation in
                self.contactPointsProvider { result in
                    switch result {
                    case .success(let contactPoints):
                        do {
                            let cluster = try self.makeCluster(contactPoints: contactPoints)
                            continuation.resume(returning: cluster)
                        } catch {
                            continuation.resume(throwing: error)
                        }
                    case .failure(let error):
                        continuation.resume(throwing: error)
                    }
                }
            }
        }

        private func makeCluster(contactPoints: ContactPoints) throws -> Cluster {
            let cluster = Cluster()

            for contactPoint in contactPoints {
                try cluster.addContactPoint(contactPoint)
            }

            try cluster.setPort(self.port)
            try cluster.setProtocolVersion(self.protocolVersion.rawValue)
            if let authenticator = self.authenticator {
                try cluster.setAuthenticator(authenticator)
            } else if let username = self.username, let password = self.password {
                try cluster.setCredentials(username: username, password: password)
            }
            if let ssl = self.ssl {
                // The driver matches DNS identity against a hostname it only resolves when hostname
                // resolution is on; without it peers reached by IP carry no hostname and every
                // handshake fails the subject match.
                if ssl.verifyFlag == .peerIdentityDNS, self.hostnameResolution != true {
                    throw CassandraClient.Error.badParams(
                        "SSL verifyFlag .peerIdentityDNS requires hostnameResolution to be true"
                    )
                }
                try cluster.setSSL(try ssl.makeSSLContext())
            }
            if let value = self.numIOThreads {
                try cluster.setNumThreadsIO(value)
            }
            if let value = self.connectTimeoutMillis {
                try cluster.setConnectTimeout(value)
            }
            if let value = self.requestTimeoutMillis {
                try cluster.setRequestTimeout(value)
            }
            if let value = self.resolveTimeoutMillis {
                try cluster.setResolveTimeout(value)
            }
            if let value = self.coreConnectionsPerHost {
                try cluster.setCoreConnectionsPerHost(value)
            }
            if let value = self.tcpNodelay {
                try cluster.setTcpNodelay(value)
            }
            if let value = self.tcpKeepalive {
                try cluster.setTcpKeepalive(value, delayInSeconds: self.tcpKeepaliveDelaySeconds)
            }
            if let value = self.connectionHeartbeatInterval {
                try cluster.setConnectionHeartbeatInterval(value)
            }
            if let value = self.connectionIdleTimeout {
                try cluster.setConnectionIdleTimeout(value)
            }
            if let value = self.schema {
                try cluster.setUseSchema(value)
            }
            if let value = self.hostnameResolution {
                try cluster.setUseHostnameResolution(value)
            }
            if let loadBalancingStrategy = self.loadBalancingStrategy {
                try cluster.setLoadBalancingStrategy(loadBalancingStrategy)
            }
            if let value = self.randomizedContactPoints {
                try cluster.setUseRandomizedContactPoints(value)
            }
            switch self.speculativeExecutionPolicy {
            case .constant(let delayInMillseconds, let maxExecutions):
                try cluster.setConstantSpeculativeExecutionPolicy(
                    delayInMillseconds: delayInMillseconds,
                    maxExecutions: maxExecutions
                )
            case .disabled:
                try cluster.setNoSpeculativeExecutionPolicy()
            case .none:
                break
            }
            switch self.prepareStrategy {
            case .allHosts:
                try cluster.setPrepareOnAllHosts(true)
            case .upOrAddHost:
                try cluster.setPrepareOnUpOrAddHost(true)
            case .none:
                break
            }
            if let value = self.compact {
                try cluster.setNoCompact(!value)
            }
            if let value = self.consistency {
                try cluster.setConsistency(value.cassConsistency)
            }
            if let value = self.serialConsistency {
                try cluster.setSerialConsistency(value.cassConsistency)
            }

            return cluster
        }

        /// A warning to log when SSL is enabled but the peer's identity is not verified, otherwise
        /// `nil`. The driver raises its own anti-pattern warning for this, but only for the
        /// no-verification case and only from a startup message a non-DSE cluster never triggers.
        internal var insecureSSLWarning: String? {
            guard let ssl = self.ssl else { return nil }
            switch ssl.verifyFlag {
            case .none:
                return
                    "SSL is enabled with verifyFlag .none: the peer's certificate is not checked at "
                    + "all, leaving the connection open to interception"
            case .peerCert:
                return
                    "SSL is enabled with verifyFlag .peerCert: the peer's identity is not verified, "
                    + "so any certificate chaining to trustedCertificates is accepted for any host"
            case .peerIdentity, .peerIdentityDNS:
                return nil
            }
        }

        public var description: String {
            """
            [\(Configuration.self):
            port: \(self.port),
            username: \(self.username ?? "none"),
            password: *****,
            authenticator: \(self.authenticator == nil ? "none" : "custom"),
            ssl: \(self.ssl.map { "enabled, verify \($0.verifyFlag)" } ?? "disabled")]
            """
        }
    }
}

// MARK: - Cluster

internal final class Cluster {
    let rawPointer: OpaquePointer

    init() {
        self.rawPointer = cass_cluster_new()
    }

    deinit {
        cass_cluster_free(self.rawPointer)
    }

    func addContactPoint(_ contactPoint: String) throws {
        try self.checkResult { cass_cluster_set_contact_points(self.rawPointer, contactPoint) }
    }

    func setPort(_ port: Int32) throws {
        try self.checkResult { cass_cluster_set_port(self.rawPointer, port) }
    }

    func setProtocolVersion(_ protocolVersion: Int32) throws {
        try self.checkResult { cass_cluster_set_protocol_version(self.rawPointer, protocolVersion) }
    }

    func setCredentials(username: String, password: String) throws {
        cass_cluster_set_credentials(self.rawPointer, username, password)
    }

    func clearContactPointers() throws {
        try self.checkResult { cass_cluster_set_contact_points(self.rawPointer, "") }
    }

    func setNumThreadsIO(_ threads: UInt32) throws {
        try self.checkResult { cass_cluster_set_num_threads_io(self.rawPointer, threads) }
    }

    func setConnectTimeout(_ milliseconds: UInt32) throws {
        cass_cluster_set_connect_timeout(self.rawPointer, milliseconds)
    }

    func setRequestTimeout(_ milliseconds: UInt32) throws {
        cass_cluster_set_request_timeout(self.rawPointer, milliseconds)
    }

    func setResolveTimeout(_ milliseconds: UInt32) throws {
        cass_cluster_set_resolve_timeout(self.rawPointer, milliseconds)
    }

    func setCoreConnectionsPerHost(_ numberOfConnection: UInt32) throws {
        try self.checkResult {
            cass_cluster_set_core_connections_per_host(self.rawPointer, numberOfConnection)
        }
    }

    func setTcpNodelay(_ enabled: Bool) throws {
        cass_cluster_set_tcp_nodelay(self.rawPointer, enabled ? cass_true : cass_false)
    }

    func setTcpKeepalive(_ enabled: Bool, delayInSeconds: UInt32) throws {
        cass_cluster_set_tcp_keepalive(
            self.rawPointer,
            enabled ? cass_true : cass_false,
            delayInSeconds
        )
    }

    func setConnectionHeartbeatInterval(_ seconds: UInt32) throws {
        cass_cluster_set_connection_heartbeat_interval(self.rawPointer, seconds)
    }

    func setConnectionIdleTimeout(_ seconds: UInt32) throws {
        cass_cluster_set_connection_idle_timeout(self.rawPointer, seconds)
    }

    func setUseSchema(_ enabled: Bool) throws {
        cass_cluster_set_use_schema(self.rawPointer, enabled ? cass_true : cass_false)
    }

    func setUseHostnameResolution(_ enabled: Bool) throws {
        try self.checkResult {
            cass_cluster_set_use_hostname_resolution(self.rawPointer, enabled ? cass_true : cass_false)
        }
    }

    func setUseRandomizedContactPoints(_ enabled: Bool) throws {
        try self.checkResult {
            cass_cluster_set_use_randomized_contact_points(
                self.rawPointer,
                enabled ? cass_true : cass_false
            )
        }
    }

    func setConstantSpeculativeExecutionPolicy(delayInMillseconds: Int64, maxExecutions: Int32) throws {
        try self.checkResult {
            cass_cluster_set_constant_speculative_execution_policy(
                self.rawPointer,
                cass_int64_t(delayInMillseconds),
                maxExecutions
            )
        }
    }

    func setNoSpeculativeExecutionPolicy() throws {
        try self.checkResult { cass_cluster_set_no_speculative_execution_policy(self.rawPointer) }
    }

    func setPrepareOnAllHosts(_ enabled: Bool) throws {
        try self.checkResult {
            cass_cluster_set_prepare_on_all_hosts(self.rawPointer, enabled ? cass_true : cass_false)
        }
    }

    func setPrepareOnUpOrAddHost(_ enabled: Bool) throws {
        try self.checkResult {
            cass_cluster_set_prepare_on_up_or_add_host(self.rawPointer, enabled ? cass_true : cass_false)
        }
    }

    func setNoCompact(_ enabled: Bool) throws {
        try self.checkResult {
            cass_cluster_set_no_compact(self.rawPointer, enabled ? cass_true : cass_false)
        }
    }

    func setLoadBalancingStrategy(_ strategy: CassandraClient.Configuration.LoadBalancingStrategy) throws {
        switch strategy.backing {
        case .roundRobin:
            cass_cluster_set_load_balance_round_robin(self.rawPointer)
        case .dataCenterAware(let dataCenterAware):
            cass_cluster_set_load_balance_dc_aware(
                self.rawPointer,
                dataCenterAware.localDataCenter,
                0,  // This is deprecated so we are using 0
                cass_false  // This is deprecated so we are using false
            )
        }
    }

    func setConsistency(_ consistency: CassConsistency) throws {
        try self.checkResult { cass_cluster_set_consistency(self.rawPointer, consistency) }
    }

    func setSerialConsistency(_ consistency: CassConsistency) throws {
        try self.checkResult { cass_cluster_set_serial_consistency(self.rawPointer, consistency) }
    }

    func setSSL(_ ssl: SSLContext) throws {
        cass_cluster_set_ssl(self.rawPointer, ssl.rawPointer)
    }

    private func checkResult(body: () -> CassError) throws {
        let result = body()
        guard result == CASS_OK else {
            throw CassandraClient.Error(result, message: "Failed to configure cluster")
        }
    }
}

// MARK: - SSL

extension CassandraClient.Configuration {
    public struct SSL: Sendable {
        public var trustedCertificates: [String]?
        public var verifyFlag: VerifyFlag = .peerIdentity
        public var cert: String?
        public var privateKey: (key: String, password: String)?

        /// Verification performed on the peer's certificate.
        ///
        /// The driver checks chain validity and peer identity independently, so the identity cases
        /// request both. ``VerifyFlag/peerCert`` accepts any certificate that chains to
        /// ``trustedCertificates`` whatever its subject, which does not protect against a
        /// network-position attacker holding another certificate from the same issuer;
        /// ``VerifyFlag/none`` checks nothing at all.
        ///
        /// Every case except ``VerifyFlag/none`` validates the chain against ``trustedCertificates``
        /// alone. The driver loads no system trust anchors, so leaving that property `nil` fails
        /// verification rather than falling back to the platform's certificate store.
        public enum VerifyFlag: String, Sendable, Equatable, CaseIterable {
            /// No verification is performed
            case none
            /// Certificate is present and valid. The peer's identity is not checked.
            case peerCert
            /// Certificate is present and valid, and the IP address the driver connected to matches
            /// an `iPAddress` subject alternative name on the certificate. That address is the
            /// resolved contact point for the node the driver reaches directly, and the
            /// `system.peers` `rpc_address` for each node discovered from the cluster, so a peer's
            /// certificate has to name its `rpc_address` even when that is not a configured contact
            /// point. Matching consumes no hostname, so
            /// ``CassandraClient/Configuration/hostnameResolution`` only adds a reverse lookup per
            /// connection here.
            case peerIdentity
            /// Certificate is present and valid, and the peer's hostname matches a `dNSName` subject
            /// alternative name on the certificate, or its common name when the certificate carries
            /// no subject alternative names at all. Requires
            /// ``CassandraClient/Configuration/hostnameResolution`` to be `true`, because the driver
            /// reaches peers it discovers from the cluster by IP address and resolves their hostname
            /// only when that is enabled.
            ///
            /// That reverse lookup does not require a PTR record. An address without one resolves to
            /// its own numeric form, which then fails the subject match and is reported as a
            /// certificate mismatch rather than a missing PTR record.
            case peerIdentityDNS
        }

        public init() {}

        /// The driver verify flags for ``verifyFlag``. The driver reads these as a bitmask and runs
        /// `SSL_get_verify_result` only when `CASS_SSL_VERIFY_PEER_CERT` is set, so the identity
        /// cases set it alongside the subject-match bit; setting a subject-match bit alone would
        /// match the subject without validating the chain.
        internal var cassVerifyFlags: Int32 {
            switch self.verifyFlag {
            case .none:
                return Int32(CASS_SSL_VERIFY_NONE.rawValue)
            case .peerCert:
                return Int32(CASS_SSL_VERIFY_PEER_CERT.rawValue)
            case .peerIdentity:
                return Int32(
                    CASS_SSL_VERIFY_PEER_CERT.rawValue | CASS_SSL_VERIFY_PEER_IDENTITY.rawValue
                )
            case .peerIdentityDNS:
                return Int32(
                    CASS_SSL_VERIFY_PEER_CERT.rawValue | CASS_SSL_VERIFY_PEER_IDENTITY_DNS.rawValue
                )
            }
        }

        func makeSSLContext() throws -> SSLContext {
            let sslContext = SSLContext()

            if let trustedCerts = trustedCertificates {
                for cert in trustedCerts {
                    try sslContext.addTrustedCert(cert)
                }
            }

            sslContext.setVerifyFlags(self.cassVerifyFlags)

            if let cert = self.cert {
                try sslContext.setCert(cert)
            }
            if let privateKey = self.privateKey {
                try sslContext.setPrivateKey(privateKey.key, password: privateKey.password)
            }

            return sslContext
        }
    }
}

internal final class SSLContext {
    let rawPointer: OpaquePointer

    /// The verify flags last applied through ``setVerifyFlags(_:)``.
    private(set) var verifyFlags: Int32 = Int32(CASS_SSL_VERIFY_PEER_CERT.rawValue)

    init() {
        self.rawPointer = cass_ssl_new()
    }

    deinit {
        cass_ssl_free(self.rawPointer)
    }

    /// Adds a trusted certificate. This is used to verify the peer's certificate.
    func addTrustedCert(_ cert: String) throws {
        try self.checkResult { cass_ssl_add_trusted_cert(self.rawPointer, cert) }
    }

    /// Sets verification performed on the peer's certificate. `flags` is a bitwise OR of
    /// `CassSslVerifyFlags` values. The C API offers no readback, so the mask is retained here.
    func setVerifyFlags(_ flags: Int32) {
        self.verifyFlags = flags
        cass_ssl_set_verify_flags(self.rawPointer, flags)
    }

    /// Sets client-side certificate chain. This is used to authenticate the client on the server-side.
    /// This should contain the entire certificate chain starting with the certificate itself.
    func setCert(_ cert: String) throws {
        try self.checkResult { cass_ssl_set_cert(self.rawPointer, cert) }
    }

    /// Set client-side private key. This is used to authenticate the client on the server-side.
    func setPrivateKey(_ key: String, password: String) throws {
        try self.checkResult { cass_ssl_set_private_key(self.rawPointer, key, password) }
    }

    private func checkResult(body: () -> CassError) throws {
        let result = body()
        guard result == CASS_OK else {
            throw CassandraClient.Error(result, message: "Failed to configure SSL")
        }
    }
}
