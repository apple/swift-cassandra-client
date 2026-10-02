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
    public struct Configuration: Sendable {
        public typealias ContactPoints = [String]

        /// Provides the initial `ContactPoints` of the Cassandra cluster.
        /// This can be a subset since each Cassandra instance is capable of discovering its peers.
        public var contactPointsProvider:
            @Sendable (@escaping @Sendable (Result<ContactPoints, Swift.Error>) -> Void) -> Void

        /// The port the cluster listens on.
        public var port: Int32
        /// The native protocol version used to talk to the cluster.
        public var protocolVersion: ProtocolVersion

        /// Authenticates each connection. `nil` connects without authentication.
        ///
        /// Use ``CassandraClient/PasswordAuthenticator`` for username and password authentication, or a
        /// custom ``CassandraClient/Authenticator`` for other SASL mechanisms. The instance is shared across
        /// all connections and invoked concurrently; see ``CassandraClient/Authenticator``.
        public var authenticator: (any CassandraClient.Authenticator)? = nil

        /// SSL configuration. `nil` connects in plain text.
        public var ssl: SSL?
        /// The keyspace the session connects to, used to resolve unqualified table names.
        public var keyspace: String?
        /// Number of driver I/O threads. `nil` leaves the driver's default.
        public var numIOThreads: UInt32?
        /// Timeout for establishing a connection, rounded up to whole milliseconds. `nil` leaves the
        /// driver's default.
        public var connectTimeout: Duration?
        /// Default timeout for a request, rounded up to whole milliseconds. `nil` leaves the driver's
        /// default. A statement or batch can override it.
        public var requestTimeout: Duration?
        /// Timeout for resolving a contact point's hostname, rounded up to whole milliseconds. `nil` leaves
        /// the driver's default.
        public var resolveTimeout: Duration?

        /// Logs a successful query at `.debug` when its latency reaches this threshold. `nil` disables the
        /// check; `.zero`, or a negative duration, logs every success.
        public var slowQueryThreshold: Duration? = nil

        /// Includes bound parameter values in request logs when `true`. Off by default — values are potential PII.
        public var logBoundValues: Bool = false

        /// Maximum length of query text in a log record; longer text is truncated.
        internal static let maxLoggedQueryLength = 500
        /// Maximum length of each bound value in a log record when ``logBoundValues`` is set.
        internal static let maxLoggedValueLength = 50

        /// Number of connections kept open per host. `nil` leaves the driver's default.
        public var coreConnectionsPerHost: UInt32?
        /// Whether to disable Nagle's algorithm on each connection. Default `true`.
        public var tcpNodelay: Bool = true
        /// Whether to enable TCP keepalive on each connection. Default `false`.
        public var tcpKeepalive: Bool = false
        /// Delay before the first keepalive probe, rounded up to whole seconds. Used only when
        /// ``tcpKeepalive`` is `true`. Default `.zero`.
        public var tcpKeepaliveDelay: Duration = .zero
        /// Interval between heartbeat messages on an idle connection, rounded up to whole seconds. `.zero`
        /// disables heartbeats. `nil` leaves the driver's default.
        public var connectionHeartbeatInterval: Duration?
        /// Time without a heartbeat response after which a connection is closed, rounded up to whole
        /// seconds. `nil` leaves the driver's default.
        public var connectionIdleTimeout: Duration?
        /// Whether the driver retrieves and updates schema metadata. Default `true`.
        ///
        /// Encryption context inference reads primary key columns from this metadata, so turning it off
        /// breaks automatic encryption context resolution for prepared statements.
        public var isSchemaMetadataEnabled: Bool = true
        /// Whether to resolve each cluster host's hostname with a reverse DNS lookup. Default `false`.
        ///
        /// Required by ``SSL/CertificateVerification/fullVerification``, which matches the certificate
        /// against that hostname.
        public var hostnameResolution: Bool = false
        /// Whether to shuffle the resolved contact points before connecting. Default `true`.
        public var randomizedContactPoints: Bool = true
        /// The speculative execution policy. `nil` leaves the driver's default.
        public var speculativeExecutionPolicy: SpeculativeExecutionPolicy?
        /// When statements are prepared on hosts other than the one that handled the request. `nil` leaves
        /// the driver's default.
        public var prepareStrategy: PrepareStrategy?
        /// Whether to send the `NO_COMPACT` startup option, which puts `COMPACT STORAGE` tables into
        /// compatibility mode. Default `false`.
        public var isNoCompactEnabled: Bool = false

        /// Enables driver metrics emission. Default `false` (off).
        /// When enabled, the session polls the driver's snapshot and pushes gauges to swift-metrics.
        public var metricsEnabled: Bool = false

        /// Poller cadence. Default 10 seconds. `nil`, `.zero` or a negative duration disables the poller while
        /// leaving ``metricsEnabled`` on; a zero interval would busy-loop the poller.
        public var metricsPollInterval: Duration? = .seconds(10)

        /// Optional session name attached as a `session` dimension on every emitted metric.
        /// Set this to disambiguate metrics when more than one metrics-enabled session runs in a
        /// process, otherwise their identically-named gauges overwrite each other. `nil` = no dimension.
        public var metricsSessionName: String? = nil

        /// Encryptor for transparent column encryption.
        public var encryptor: Encryptor?

        /// Registered encryption schemas.
        public var encryptionSchemas: [String: EncryptionSchema] = [:]

        /// Register an encryption schema for automatic context building during decoding.
        public mutating func registerEncryptionSchema(_ schema: EncryptionSchema) {
            self.encryptionSchemas[schema.registryKey] = schema
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

        /// When the driver starts additional executions of an idempotent request that has not yet completed.
        #if hasAttribute(nonexhaustive)
        @nonexhaustive
        #endif
        public enum SpeculativeExecutionPolicy: Sendable, Hashable {
            /// Starts up to `maxExecutions` additional executions, each `delay` after the previous one. The
            /// delay is rounded up to whole milliseconds.
            case constant(delay: Duration, maxExecutions: Int32)
            /// Starts no speculative executions.
            case disabled
        }

        /// Where statements are prepared beyond the host that handled the prepare request.
        #if hasAttribute(nonexhaustive)
        @nonexhaustive
        #endif
        public enum PrepareStrategy: String, Sendable, Hashable {
            /// Prepares the statement on every host.
            case allHosts
            /// Prepares already-prepared statements on a host when it becomes available again or is added to the
            /// cluster.
            case upOrAddHost
        }

        /// The native protocol version used to talk to the cluster.
        #if hasAttribute(nonexhaustive)
        @nonexhaustive
        #endif
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
            if let authenticator = self.authenticator as? CassandraClient.PasswordAuthenticator {
                try cluster.setCredentials(username: authenticator.username, password: authenticator.password)
            } else if let authenticator = self.authenticator {
                try cluster.setAuthenticator(authenticator)
            }
            if let ssl = self.ssl {
                // The driver matches DNS identity against a hostname it only resolves when hostname
                // resolution is on; without it peers reached by IP carry no hostname and every
                // handshake fails the subject match.
                if ssl.certificateVerification == .fullVerification, !self.hostnameResolution {
                    throw CassandraClient.Error.badParams(
                        "SSL certificateVerification .fullVerification requires hostnameResolution to be true"
                    )
                }
                try cluster.setSSL(try ssl.makeSSLContext())
            }
            if let value = self.numIOThreads {
                try cluster.setNumThreadsIO(value)
            }
            if let value = self.connectTimeout {
                try cluster.setConnectTimeout(try value.driverMilliseconds(UInt32.self, name: "connectTimeout"))
            }
            if let value = self.requestTimeout {
                try cluster.setRequestTimeout(try value.driverMilliseconds(UInt32.self, name: "requestTimeout"))
            }
            if let value = self.resolveTimeout {
                try cluster.setResolveTimeout(try value.driverMilliseconds(UInt32.self, name: "resolveTimeout"))
            }
            if let value = self.coreConnectionsPerHost {
                try cluster.setCoreConnectionsPerHost(value)
            }
            try cluster.setTcpNodelay(self.tcpNodelay)
            try cluster.setTcpKeepalive(
                self.tcpKeepalive,
                delayInSeconds: try self.tcpKeepaliveDelay.driverSeconds(name: "tcpKeepaliveDelay")
            )
            if let value = self.connectionHeartbeatInterval {
                try cluster.setConnectionHeartbeatInterval(try value.driverSeconds(name: "connectionHeartbeatInterval"))
            }
            if let value = self.connectionIdleTimeout {
                try cluster.setConnectionIdleTimeout(try value.driverSeconds(name: "connectionIdleTimeout"))
            }
            try cluster.setUseSchema(self.isSchemaMetadataEnabled)
            try cluster.setUseHostnameResolution(self.hostnameResolution)
            if let loadBalancingStrategy = self.loadBalancingStrategy {
                try cluster.setLoadBalancingStrategy(loadBalancingStrategy)
            }
            try cluster.setUseRandomizedContactPoints(self.randomizedContactPoints)
            switch self.speculativeExecutionPolicy {
            case .constant(let delay, let maxExecutions):
                try cluster.setConstantSpeculativeExecutionPolicy(
                    delayInMilliseconds: try delay.driverMilliseconds(
                        Int64.self,
                        name: "speculativeExecutionPolicy delay"
                    ),
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
            try cluster.setNoCompact(self.isNoCompactEnabled)
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
            switch ssl.certificateVerification {
            case .none:
                return
                    "SSL is enabled with certificateVerification .none: the peer's certificate is not "
                    + "checked at all, leaving the connection open to interception"
            case .noHostnameVerification:
                return
                    "SSL is enabled with certificateVerification .noHostnameVerification: the peer's "
                    + "identity is not verified, so any certificate chaining to trustedCertificates is "
                    + "accepted for any host"
            case .ipAddressVerification, .fullVerification:
                return nil
            }
        }
    }
}

// Redact secrets from every string form. The authenticator typically holds credentials and the SSL
// configuration holds a private key and its password; default reflection would print them through any
// interpolation, `dump(_:)` or `Mirror` of a configuration. Same approach as `Encrypted<T>`.
extension CassandraClient.Configuration: CustomStringConvertible, CustomDebugStringConvertible, CustomReflectable {
    public var description: String {
        """
        [\(CassandraClient.Configuration.self):
        port: \(self.port),
        protocolVersion: \(self.protocolVersion),
        keyspace: \(self.keyspace ?? "none"),
        authenticator: \(self.authenticator == nil ? "none" : "<redacted>"),
        ssl: \(self.ssl.map(\.description) ?? "disabled")]
        """
    }

    public var debugDescription: String { self.description }

    public var customMirror: Mirror {
        Mirror(
            self,
            children: [
                "port": self.port,
                "protocolVersion": self.protocolVersion,
                "keyspace": self.keyspace as Any,
                "authenticator": self.authenticator == nil ? "none" : "<redacted>",
                "ssl": self.ssl as Any,
            ]
        )
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

    func setConstantSpeculativeExecutionPolicy(delayInMilliseconds: Int64, maxExecutions: Int32) throws {
        try self.checkResult {
            cass_cluster_set_constant_speculative_execution_policy(
                self.rawPointer,
                cass_int64_t(delayInMilliseconds),
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
    /// SSL configuration for connections to the cluster.
    public struct SSL: Sendable {
        /// PEM encoded certificates the peer's certificate chain is validated against. The driver loads no
        /// system trust anchors, so `nil` fails every verification mode except
        /// ``CertificateVerification/none``.
        public var trustedCertificates: [String]?
        /// Verification performed on the peer's certificate. Default ``CertificateVerification/ipAddressVerification``.
        public var certificateVerification: CertificateVerification = .ipAddressVerification
        /// PEM encoded client certificate chain, starting with the certificate itself, used to authenticate
        /// the client to the server.
        public var cert: String?
        /// PEM encoded client private key and its password, used to authenticate the client to the server.
        public var privateKey: (key: String, password: String)?

        /// Verification performed on the peer's certificate.
        ///
        /// The driver checks chain validity and peer identity independently, so the identity cases
        /// request both. ``noHostnameVerification`` accepts any certificate that chains to
        /// ``trustedCertificates`` whatever its subject, which does not protect against a
        /// network-position attacker holding another certificate from the same issuer;
        /// ``none`` checks nothing at all.
        ///
        /// Every case except ``none`` validates the chain against ``trustedCertificates``
        /// alone. The driver loads no system trust anchors, so leaving that property `nil` fails
        /// verification rather than falling back to the platform's certificate store.
        #if hasAttribute(nonexhaustive)
        @nonexhaustive
        #endif
        public enum CertificateVerification: String, Sendable, Equatable, CaseIterable {
            /// No verification is performed
            case none
            /// Certificate is present and valid. The peer's identity is not checked.
            case noHostnameVerification
            /// Certificate is present and valid, and the IP address the driver connected to matches
            /// an `iPAddress` subject alternative name on the certificate. That address is the
            /// resolved contact point for the node the driver reaches directly, and the
            /// `system.peers` `rpc_address` for each node discovered from the cluster, so a peer's
            /// certificate has to name its `rpc_address` even when that is not a configured contact
            /// point. Matching consumes no hostname, so
            /// ``CassandraClient/Configuration/hostnameResolution`` only adds a reverse lookup per
            /// connection here.
            case ipAddressVerification
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
            case fullVerification
        }

        public init() {}

        /// The driver verify flags for ``certificateVerification``. The driver reads these as a bitmask and
        /// runs `SSL_get_verify_result` only when `CASS_SSL_VERIFY_PEER_CERT` is set, so the identity
        /// cases set it alongside the subject-match bit; setting a subject-match bit alone would
        /// match the subject without validating the chain.
        internal var cassVerifyFlags: Int32 {
            switch self.certificateVerification {
            case .none:
                return Int32(CASS_SSL_VERIFY_NONE.rawValue)
            case .noHostnameVerification:
                return Int32(CASS_SSL_VERIFY_PEER_CERT.rawValue)
            case .ipAddressVerification:
                return Int32(
                    CASS_SSL_VERIFY_PEER_CERT.rawValue | CASS_SSL_VERIFY_PEER_IDENTITY.rawValue
                )
            case .fullVerification:
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

extension CassandraClient.Configuration.SSL: CustomStringConvertible, CustomDebugStringConvertible,
    CustomReflectable
{
    public var description: String {
        """
        [\(CassandraClient.Configuration.SSL.self):
        certificateVerification: \(self.certificateVerification),
        trustedCertificates: \(self.trustedCertificates?.count ?? 0),
        cert: \(self.cert == nil ? "none" : "set"),
        privateKey: \(self.privateKey == nil ? "none" : "<redacted>")]
        """
    }

    public var debugDescription: String { self.description }

    public var customMirror: Mirror {
        Mirror(
            self,
            children: [
                "trustedCertificates": self.trustedCertificates as Any,
                "certificateVerification": self.certificateVerification,
                "cert": self.cert as Any,
                "privateKey": self.privateKey == nil ? "none" : "<redacted>",
            ]
        )
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

// MARK: - Duration conversion

extension Duration {
    /// Whole milliseconds for a driver parameter, rounded up so a nonzero duration never becomes zero.
    ///
    /// - Throws: ``CassandraClient/Error/badParams(_:)`` if the duration is negative or does not fit `T`.
    internal func driverMilliseconds<T: FixedWidthInteger>(_: T.Type, name: String) throws -> T {
        try self.roundedUp(unitsPerSecond: 1000, attosecondsPerUnit: 1_000_000_000_000_000, as: T.self, name: name)
    }

    /// Whole seconds for a driver parameter, rounded up so a nonzero duration never becomes zero.
    ///
    /// - Throws: ``CassandraClient/Error/badParams(_:)`` if the duration is negative or does not fit `UInt32`.
    internal func driverSeconds(name: String) throws -> UInt32 {
        try self.roundedUp(
            unitsPerSecond: 1,
            attosecondsPerUnit: 1_000_000_000_000_000_000,
            as: UInt32.self,
            name: name
        )
    }

    private func roundedUp<T: FixedWidthInteger>(
        unitsPerSecond: Int64,
        attosecondsPerUnit: Int64,
        as: T.Type,
        name: String
    ) throws -> T {
        let (seconds, attoseconds) = self.components
        guard self >= .zero else {
            throw CassandraClient.Error.badParams("'\(name)' must not be negative, got \(self)")
        }
        var (units, overflow) = seconds.multipliedReportingOverflow(by: unitsPerSecond)
        let partial = attoseconds / attosecondsPerUnit + (attoseconds % attosecondsPerUnit == 0 ? 0 : 1)
        if !overflow {
            (units, overflow) = units.addingReportingOverflow(partial)
        }
        guard !overflow, let value = T(exactly: units) else {
            throw CassandraClient.Error.badParams("'\(name)' is out of range, got \(self)")
        }
        return value
    }
}

// MARK: - Renamed before 1.0

extension CassandraClient.Configuration {
    @available(*, unavailable, renamed: "isSchemaMetadataEnabled")
    public var schema: Bool {
        get { fatalError("unavailable") }
        set { fatalError("unavailable") }
    }
}

extension CassandraClient.Configuration.SSL {
    @available(*, unavailable, renamed: "CertificateVerification")
    public typealias VerifyFlag = CertificateVerification

    @available(*, unavailable, renamed: "certificateVerification")
    public var verifyFlag: CertificateVerification {
        get { fatalError("unavailable") }
        set { fatalError("unavailable") }
    }
}

extension CassandraClient.Configuration.SSL.CertificateVerification {
    @available(*, unavailable, renamed: "noHostnameVerification")
    public static var peerCert: Self { fatalError("unavailable") }

    @available(*, unavailable, renamed: "ipAddressVerification")
    public static var peerIdentity: Self { fatalError("unavailable") }

    @available(*, unavailable, renamed: "fullVerification")
    public static var peerIdentityDNS: Self { fatalError("unavailable") }
}

extension CassandraClient {
    @available(*, unavailable, renamed: "CassandraClient.Batch.Kind")
    public typealias BatchType = Batch.Kind
}
