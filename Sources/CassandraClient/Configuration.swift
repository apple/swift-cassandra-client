@_implementationOnly import CDataStaxDriver
import NIO

// TODO: add more config option per C++ cluster impl
public extension CassandraClient {
    struct Configuration: CustomStringConvertible {
        public typealias ContactPoints = [String]

        public var contactPointsProvider: (@escaping (Result<ContactPoints, Swift.Error>) -> Void) -> Void
        public var port: Int32
        public var protocolVersion: ProtocolVersion
        public var username: String?
        public var password: String?
        public var ssl: SSL?
        public var keyspace: String?
        public var numIOThreads: UInt32?
        public var connectTimeoutMillis: UInt32?
        public var requestTimeoutMillis: UInt32?
        public var resolveTimeoutMillis: UInt32?
        public var coreConnectionsPerHost: UInt32?
        public var tcpNodelay: Bool?
        public var tcpKeepalive: Bool?
        public var tcpKeepaliveDelaySeconds: UInt32
        public var connectionHeartbeatInterval: UInt32?
        public var connectionIdleTimeout: UInt32?
        public var schema: Bool?
        public var hostnameResolution: Bool?
        public var randomizedContactPoints: Bool?
        public var speculativeExecutionPolicy: SpeculativeExecutionPolicy?
        public var prepareStrategy: PrepareStrategy?
        public var compact: Bool?

        public enum SpeculativeExecutionPolicy {
            case constant(delayInMillseconds: Int64, maxExecutions: Int32)
            case disabled
        }

        public enum PrepareStrategy {
            case allHosts
            case upOrAddHost
        }

        public enum ProtocolVersion: Int32 {
            case v1 = 1
            case v2 = 2
            case v3 = 3
            case v4 = 4
            case v5 = 5
        }

        public init(contactPointsProvider: @escaping (@escaping (Result<ContactPoints, Swift.Error>) -> Void) -> Void,
                    port: Int32,
                    protocolVersion: ProtocolVersion,
                    username: String? = nil,
                    password: String? = nil,
                    ssl: SSL? = nil,
                    keyspace: String? = nil,
                    numIOThreads: UInt32? = nil,
                    connectTimeoutMillis: UInt32? = nil,
                    requestTimeoutMillis: UInt32? = nil,
                    resolveTimeoutMillis: UInt32? = nil,
                    coreConnectionsPerHost: UInt32? = nil,
                    tcpNodelay: Bool? = nil,
                    tcpKeepalive: Bool? = nil,
                    tcpKeepaliveDelaySeconds: UInt32 = 0,
                    connectionHeartbeatInterval: UInt32? = nil,
                    connectionIdleTimeout: UInt32? = nil,
                    schema: Bool? = nil,
                    hostnameResolution: Bool? = nil,
                    randomizedContactPoints: Bool? = nil,
                    speculativeExecutionPolicy: SpeculativeExecutionPolicy? = nil,
                    prepareStrategy: PrepareStrategy? = nil,
                    compact: Bool? = nil)
        {
            self.contactPointsProvider = contactPointsProvider
            self.port = port
            self.protocolVersion = protocolVersion
            self.username = username
            self.password = password
            self.ssl = ssl
            self.keyspace = keyspace
            self.numIOThreads = numIOThreads
            self.connectTimeoutMillis = connectTimeoutMillis
            self.requestTimeoutMillis = requestTimeoutMillis
            self.resolveTimeoutMillis = resolveTimeoutMillis
            self.coreConnectionsPerHost = coreConnectionsPerHost
            self.tcpNodelay = tcpNodelay
            self.tcpKeepalive = tcpKeepalive
            self.tcpKeepaliveDelaySeconds = tcpKeepaliveDelaySeconds
            self.connectionHeartbeatInterval = connectionHeartbeatInterval
            self.connectionIdleTimeout = connectionIdleTimeout
            self.schema = schema
            self.hostnameResolution = hostnameResolution
            self.randomizedContactPoints = randomizedContactPoints
            self.speculativeExecutionPolicy = speculativeExecutionPolicy
            self.prepareStrategy = prepareStrategy
            self.compact = compact
        }

        internal func makeCluster(on eventLoop: EventLoop) -> EventLoopFuture<Cluster> {
            let clusterPromise = eventLoop.makePromise(of: Cluster.self)
            self.contactPointsProvider { result in
                switch result {
                case .success(let contactPoints):
                    do {
                        let cluster = try self.makeCluster(contactPoints: contactPoints)
                        clusterPromise.succeed(cluster)
                    } catch {
                        clusterPromise.fail(error)
                    }
                case .failure(let error):
                    clusterPromise.fail(error)
                }
            }
            return clusterPromise.futureResult
        }

        #if compiler(>=5.5) && canImport(_Concurrency)
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
        #endif

        private func makeCluster(contactPoints: ContactPoints) throws -> Cluster {
            let cluster = Cluster()

            try contactPoints.forEach { try cluster.addContactPoint($0) }

            try cluster.setPort(self.port)
            try cluster.setProtocolVersion(self.protocolVersion.rawValue)
            if let username = self.username, let password = self.password {
                try cluster.setCredentials(username: username, password: password)
            }
            if let ssl = self.ssl {
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
            if let value = self.randomizedContactPoints {
                try cluster.setUseRandomizedContactPoints(value)
            }
            switch self.speculativeExecutionPolicy {
            case .constant(let delayInMillseconds, let maxExecutions):
                try cluster.setConstantSpeculativeExecutionPolicy(delayInMillseconds: delayInMillseconds, maxExecutions: maxExecutions)
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

            return cluster
        }

        public var description: String {
            return """
            [\(Configuration.self):
            port: \(self.port),
            username: \(self.username ?? "none"),
            password: *****]
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
        try self.checkResult { cass_cluster_set_core_connections_per_host(self.rawPointer, numberOfConnection) }
    }

    func setTcpNodelay(_ enabled: Bool) throws {
        cass_cluster_set_tcp_nodelay(self.rawPointer, enabled ? cass_true : cass_false)
    }

    func setTcpKeepalive(_ enabled: Bool, delayInSeconds: UInt32) throws {
        cass_cluster_set_tcp_keepalive(self.rawPointer, enabled ? cass_true : cass_false, delayInSeconds)
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
        try self.checkResult { cass_cluster_set_use_hostname_resolution(self.rawPointer, enabled ? cass_true : cass_false) }
    }

    func setUseRandomizedContactPoints(_ enabled: Bool) throws {
        try self.checkResult { cass_cluster_set_use_randomized_contact_points(self.rawPointer, enabled ? cass_true : cass_false) }
    }

    func setConstantSpeculativeExecutionPolicy(delayInMillseconds: Int64, maxExecutions: Int32) throws {
        try self.checkResult { cass_cluster_set_constant_speculative_execution_policy(self.rawPointer, cass_int64_t(delayInMillseconds), maxExecutions) }
    }

    func setNoSpeculativeExecutionPolicy() throws {
        try self.checkResult { cass_cluster_set_no_speculative_execution_policy(self.rawPointer) }
    }

    func setPrepareOnAllHosts(_ enabled: Bool) throws {
        try self.checkResult { cass_cluster_set_prepare_on_all_hosts(self.rawPointer, enabled ? cass_true : cass_false) }
    }

    func setPrepareOnUpOrAddHost(_ enabled: Bool) throws {
        try self.checkResult { cass_cluster_set_prepare_on_up_or_add_host(self.rawPointer, enabled ? cass_true : cass_false) }
    }

    func setNoCompact(_ enabled: Bool) throws {
        try self.checkResult { cass_cluster_set_no_compact(self.rawPointer, enabled ? cass_true : cass_false) }
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

public extension CassandraClient.Configuration {
    struct SSL {
        public var trustedCertificates: [String]?
        public var verifyFlag: VerifyFlag?
        public var cert: String?
        public var privateKey: (key: String, password: String)?

        /// Verification performed on the peer's certificate.
        public enum VerifyFlag {
            /// No verification is performed
            case none
            /// Certificate is present and valid
            case peerCert
            /// IP address matches the certificate's common name or one of its subject alternative names.
            /// This implies the certificate is also present.
            case peerIdentity
            /// Hostname matches the certificate's common name or one of its subject alternative names.
            /// This implies the certificate is also present. Hostname resolution must also be enabled.
            case peerIdentityDNS
        }

        public init(trustedCertificates: [String]?) {
            self.trustedCertificates = trustedCertificates
        }

        func makeSSLContext() throws -> SSLContext {
            let sslContext = SSLContext()

            if let trustedCerts = self.trustedCertificates {
                for cert in trustedCerts {
                    try sslContext.addTrustedCert(cert)
                }
            }
            if let verifyFlag = self.verifyFlag {
                switch verifyFlag {
                case .none:
                    sslContext.setVerifyFlags(CASS_SSL_VERIFY_NONE)
                case .peerCert:
                    sslContext.setVerifyFlags(CASS_SSL_VERIFY_PEER_CERT)
                case .peerIdentity:
                    sslContext.setVerifyFlags(CASS_SSL_VERIFY_PEER_IDENTITY)
                case .peerIdentityDNS:
                    sslContext.setVerifyFlags(CASS_SSL_VERIFY_PEER_IDENTITY_DNS)
                }
            }
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

    /// Sets verification performed on the peer's certificate.
    func setVerifyFlags(_ flags: CassSslVerifyFlags_) {
        cass_ssl_set_verify_flags(self.rawPointer, Int32(flags.rawValue))
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
