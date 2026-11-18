# Client and session

## Overview

- **Area:** Client and session — 135 of the module's public symbols.
- **Note:** This PR exists to collect API review feedback ahead of 1.0. It will be closed rather than merged; anything it settles lands as its own change.
- **Generated from:** `Sources/CassandraClient` at `3780cda`, with Apple Swift version 6.3.1 (swiftlang-6.3.1.1.2 clang-2100.0.123.102). Derived, not maintained by hand — see `api-review/README.md` to regenerate.

Inherited protocol members are listed without their standard-library documentation, grouped under the protocol that declared each. Members the symbol graph does not emit are marked `[reconstructed]`.

## Public API

````swift
/// `CassandraClient` is a wrapper around the [Datastax Cassandra C++ Driver](https://github.com/datastax/cpp-driver)
///  and can be used to run queries against a Cassandra database.
///
/// Use ``withClient(configuration:_:)`` for work with a bounded scope. To own a client for longer,
/// create it with ``init(configuration:)`` and run ``run()``, for example as a service in a
/// `ServiceGroup`.
public final class CassandraClient : Sendable, Service {
    /// Create a new instance of `CassandraClient`.
    ///
    /// The client connects when ``run()`` starts; requests made before then wait for it. A request on a client
    /// whose ``run()`` is never called keeps waiting until its task is cancelled.
    ///
    /// - Parameters:
    ///   - configuration: The  client's ``Configuration``.
    public init(configuration: Configuration)

    /// Execute a batch of statements.
    ///
    /// - Parameters:
    ///   - configuration: Options to apply to the batch.
    ///   - logger: The `Logger` to use. Defaults to the task-local `Logger.current`.
    ///   - build: Closure that adds statements to the batch.
    public func batch(configuration: Batch.Configuration = .init(), logger: Logger = Logger.current, _ build: (inout Batch) async throws -> Void) async throws
    /// Execute insert / update / delete or DDL commands where no result is expected.
    public func execute(_ command: String, parameters: [Statement.Value] = [], options: Statement.Options = .init(), logger: Logger = Logger.current) async throws
    /// Execute a ``PreparedStatement`` with bound parameters.
    ///
    /// - Parameters:
    ///   - prepared: The ``PreparedStatement`` to execute.
    ///   - parameters: The values to bind to the statement's `?` placeholders.
    ///   - options: Statement options (consistency, timeout, encryption context).
    ///   - logger: The `Logger` to use. Defaults to the task-local `Logger.current`.
    ///
    /// - Returns: The resulting ``Rows``.
    public func execute(prepared: PreparedStatement, parameters: [Statement.Value] = [], options: Statement.Options = .init(), logger: Logger = Logger.current) async throws -> Rows
    /// Execute a ``PreparedStatement`` and decode each row into a `Decodable` type.
    ///
    /// - Parameters:
    ///   - prepared: The ``PreparedStatement`` to execute.
    ///   - parameters: The values to bind to the statement's `?` placeholders.
    ///   - options: Statement options (consistency, timeout, encryption context).
    ///   - logger: The `Logger` to use. Defaults to the task-local `Logger.current`.
    ///
    /// - Returns: The decoded rows.
    public func execute<T>(prepared: PreparedStatement, parameters: [Statement.Value] = [], options: Statement.Options = .init(), logger: Logger = Logger.current) async throws -> [T] where T : Decodable
    /// Execute a ``PreparedStatement`` and decode each row into `model`.
    ///
    /// This is equivalent to the sibling `execute(...)` overload that infers `T` purely from the return type,
    /// but spells out the decoded type explicitly at the call site, e.g.
    /// `try await cassandraClient.execute(prepared: statement, withModelType: Model.self)`.
    ///
    /// - Parameters:
    ///   - prepared: The ``PreparedStatement`` to execute.
    ///   - parameters: The values to bind to the statement's `?` placeholders.
    ///   - options: Statement options (consistency, timeout, encryption context).
    ///   - logger: The `Logger` to use. Defaults to the task-local `Logger.current`.
    ///   - model: The type to decode each row into.
    ///
    /// - Returns: The decoded rows.
    public func execute<T>(prepared: PreparedStatement, parameters: [Statement.Value] = [], options: Statement.Options = .init(), logger: Logger = Logger.current, withModelType model: T.Type) async throws -> [T] where T : Decodable
    /// Execute a ``Statement``.
    ///
    /// **All** rows are returned, unless the statement sets a page size with
    /// ``Statement/setPagingSize(_:)``, which limits the result to a single page.
    ///
    /// - Parameters:
    ///   - statement: The ``Statement`` to execute.
    ///   - logger: The `Logger` to use. Defaults to the task-local `Logger.current`.
    ///
    /// - Returns: The resulting ``Rows``.
    public func execute(statement: Statement, logger: Logger = Logger.current) async throws -> Rows
    /// Execute a ``Statement``.
    ///
    /// Resulting rows are paginated.
    ///
    /// - Parameters:
    ///   - statement: The ``Statement`` to execute.
    ///   - pageSize: The maximum number of rows returned per page. Must be positive; a
    ///     non-positive size fails the call with ``CassandraClient/Error/badParams(_:)``.
    ///   - logger: The `Logger` to use. Defaults to the task-local `Logger.current`.
    ///
    /// - Returns: The ``PaginatedRows``.
    public func execute(statement: sending Statement, pageSize: Int32, logger: Logger = Logger.current) async throws -> PaginatedRows
    /// Get the driver's metrics snapshot.
    public func getMetrics() -> CassandraMetrics
    /// Prepare a CQL query for repeated execution.
    ///
    /// The server parses and validates the query once. The returned ``PreparedStatement`` can then be bound with
    /// different parameters and executed multiple times without re-parsing.
    ///
    /// - Parameters:
    ///   - query: The CQL query string with `?` placeholders.
    ///   - encryptionTable: The table name for encryption context resolution. If provided, PK column names are looked up at prepare time.
    ///   - logger: The `Logger` to use. Defaults to the task-local `Logger.current`.
    ///
    /// - Returns: A ``PreparedStatement``.
    public func prepare(_ query: String, encryptionTable: String? = nil, logger: Logger = Logger.current) async throws -> PreparedStatement
    /// Query large data-sets where using an interator helps control memory usage.
    ///
    /// - Important:
    ///   - Advancing the iterator invalidates values retrieved by the previous iteration.
    ///   - Attempting to wrap the ``Rows`` sequence in a list will not work, use the transformer variant instead.
    public func query(_ query: String, parameters: [Statement.Value] = [], options: Statement.Options = .init(), logger: Logger = Logger.current) async throws -> Rows
    /// Query small data-sets that fit into memory. Only use this when it's safe to buffer the entire data-set into memory.
    public func query<T>(_ query: String, parameters: [Statement.Value] = [], options: Statement.Options = .init(), logger: Logger = Logger.current) async throws -> [T] where T : Decodable
    /// Query small data-sets that fit into memory. Only use this when it's safe to buffer the entire data-set into memory.
    public func query<T>(_ query: String, parameters: [Statement.Value] = [], options: Statement.Options = .init(), logger: Logger = Logger.current, transform: @escaping (Row) -> T?) async throws -> [T]
    /// Query small data-sets that fit into memory, decoding each row into `model`.
    ///
    /// This is equivalent to the sibling `query(...)` overload that infers `T` purely from the return type,
    /// but spells out the decoded type explicitly at the call site, e.g.
    /// `try await cassandraClient.query("select ...", withModelType: Model.self)`.
    public func query<T>(_ query: String, parameters: [Statement.Value] = [], options: Statement.Options = .init(), logger: Logger = Logger.current, withModelType model: T.Type) async throws -> [T] where T : Decodable
    /// Query large data-sets where the number of rows fetched at a time is limited by `pageSize`.
    ///
    /// A non-positive `pageSize` fails the call with ``CassandraClient/Error/badParams(_:)``.
    public func query(_ query: String, parameters: sending [Statement.Value] = [], pageSize: Int32, options: Statement.Options = .init(), logger: Logger = Logger.current) async throws -> PaginatedRows
    /// Query large data-sets where the number of rows fetched at a time is limited by `pageSize`,
    /// decoding each row into `T` as the sequence is iterated, rather than materializing the whole
    /// decoded result set as an array.
    ///
    /// A non-positive `pageSize` fails the call with ``CassandraClient/Error/badParams(_:)``.
    ///
    /// - Note: Unlike the raw ``query(_:parameters:pageSize:options:logger:)`` sequence, decoded values
    ///   are independent Swift values and remain valid after the sequence is advanced.
    public func query<T>(_ query: String, parameters: sending [Statement.Value] = [], pageSize: Int32, options: Statement.Options = .init(), logger: Logger = Logger.current) async throws -> AsyncThrowingMapSequence<PaginatedRows, T> where T : Decodable, T : Sendable
    /// Query large data-sets where the number of rows fetched at a time is limited by `pageSize`,
    /// decoding each row into `model` as the sequence is iterated.
    ///
    /// This is equivalent to the sibling `query(...)` overload that infers `T` purely from the return type,
    /// but spells out the decoded type explicitly at the call site, e.g.
    /// `try await cassandraClient.query("select ...", pageSize: 100, withModelType: Model.self)`.
    ///
    /// A non-positive `pageSize` fails the call with ``CassandraClient/Error/badParams(_:)``.
    public func query<T>(_ query: String, parameters: sending [Statement.Value] = [], pageSize: Int32, options: Statement.Options = .init(), logger: Logger = Logger.current, withModelType model: T.Type) async throws -> AsyncThrowingMapSequence<PaginatedRows, T> where T : Decodable, T : Sendable
    /// Connect, run the client's background work until graceful shutdown is triggered or the calling task is
    /// cancelled, then shut the client down.
    ///
    /// Requests need `run()` to be running: they wait until it has connected. The background work is the metrics
    /// poller, when ``Configuration/metricsEnabled`` is set. If connecting fails, `run()` shuts the client down and
    /// throws the error.
    ///
    /// - Throws: ``CassandraClient/Error/disconnected`` if the client is already shut down, and
    ///   ``CassandraClient/Error/invalidState(_:)`` if `run()` has already been called.
    public func run() async throws
    /// Ask the client to stop. ``run()`` shuts the client down and returns.
    ///
    /// Returns immediately, before the client has shut down: wait for ``run()`` or
    /// ``withClient(configuration:_:)`` to return to know it has. Calling it again has no further effect.
    /// Calling it before ``run()`` starts is remembered: ``run()`` then connects, shuts the client down and
    /// returns. Cancelling the task that runs ``run()`` or ``withClient(configuration:_:)`` stops the client the
    /// same way.
    public func triggerGracefulShutdown()
    /// Create a client, run it for the duration of `body`, then shut it down, whether `body` returns or throws.
    ///
    /// If `body` throws, its error is rethrown even when shutting the client down also fails.
    ///
    /// - Parameters:
    ///   - configuration: The client's ``Configuration``.
    ///   - body: The closure to invoke with the client.
    ///
    /// - Returns: The result of `body`.
    public static func withClient<Return>(configuration: Configuration, _ body: (CassandraClient) async throws -> Return) async throws -> Return where Return : ~Copyable

    /// A SASL authenticator. Use ``PasswordAuthenticator`` for username and password; conform a custom
    /// type for other mechanisms (e.g. AWS SigV4 for Amazon Keyspaces, or Kerberos).
    ///
    /// The client drives a SASL (Simple Authentication and Security Layer) handshake: it produces an
    /// initial response, answers any server challenges, then observes success. Set an instance on
    /// ``Configuration/authenticator`` to use it.
    ///
    /// Tokens are opaque bytes. Returned responses are `[UInt8]?` where `nil` sends an empty response;
    /// received challenge/success tokens are always a (possibly empty) `[UInt8]`.
    ///
    /// - Important: A single authenticator instance is shared across every connection the driver opens —
    ///   the initial connection and every reconnect over the session's lifetime — and its methods are
    ///   invoked **concurrently** by the driver's I/O threads. Conforming types must therefore be
    ///   `Sendable` and safe under concurrent invocation. `Sendable` governs transfer between concurrency
    ///   domains, not concurrent entry into these synchronous methods, so **the compiler enforces none of
    ///   this**: an `@unchecked Sendable` conformer over mutable state can compile and still race. A
    ///   mechanism carrying mutable state must serialize its own access.
    ///
    /// A single-token mechanism such as SASL PLAIN implements only ``initialResponse()``:
    ///
    /// ```swift
    /// struct PlaintextAuthenticator: CassandraClient.Authenticator {
    ///     let username: String
    ///     let password: String
    ///     func initialResponse() throws -> [UInt8]? {
    ///         [0x00] + Array(username.utf8) + [0x00] + Array(password.utf8)
    ///     }
    /// }
    /// ```
    public protocol Authenticator : Sendable {
        /// Answer a server challenge. The challenge is always present (possibly empty). Return `nil` when
        /// the client has nothing further to send.
        func evaluateChallenge(_ challenge: [UInt8]) throws -> [UInt8]?
        /// The initial response that begins the SASL handshake. `nil` sends an empty response.
        func initialResponse() throws -> [UInt8]?
        /// Check the final token the server sends when it accepts authentication (always present, may be empty).
        /// Throw to reject it, which fails the connection. Mechanisms with nothing to check can rely on the
        /// default, which does nothing.
        func onSuccess(_ token: [UInt8]) throws
    }

    /// Configuration for the ``CassandraClient``.
    public struct Configuration : CustomDebugStringConvertible, CustomReflectable, CustomStringConvertible, Sendable {
        /// Initializes ``CassandraClient/Configuration`` from a `ConfigReader`, with the cluster's contact
        /// points supplied in code rather than read from configuration.
        ///
        /// Use this initializer when the contact points come from somewhere other than configuration — service
        /// discovery, for example. Every key documented on ``init(configReader:logger:)`` is read in the same way,
        /// except `contactPoints`, which is ignored here. A warning is logged to `logger` if it is set.
        ///
        /// - Throws: If a value is out of range or is not one of the accepted values for its key, or a required
        ///   key is missing.
        ///
        /// - Parameters:
        ///   - configReader: The reader to read configuration from.
        ///   - contactPointsProvider: Provides the initial `ContactPoints` of the Cassandra cluster, and is
        ///     invoked once per cluster creation. This can be a subset since each Cassandra instance is capable
        ///     of discovering its peers.
        ///   - logger: The `Logger` for configuration warnings, such as TLS properties set while TLS is disabled.
        ///     Defaults to the task-local `Logger.current`.
        public init(configReader: ConfigReader, contactPointsProvider: @escaping @Sendable (@escaping @Sendable (Result<ContactPoints, Swift.Error>) -> Void) -> Void, logger: Logger = Logger.current) throws
        /// Initializes ``CassandraClient/Configuration`` from a `ConfigReader`.
        ///
        /// Unless noted otherwise, each key maps onto the property of the same name, an absent key leaves that
        /// property at its default, and every value is read once here — later provider updates do not affect the
        /// returned configuration. `contactPoints` is the only exception.
        ///
        /// To supply the contact points in code instead — e.g. from service discovery — use
        /// ``init(configReader:contactPointsProvider:logger:)``, which reads every other key exactly as this
        /// initializer does.
        ///
        /// ## Configuration keys:
        /// - `contactPoints` (string array, required): Initial contact points of the Cassandra cluster. Must hold
        ///   at least one entry, none of them blank. Unlike every other key, this one is re-read from
        ///   `configReader` on each cluster creation rather than captured, so a reloading provider's new seeds
        ///   apply to subsequent connections. A re-read that fails validation fails that connection.
        /// - `port` (int, optional, default: 9042): Port the cluster listens on, 1 through 65535.
        /// - `protocolVersion` (int, optional, default: 4): Native protocol version, either 3 or 4.
        /// - `username` (string, optional): Username for password authentication. Together with `password`, sets
        ///   ``authenticator`` to a ``CassandraClient/PasswordAuthenticator``. Requires `password`.
        /// - `password` (string, optional, secret): Password for password authentication. Requires `username`.
        /// - `keyspace` (string, optional): Keyspace the session connects to.
        /// - `numIOThreads` (int, optional): Number of driver IO threads.
        /// - `connectTimeoutMillis` (int, optional): ``connectTimeout``, in milliseconds.
        /// - `requestTimeoutMillis` (int, optional): ``requestTimeout``, in milliseconds.
        /// - `resolveTimeoutMillis` (int, optional): ``resolveTimeout``, in milliseconds.
        /// - `slowQueryThresholdMillis` (int, optional): ``slowQueryThreshold``, in milliseconds.
        /// - `logBoundValues` (bool, optional): Include bound parameter values in request logs.
        /// - `coreConnectionsPerHost` (int, optional): Number of connections kept open per host.
        /// - `tcpNodelay` (bool, optional, default: true): Whether to disable Nagle's algorithm on each connection.
        /// - `tcpKeepalive` (bool, optional, default: false): Whether to enable TCP keepalive.
        /// - `tcpKeepaliveDelaySeconds` (int, optional): ``tcpKeepaliveDelay``, in seconds.
        /// - `connectionHeartbeatIntervalSeconds` (int, optional): ``connectionHeartbeatInterval``, in seconds.
        /// - `connectionIdleTimeoutSeconds` (int, optional): ``connectionIdleTimeout``, in seconds.
        /// - `isSchemaMetadataEnabled` (bool, optional, default: true): Whether the driver retrieves and updates
        ///   schema metadata.
        /// - `hostnameResolution` (bool, optional, default: false): Whether to perform reverse DNS lookups on cluster hosts.
        /// - `randomizedContactPoints` (bool, optional, default: true): Whether to shuffle the resolved contact points.
        /// - `isNoCompactEnabled` (bool, optional, default: false): Whether to send the `NO_COMPACT` startup option.
        /// - `consistency` (string, optional): Consistency level, one of the levels of ``CassandraClient/Consistency``.
        /// - `serialConsistency` (string, optional): Serial consistency level for LWT operations, one of the levels of ``CassandraClient/SerialConsistency``.
        /// - `prepareStrategy` (string, optional): When to prepare statements, one of the cases from ``CassandraClient/Configuration/PrepareStrategy``.
        /// - `metricsEnabled` (bool, optional): Whether driver metrics are emitted.
        /// - `metricsPollIntervalMillis` (int, optional): ``metricsPollInterval``, in milliseconds. `0` leaves ``metricsEnabled`` on but stops the poller.
        /// - `metricsSessionName` (string, optional): Value of the `session` dimension on emitted metrics.
        /// - `tls` (scoped, optional): TLS configuration read by ``CassandraClient/Configuration/TLS/init(configReader:)``.
        ///   Only applied if `tls.enabled` is `true`. If it is not, but other `tls` keys are set, those keys are
        ///   ignored and a warning is logged to `logger`.
        /// - `loadBalancingStrategy` (scoped, optional): Load balancing strategy read by
        ///   ``CassandraClient/Configuration/LoadBalancingStrategy/init(configReader:)``. Only applied if
        ///   `loadBalancingStrategy.strategy` is present.
        /// - `speculativeExecutionPolicy` (scoped, optional): Speculative execution policy, one of the cases from ``CassandraClient/Configuration/SpeculativeExecutionPolicy``.
        ///
        /// A custom ``authenticator``, ``encryptor`` and ``encryptionSchemas`` cannot be expressed in
        /// configuration and must be set in code.
        ///
        /// The keys `schema`, `compact`, `connectionHeartbeatInterval` and `connectionIdleTimeout` were replaced, and
        /// the `ssl` scope was renamed to `tls`. Setting one throws, naming its replacement, rather than being ignored.
        ///
        /// - Throws: If a value is out of range or is not one of the accepted values for its key, a required key is
        ///   missing, or a replaced key is set. `contactPoints` is
        ///   validated here too, but because it is re-read it can also fail later, via the callback passed to ``contactPointsProvider``.
        ///
        /// - Parameters:
        ///   - configReader: The reader to read configuration from.
        ///   - logger: The `Logger` for configuration warnings, such as TLS properties set while TLS is disabled.
        ///     Defaults to the task-local `Logger.current`.
        public init(configReader: ConfigReader, logger: Logger = Logger.current) throws
        public init(contactPointsProvider: @escaping @Sendable (@escaping @Sendable (Result<ContactPoints, Swift.Error>) -> Void) -> Void, port: Int32, protocolVersion: ProtocolVersion)

        /// Authenticates each connection. `nil` connects without authentication.
        ///
        /// Use ``CassandraClient/PasswordAuthenticator`` for username and password authentication, or a
        /// custom ``CassandraClient/Authenticator`` for other SASL mechanisms. The instance is shared across
        /// all connections and invoked concurrently; see ``CassandraClient/Authenticator``.
        public var authenticator: (any CassandraClient.Authenticator)?
        /// Timeout for establishing a connection, rounded up to whole milliseconds. `nil` leaves the
        /// driver's default.
        public var connectTimeout: Duration?
        /// Interval between heartbeat messages on an idle connection, rounded up to whole seconds. `.zero`
        /// disables heartbeats. `nil` leaves the driver's default.
        public var connectionHeartbeatInterval: Duration?
        /// Time without a heartbeat response after which a connection is closed, rounded up to whole
        /// seconds. `nil` leaves the driver's default.
        public var connectionIdleTimeout: Duration?
        /// Sets the cluster's consistency level. Default is `.localOne`.
        public var consistency: CassandraClient.Consistency?
        /// Provides the initial `ContactPoints` of the Cassandra cluster.
        /// This can be a subset since each Cassandra instance is capable of discovering its peers.
        public var contactPointsProvider: @Sendable (@escaping @Sendable (Result<ContactPoints, Swift.Error>) -> Void) -> Void
        /// Number of connections kept open per host. `nil` leaves the driver's default.
        public var coreConnectionsPerHost: UInt32?
        public var customMirror: Mirror { get }
        public var debugDescription: String { get }
        public var description: String { get }
        /// Registered encryption schemas.
        public var encryptionSchemas: [String : EncryptionSchema]
        /// Encryptor for transparent column encryption.
        public var encryptor: Encryptor?
        /// Whether to resolve each cluster host's hostname with a reverse DNS lookup. Default `false`.
        ///
        /// Required by ``TLS/CertificateVerification/fullVerification``, which matches the certificate
        /// against that hostname.
        public var hostnameResolution: Bool
        /// Whether to send the `NO_COMPACT` startup option, which puts `COMPACT STORAGE` tables into
        /// compatibility mode. Default `false`.
        public var isNoCompactEnabled: Bool
        /// Whether the driver retrieves and updates schema metadata. Default `true`.
        ///
        /// Encryption context inference reads primary key columns from this metadata, so turning it off
        /// breaks automatic encryption context resolution for prepared statements.
        public var isSchemaMetadataEnabled: Bool
        /// The keyspace the session connects to, used to resolve unqualified table names.
        public var keyspace: String?
        /// The load balancing strategy to use. Default is `nil` which uses ``LoadBalancingStrategy/dataCenterAware(_:)``.
        public var loadBalancingStrategy: LoadBalancingStrategy?
        /// Includes bound parameter values in request logs when `true`. Off by default — values are potential PII.
        public var logBoundValues: Bool
        /// Enables driver metrics emission. Default `false` (off).
        /// When enabled, the session polls the driver's snapshot and pushes gauges to swift-metrics.
        public var metricsEnabled: Bool
        /// Poller cadence. Default 10 seconds. `nil`, `.zero` or a negative duration disables the poller while
        /// leaving ``metricsEnabled`` on; a zero interval would busy-loop the poller.
        public var metricsPollInterval: Duration?
        /// Optional session name attached as a `session` dimension on every emitted metric.
        /// Set this to disambiguate metrics when more than one metrics-enabled session runs in a
        /// process, otherwise their identically-named gauges overwrite each other. `nil` = no dimension.
        public var metricsSessionName: String?
        /// Number of driver I/O threads. `nil` leaves the driver's default.
        public var numIOThreads: UInt32?
        /// The port the cluster listens on.
        public var port: Int32
        /// When statements are prepared on hosts other than the one that handled the request. `nil` leaves
        /// the driver's default.
        public var prepareStrategy: PrepareStrategy?
        /// The native protocol version used to talk to the cluster.
        public var protocolVersion: ProtocolVersion
        /// Whether to shuffle the resolved contact points before connecting. Default `true`.
        public var randomizedContactPoints: Bool
        /// Default timeout for a request, rounded up to whole milliseconds. `nil` leaves the driver's
        /// default. A statement or batch can override it.
        public var requestTimeout: Duration?
        /// Timeout for resolving a contact point's hostname, rounded up to whole milliseconds. `nil` leaves
        /// the driver's default.
        public var resolveTimeout: Duration?
        /// Sets the cluster's serial consistency level for LWT operations.
        /// Default is `.serial`.
        public var serialConsistency: CassandraClient.SerialConsistency?
        /// Logs a successful query at `.debug` when its latency reaches this threshold. `nil` disables the
        /// check; `.zero`, or a negative duration, logs every success.
        public var slowQueryThreshold: Duration?
        /// The speculative execution policy. `nil` leaves the driver's default.
        public var speculativeExecutionPolicy: SpeculativeExecutionPolicy?
        /// Whether to enable TCP keepalive on each connection. Default `false`.
        public var tcpKeepalive: Bool
        /// Delay before the first keepalive probe, rounded up to whole seconds. Used only when
        /// ``tcpKeepalive`` is `true`. Default `.zero`.
        public var tcpKeepaliveDelay: Duration
        /// Whether to disable Nagle's algorithm on each connection. Default `true`.
        public var tcpNodelay: Bool
        /// Whether and how connections to the cluster are encrypted. Default ``TransportSecurity/plaintext``.
        public var transportSecurity: TransportSecurity

        public typealias ContactPoints = [String]
        /// Register an encryption schema for automatic context building during decoding.
        public mutating func registerEncryptionSchema(_ schema: EncryptionSchema)

        /// A struct representing the load balancing strategy.
        public struct LoadBalancingStrategy : Hashable, Sendable {
            /// Initializes a load balancing strategy from a `ConfigReader`.
            ///
            /// ## Configuration keys:
            /// - `strategy` (string, optional): The strategy to use, either "roundRobin" or "dataCenterAware". If
            ///   absent, the initializer returns `nil`.
            /// - `localDataCenter` (string, optional): Local data center name. Only supported by the "dataCenterAware" strategy.
            ///
            /// - Throws: If `strategy` is not one of the accepted values, or `localDataCenter` is set for the "roundRobin" strategy.
            public init?(configReader: ConfigReader) throws

            /// Returns a new data center aware load balancing strategy.
            public static func dataCenterAware(_ dataCenterAware: DataCenterAware = .init()) -> CassandraClient.Configuration.LoadBalancingStrategy
            /// Returns a new round robin load balancing strategy.
            public static func roundRobin(_ roundRobin: RoundRobin = .init()) -> CassandraClient.Configuration.LoadBalancingStrategy

            public struct DataCenterAware : Hashable, Sendable {
                /// Creates a new data center aware load balancing strategy.
                ///
                /// - Parameters:
                ///   - localDataCenter: Sets the local data center name for DC-aware routing policy.
                public init(localDataCenter: String? = nil)

                /// Sets the local data center name for DC-aware routing policy.
                /// When set, a DC-aware load balancing policy will be used that prioritizes hosts from this data center.
                public var localDataCenter: String?

                // [inherited from Equatable] 1 member
                static func != (lhs: Self, rhs: Self) -> Bool

                // [reconstructed] 1 member guaranteed by a conformance above that the symbol graph does not emit
                public static func == (lhs: Self, rhs: Self) -> Bool
            }

            public struct RoundRobin : Hashable, Sendable {
                public init()

                // [inherited from Equatable] 1 member
                static func != (lhs: Self, rhs: Self) -> Bool

                // [reconstructed] 1 member guaranteed by a conformance above that the symbol graph does not emit
                public static func == (lhs: Self, rhs: Self) -> Bool
            }

            // [inherited from Equatable] 1 member
            static func != (lhs: Self, rhs: Self) -> Bool

            // [reconstructed] 1 member guaranteed by a conformance above that the symbol graph does not emit
            public static func == (lhs: Self, rhs: Self) -> Bool
        }

        /// Where statements are prepared beyond the host that handled the prepare request.
        @nonexhaustive public enum PrepareStrategy : Hashable, RawRepresentable, Sendable {
            /// Prepares the statement on every host.
            case allHosts
            /// Prepares already-prepared statements on a host when it becomes available again or is added to the
            /// cluster.
            case upOrAddHost

            // [inherited from Equatable] 1 member
            static func != (lhs: Self, rhs: Self) -> Bool

            // [inherited from RawRepresentable] 2 members
            var hashValue: Int { get }
            func hash(into hasher: inout Hasher)

            // [compiler-synthesized] 1 member
            init?(rawValue: String)

            // [reconstructed] 1 member guaranteed by a conformance above that the symbol graph does not emit
            public static func == (lhs: Self, rhs: Self) -> Bool
        }

        /// The native protocol version used to talk to the cluster.
        @nonexhaustive public enum ProtocolVersion : CaseIterable, Hashable, RawRepresentable, Sendable {
            case v1
            case v2
            case v3
            case v4
            case v5

            // [inherited from Equatable] 1 member
            static func != (lhs: Self, rhs: Self) -> Bool

            // [inherited from RawRepresentable] 2 members
            var hashValue: Int { get }
            func hash(into hasher: inout Hasher)

            // [compiler-synthesized] 1 member
            init?(rawValue: Int32)

            // [reconstructed] 1 member guaranteed by a conformance above that the symbol graph does not emit
            public static func == (lhs: Self, rhs: Self) -> Bool
        }

        /// When the driver starts additional executions of an idempotent request that has not yet completed.
        @nonexhaustive public enum SpeculativeExecutionPolicy : Hashable, Sendable {
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
            public init?(configReader: ConfigReader) throws

            /// Starts up to `maxExecutions` additional executions, each `delay` after the previous one. The
            /// delay is rounded up to whole milliseconds.
            case constant(delay: Duration, maxExecutions: Int32)
            /// Starts no speculative executions.
            case disabled

            // [inherited from Equatable] 1 member
            static func != (lhs: Self, rhs: Self) -> Bool

            // [reconstructed] 1 member guaranteed by a conformance above that the symbol graph does not emit
            public static func == (lhs: Self, rhs: Self) -> Bool
        }

        /// TLS configuration for connections to the cluster.
        public struct TLS : CustomDebugStringConvertible, CustomReflectable, CustomStringConvertible, Sendable {
            public init()
            /// Initializes TLS configuration from a `ConfigReader`.
            ///
            /// ## Configuration keys:
            /// - `enabled` (bool, optional, default: false): Whether TLS is enabled. If `false`, the initializer
            ///   returns `nil`.
            /// - `trustedCertificates` (string array, optional): PEM encoded certificates used to verify the peer.
            /// - `certificateVerification` (string, optional, default: "ipAddressVerification"): Verification performed
            ///   on the peer's certificate, one of "none", "noHostnameVerification", "ipAddressVerification" or
            ///   "fullVerification".
            /// - `certificate` (string, optional): PEM encoded client certificate chain. Set together with `privateKey`.
            /// - `privateKey` (string, optional, secret): PEM encoded client private key.
            /// - `privateKeyPassword` (string, secret): Password for `privateKey`. Required when `privateKey` is set.
            ///
            /// - Throws: If `certificateVerification` is not one of the accepted values, `privateKey` is set without `privateKeyPassword`, or only one of `certificate` and `privateKey` is set.
            public init?(configReader: ConfigReader) throws

            /// Verification performed on the peer's certificate. Default ``CertificateVerification/ipAddressVerification``.
            public var certificateVerification: CertificateVerification
            /// The certificate and private key the client presents to authenticate itself to the server. `nil`
            /// presents none.
            public var clientCredentials: ClientCredentials?
            public var customMirror: Mirror { get }
            public var debugDescription: String { get }
            public var description: String { get }
            /// PEM encoded certificates the peer's certificate chain is validated against. The driver loads no
            /// system trust anchors, so `nil` fails every verification mode except
            /// ``CertificateVerification/none``.
            public var trustedCertificates: [String]?

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
            public struct CertificateVerification : Hashable, Sendable {
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
                public static var fullVerification: `Self` { get }
                /// Certificate is present and valid, and the IP address the driver connected to matches
                /// an `iPAddress` subject alternative name on the certificate. That address is the
                /// resolved contact point for the node the driver reaches directly, and the
                /// `system.peers` `rpc_address` for each node discovered from the cluster, so a peer's
                /// certificate has to name its `rpc_address` even when that is not a configured contact
                /// point. Matching consumes no hostname, so
                /// ``CassandraClient/Configuration/hostnameResolution`` only adds a reverse lookup per
                /// connection here.
                public static var ipAddressVerification: `Self` { get }
                /// Certificate is present and valid. The peer's identity is not checked.
                public static var noHostnameVerification: `Self` { get }
                /// No verification is performed
                public static var none: `Self` { get }

                // [inherited from Equatable] 1 member
                static func != (lhs: Self, rhs: Self) -> Bool

                // [reconstructed] 1 member guaranteed by a conformance above that the symbol graph does not emit
                public static func == (lhs: Self, rhs: Self) -> Bool
            }

            /// A client certificate and the private key that goes with it.
            public struct ClientCredentials : Sendable {
                public init(certificate: String, privateKey: (key: String, password: String))

                /// PEM encoded client certificate chain, starting with the certificate itself.
                public var certificate: String
                /// PEM encoded client private key and its password.
                public var privateKey: (key: String, password: String)
            }
        }

        /// Whether connections to the cluster are encrypted, and the TLS settings if they are.
        public struct TransportSecurity : Sendable {
            /// Connects in plain text.
            public static var plaintext: `Self` { get }
            /// The TLS settings, or `nil` when connecting in plain text.
            public var tls: TLS? { get }

            /// Connects over TLS with the given settings.
            public static func tls(_ tls: TLS) -> CassandraClient.Configuration.TransportSecurity
        }
    }

    /// Username and password authentication, the mechanism Cassandra's `PasswordAuthenticator` accepts.
    ///
    /// Set an instance on ``Configuration/authenticator``. The client hands the credentials to the
    /// driver's built-in implementation rather than driving the handshake through this type's methods.
    public struct PasswordAuthenticator : Authenticator, CustomDebugStringConvertible, CustomReflectable, CustomStringConvertible, Sendable {
        public init(username: String, password: String)

        public var customMirror: Mirror { get }
        public var debugDescription: String { get }
        public var description: String { get }
        /// The password for ``username``.
        public var password: String
        /// The username to authenticate as.
        public var username: String

        /// Default: a single-token mechanism (e.g. SASL PLAIN) sends nothing further after its initial response.
        public func evaluateChallenge(_ challenge: [UInt8]) throws -> [UInt8]?
        /// The SASL PLAIN response: an empty authorization identity, then the username and password, each
        /// preceded by a zero byte.
        public func initialResponse() throws -> [UInt8]?
        /// Default: nothing to do on success.
        public func onSuccess(_ token: [UInt8]) throws
    }
}


// Supplied by the module, not required of a conformer: default implementations and extension members.
extension CassandraClient.Authenticator {
    /// Default: a single-token mechanism (e.g. SASL PLAIN) sends nothing further after its initial response.
    public func evaluateChallenge(_ challenge: [UInt8]) throws -> [UInt8]?
    /// Default: nothing to do on success.
    public func onSuccess(_ token: [UInt8]) throws
}
````
