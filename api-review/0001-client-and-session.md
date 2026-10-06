# Client and session

## Overview

- **Area:** Client and session — 189 of the module's public symbols.
- **Note:** This PR exists to collect API review feedback ahead of 1.0. It will be closed rather than merged; anything it settles lands as its own change.
- **Generated from:** `Sources/CassandraClient` at `e60f45a`, with Apple Swift version 6.3.1 (swiftlang-6.3.1.1.2 clang-2100.0.123.102). Derived, not maintained by hand — see `api-review/README.md` to regenerate.

Inherited protocol members are listed without their standard-library documentation, grouped under the protocol that declared each. Members the symbol graph does not emit are marked `[reconstructed]`.

## Public API

````swift
/// `CassandraClient` is a wrapper around the [Datastax Cassandra C++ Driver](https://github.com/datastax/cpp-driver)
///  and can be used to run queries against a Cassandra database.
public final class CassandraClient : CassandraSession, Sendable {
    /// Create a new instance of `CassandraClient`.
    ///
    /// - Parameters:
    ///   - eventLoopGroupProvider: The ``EventLoopGroupProvider`` to use, uses ``EventLoopGroupProvider/createNew`` strategy by default.
    ///   - configuration: The  client's ``Configuration``.
    ///   - logger: The client's default `Logger`.
    public init(eventLoopGroupProvider: EventLoopGroupProvider = .createNew, configuration: Configuration, logger: Logger? = nil)

    @available(macOS 15.0, iOS 18.0, visionOS 2.0, *)
    public var encryptionSchemas: [String : CassandraClient.EncryptionSchema] { get }
    @available(macOS 15.0, iOS 18.0, visionOS 2.0, *)
    public var encryptor: CassandraClient.Encryptor? { get }
    public var eventLoopGroup: EventLoopGroup { get }
    public var keyspace: String? { get }
    public let logger: Logger

    /// Execute a batch of statements.
    ///
    /// - Parameters:
    ///   - configuration: Options to apply to the batch.
    ///   - logger: If `nil`, the client's default `Logger` is used.
    ///   - build: Closure that adds statements to the batch.
    @available(macOS 12, watchOS 8, iOS 15, tvOS 15, *)
    public func batch(configuration: Batch.Configuration = .init(), logger: Logger? = .none, _ build: (inout Batch) async throws -> Void) async throws
    /// Execute a batch of statements.
    ///
    /// - Parameters:
    ///   - configuration: Options to apply to the batch.
    ///   - eventLoop: The `EventLoop` to use, or create a new one.
    ///   - logger: If `nil`, the client's default `Logger` is used.
    ///   - build: Closure that adds statements to the batch.
    public func batch(configuration: Batch.Configuration = .init(), on eventLoop: EventLoop? = .none, logger: Logger? = .none, _ build: (inout Batch) throws -> Void) -> EventLoopFuture<Void>
    /// Execute a prepared statement with bound parameters.
    @available(macOS 12, watchOS 8, iOS 15, tvOS 15, *)
    public func execute(prepared: CassandraClient.PreparedStatement, parameters: [CassandraClient.Statement.Value] = [], options: CassandraClient.Statement.Options = .init(), logger: Logger? = .none) async throws -> CassandraClient.Rows
    /// Execute a ``PreparedStatement`` with bound parameters using the default ``CassandraSession``.
    ///
    /// - Parameters:
    ///   - prepared: The ``PreparedStatement`` to execute.
    ///   - parameters: The values to bind to the statement's `?` placeholders.
    ///   - options: Statement options (consistency, timeout, encryption context).
    ///   - logger: If `nil`, the client's default `Logger` is used.
    ///
    /// - Returns: The resulting ``Rows``.
    @available(macOS 12, watchOS 8, iOS 15, tvOS 15, *)
    public func execute(prepared: PreparedStatement, parameters: [Statement.Value] = [], options: Statement.Options = .init(), logger: Logger? = .none) async throws -> Rows
    /// Execute a prepared statement and decode each row into a `Decodable` type.
    @available(macOS 12, watchOS 8, iOS 15, tvOS 15, *)
    public func execute<T>(prepared: CassandraClient.PreparedStatement, parameters: [CassandraClient.Statement.Value] = [], options: CassandraClient.Statement.Options = .init(), logger: Logger? = .none) async throws -> [T] where T : Decodable
    /// Execute a ``PreparedStatement`` and decode each row into a `Decodable` type using the default ``CassandraSession``.
    ///
    /// - Parameters:
    ///   - prepared: The ``PreparedStatement`` to execute.
    ///   - parameters: The values to bind to the statement's `?` placeholders.
    ///   - options: Statement options (consistency, timeout, encryption context).
    ///   - logger: If `nil`, the client's default `Logger` is used.
    ///
    /// - Returns: The decoded rows.
    @available(macOS 12, watchOS 8, iOS 15, tvOS 15, *)
    public func execute<T>(prepared: PreparedStatement, parameters: [Statement.Value] = [], options: Statement.Options = .init(), logger: Logger? = .none) async throws -> [T] where T : Decodable
    /// Execute a prepared statement, decoding each row into `model`.
    ///
    /// This is equivalent to the sibling `execute(...)` overload that infers `T` purely from the return type,
    /// but spells out the decoded type explicitly at the call site, e.g.
    /// `try await session.execute(prepared: statement, withModelType: Model.self)`.
    @available(macOS 12, watchOS 8, iOS 15, tvOS 15, *)
    public func execute<T>(prepared: CassandraClient.PreparedStatement, parameters: [CassandraClient.Statement.Value] = [], options: CassandraClient.Statement.Options = .init(), logger: Logger? = .none, withModelType model: T.Type) async throws -> [T] where T : Decodable
    /// Execute a ``PreparedStatement`` and decode each row into `model` using the default ``CassandraSession``.
    ///
    /// This is equivalent to the sibling `execute(...)` overload that infers `T` purely from the return type,
    /// but spells out the decoded type explicitly at the call site, e.g.
    /// `try await cassandraClient.execute(prepared: statement, withModelType: Model.self)`.
    ///
    /// - Parameters:
    ///   - prepared: The ``PreparedStatement`` to execute.
    ///   - parameters: The values to bind to the statement's `?` placeholders.
    ///   - options: Statement options (consistency, timeout, encryption context).
    ///   - logger: If `nil`, the client's default `Logger` is used.
    ///   - model: The type to decode each row into.
    ///
    /// - Returns: The decoded rows.
    @available(macOS 12, watchOS 8, iOS 15, tvOS 15, *)
    public func execute<T>(prepared: PreparedStatement, parameters: [Statement.Value] = [], options: Statement.Options = .init(), logger: Logger? = .none, withModelType model: T.Type) async throws -> [T] where T : Decodable
    /// Execute a ``PreparedStatement`` and decode each row into a `Decodable` type using the default ``CassandraSession``.
    ///
    /// - Parameters:
    ///   - prepared: The ``PreparedStatement`` to execute.
    ///   - parameters: The values to bind to the statement's `?` placeholders.
    ///   - options: Statement options (consistency, timeout, encryption context).
    ///   - eventLoop: The `EventLoop` to use, or create a new one.
    ///   - logger: If `nil`, the client's default `Logger` is used.
    ///
    /// - Returns: The decoded rows.
    @preconcurrency public func execute<T>(prepared: PreparedStatement, parameters: sending [Statement.Value] = [], options: Statement.Options = .init(), on eventLoop: EventLoop? = .none, logger: Logger? = .none) -> EventLoopFuture<[T]> where T : Decodable, T : Sendable
    /// Execute a ``PreparedStatement`` with bound parameters using the default ``CassandraSession``.
    ///
    /// - Parameters:
    ///   - prepared: The ``PreparedStatement`` to execute.
    ///   - parameters: The values to bind to the statement's `?` placeholders.
    ///   - options: Statement options (consistency, timeout, encryption context).
    ///   - eventLoop: The `EventLoop` to use, or create a new one.
    ///   - logger: If `nil`, the client's default `Logger` is used.
    ///
    /// - Returns: The resulting ``Rows``.
    public func execute(prepared: PreparedStatement, parameters: sending [Statement.Value] = [], options: Statement.Options = .init(), on eventLoop: EventLoop? = .none, logger: Logger? = .none) -> EventLoopFuture<Rows>
    /// Execute a prepared statement, decoding each row into `model`.
    ///
    /// This is equivalent to the sibling `execute(...)` overload that infers `T` purely from the return type,
    /// but spells out the decoded type explicitly at the call site, e.g.
    /// `session.execute(prepared: statement, withModelType: Model.self)`.
    ///
    /// If `eventLoop` is `nil`, a new one will get created through the `EventLoopGroup` provided during initialization.
    @preconcurrency public func execute<T>(prepared: CassandraClient.PreparedStatement, parameters: sending [CassandraClient.Statement.Value] = [], options: CassandraClient.Statement.Options = .init(), on eventLoop: EventLoop? = .none, logger: Logger? = .none, withModelType model: T.Type) -> EventLoopFuture<[T]> where T : Decodable, T : Sendable
    /// Execute a ``PreparedStatement`` and decode each row into `model` using the default ``CassandraSession``.
    ///
    /// This is equivalent to the sibling `execute(...)` overload that infers `T` purely from the return type,
    /// but spells out the decoded type explicitly at the call site, e.g.
    /// `cassandraClient.execute(prepared: statement, withModelType: Model.self)`.
    ///
    /// - Parameters:
    ///   - prepared: The ``PreparedStatement`` to execute.
    ///   - parameters: The values to bind to the statement's `?` placeholders.
    ///   - options: Statement options (consistency, timeout, encryption context).
    ///   - eventLoop: The `EventLoop` to use, or create a new one.
    ///   - logger: If `nil`, the client's default `Logger` is used.
    ///   - model: The type to decode each row into.
    ///
    /// - Returns: The decoded rows.
    @preconcurrency public func execute<T>(prepared: PreparedStatement, parameters: sending [Statement.Value] = [], options: Statement.Options = .init(), on eventLoop: EventLoop? = .none, logger: Logger? = .none, withModelType model: T.Type) -> EventLoopFuture<[T]> where T : Decodable, T : Sendable
    /// Execute a ``Statement`` using the default ``CassandraSession``.
    ///
    /// **All** rows are returned, unless the statement sets a page size with
    /// ``Statement/setPagingSize(_:)``, which limits the result to a single page.
    ///
    /// - Parameters:
    ///   - statement: The ``Statement`` to execute.
    ///   - logger: If `nil`, the client's default `Logger` is used.
    ///
    /// - Returns: The resulting ``Rows``.
    @available(macOS 12, watchOS 8, iOS 15, tvOS 15, *)
    public func execute(statement: Statement, logger: Logger? = .none) async throws -> Rows
    /// Execute a ``Statement`` using the default ``CassandraSession`` on the given `EventLoop` or create a new one.
    ///
    /// **All** rows are returned, unless the statement sets a page size with
    /// ``Statement/setPagingSize(_:)``, which limits the result to a single page.
    ///
    /// - Parameters:
    ///   - statement: The ``Statement`` to execute.
    ///   - eventLoop: The `EventLoop` to use, or create a new one.
    ///   - logger: If `nil`, the client's default `Logger` is used.
    ///
    /// - Returns: The resulting ``Rows``.
    public func execute(statement: sending Statement, on eventLoop: EventLoop?, logger: Logger? = .none) -> EventLoopFuture<Rows>
    /// Execute a ``Statement`` using the default ``CassandraSession``.
    ///
    /// Resulting rows are paginated.
    ///
    /// - Parameters:
    ///   - statement: The ``Statement`` to execute.
    ///   - pageSize: The maximum number of rows returned per page. Must be positive; a
    ///     non-positive size fails the call with ``CassandraClient/Error/badParams(_:)``.
    ///   - logger: If `nil`, the client's default `Logger` is used.
    ///
    /// - Returns: The ``PaginatedRows``.
    @available(macOS 12, watchOS 8, iOS 15, tvOS 15, *)
    public func execute(statement: sending Statement, pageSize: Int32, logger: Logger? = .none) async throws -> PaginatedRows
    /// Execute a ``Statement`` using the default ``CassandraSession`` on the given `EventLoop` or create a new one.
    ///
    /// Resulting rows are paginated.
    ///
    /// - Parameters:
    ///   - statement: The ``Statement`` to execute.
    ///   - pageSize: The maximum number of rows returned per page. Must be positive; a
    ///     non-positive size fails the call with ``CassandraClient/Error/badParams(_:)``.
    ///   - eventLoop: The `EventLoop` to use, or create a new one.
    ///   - logger: If `nil`, the client's default `Logger` is used.
    ///
    /// - Returns: The ``PaginatedRows``.
    public func execute(statement: sending Statement, pageSize: Int32, on eventLoop: EventLoop?, logger: Logger? = .none) -> EventLoopFuture<PaginatedRows>
    public func getMetrics() -> CassandraMetrics
    /// Create a new ``CassandraSession`` that can be used to perform queries on the given or configured keyspace.
    ///
    /// - Parameters:
    ///   - keyspace: If `nil`, the client's default keyspace is used.
    ///   - logger: If `nil`, the client's default `Logger` is used.
    ///
    /// - Returns: The newly created session.
    public func makeSession(keyspace: String?, logger: Logger? = .none) -> CassandraSession
    /// Prepare a CQL query for repeated execution.
    @available(macOS 12, watchOS 8, iOS 15, tvOS 15, *)
    public func prepare(_ query: String, encryptionTable: String? = nil, logger: Logger? = .none) async throws -> CassandraClient.PreparedStatement
    /// Prepare a CQL query for repeated execution using the default ``CassandraSession``.
    ///
    /// - Parameters:
    ///   - query: The CQL query string with `?` placeholders.
    ///   - encryptionTable: The table name for encryption context resolution. If provided, PK column names are looked up at prepare time.
    ///   - logger: If `nil`, the client's default `Logger` is used.
    ///
    /// - Returns: A ``PreparedStatement``.
    @available(macOS 12, watchOS 8, iOS 15, tvOS 15, *)
    public func prepare(_ query: String, encryptionTable: String? = nil, logger: Logger? = .none) async throws -> PreparedStatement
    /// Prepare a CQL query for repeated execution using the default ``CassandraSession``.
    ///
    /// - Parameters:
    ///   - query: The CQL query string with `?` placeholders.
    ///   - encryptionTable: The table name for encryption context resolution. If provided, PK column names are looked up at prepare time.
    ///   - eventLoop: The `EventLoop` to use, or create a new one.
    ///   - logger: If `nil`, the client's default `Logger` is used.
    ///
    /// - Returns: A ``PreparedStatement``.
    public func prepare(_ query: String, encryptionTable: String? = nil, on eventLoop: EventLoop? = .none, logger: Logger? = .none) -> EventLoopFuture<PreparedStatement>
    /// Query large data-sets where using an interator helps control memory usage.
    ///
    /// - Important:
    ///   - Advancing the iterator invalidates values retrieved by the previous iteration.
    ///   - Attempting to wrap the ``CassandraClient/Rows`` sequence in a list will not work, use the transformer variant instead.
    @available(macOS 12, watchOS 8, iOS 15, tvOS 15, *)
    public func query(_ query: String, parameters: [CassandraClient.Statement.Value] = [], options: CassandraClient.Statement.Options = .init(), logger: Logger? = .none) async throws -> CassandraClient.Rows
    /// Query small data-sets that fit into memory. Only use this when it's safe to buffer the entire data-set into memory.
    @available(macOS 12, watchOS 8, iOS 15, tvOS 15, *)
    public func query<T>(_ query: String, parameters: [CassandraClient.Statement.Value] = [], options: CassandraClient.Statement.Options = .init(), logger: Logger? = .none) async throws -> [T] where T : Decodable
    /// Query small data-sets that fit into memory. Only use this when it's safe to buffer the entire data-set into memory.
    @available(macOS 12, watchOS 8, iOS 15, tvOS 15, *)
    public func query<T>(_ query: String, parameters: [CassandraClient.Statement.Value] = [], options: CassandraClient.Statement.Options = .init(), logger: Logger? = .none, transform: @escaping (CassandraClient.Row) -> T?) async throws -> [T]
    /// Query small data-sets that fit into memory, decoding each row into `model`.
    ///
    /// This is equivalent to the sibling `query(...)` overload that infers `T` purely from the return type,
    /// but spells out the decoded type explicitly at the call site, e.g.
    /// `try await session.query("select ...", withModelType: Model.self)`.
    @available(macOS 12, watchOS 8, iOS 15, tvOS 15, *)
    public func query<T>(_ query: String, parameters: [CassandraClient.Statement.Value] = [], options: CassandraClient.Statement.Options = .init(), logger: Logger? = .none, withModelType model: T.Type) async throws -> [T] where T : Decodable
    /// Query large data-sets where using an interator helps control memory usage.
    ///
    /// If `eventLoop` is `nil`, a new one will get created through the `EventLoopGroup` provided during initialization.
    ///
    /// - Important:
    ///   - Advancing the iterator invalidates values retrieved by the previous iteration.
    ///   - Attempting to wrap the ``CassandraClient/Rows`` sequence in a list will not work, use the transformer variant instead.
    public func query(_ query: String, parameters: sending [CassandraClient.Statement.Value] = [], options: CassandraClient.Statement.Options = .init(), on eventLoop: EventLoop? = .none, logger: Logger? = .none) -> EventLoopFuture<CassandraClient.Rows>
    /// Query small data-sets that fit into memory. Only use this when it is safe to buffer the entire data-set into memory.
    ///
    /// If `eventLoop` is `nil`, a new one will get created through the `EventLoopGroup` provided during initialization.
    @preconcurrency public func query<T>(_ query: String, parameters: sending [CassandraClient.Statement.Value] = [], options: CassandraClient.Statement.Options = .init(), on eventLoop: EventLoop? = .none, logger: Logger? = .none, transform: @escaping @Sendable (CassandraClient.Row) -> T?) -> EventLoopFuture<[T]>
    /// Query small data-sets that fit into memory, decoding each row into `model`.
    ///
    /// This is equivalent to the sibling `query(...)` overload that infers `T` purely from the return type,
    /// but spells out the decoded type explicitly at the call site, e.g.
    /// `session.query("select ...", withModelType: Model.self)`.
    ///
    /// If `eventLoop` is `nil`, a new one will get created through the `EventLoopGroup` provided during initialization.
    @preconcurrency public func query<T>(_ query: String, parameters: sending [CassandraClient.Statement.Value] = [], options: CassandraClient.Statement.Options = .init(), on eventLoop: EventLoop? = .none, logger: Logger? = .none, withModelType model: T.Type) -> EventLoopFuture<[T]> where T : Decodable, T : Sendable
    /// Query large data-sets where the number of rows fetched at a time is limited by `pageSize`.
    ///
    /// A non-positive `pageSize` fails the call with ``CassandraClient/Error/badParams(_:)``.
    @available(macOS 12, watchOS 8, iOS 15, tvOS 15, *)
    public func query(_ query: String, parameters: sending [CassandraClient.Statement.Value] = [], pageSize: Int32, options: CassandraClient.Statement.Options = .init(), logger: Logger? = .none) async throws -> CassandraClient.PaginatedRows
    /// Query large data-sets where the number of rows fetched at a time is limited by `pageSize`,
    /// decoding each row into `T` as the sequence is iterated, rather than materializing the whole
    /// decoded result set as an array.
    ///
    /// A non-positive `pageSize` fails the call with ``CassandraClient/Error/badParams(_:)``.
    ///
    /// - Note: Unlike the raw ``query(_:parameters:pageSize:options:logger:)`` sequence, decoded values
    ///   are independent Swift values and remain valid after the sequence is advanced.
    @available(macOS 12, watchOS 8, iOS 15, tvOS 15, *)
    public func query<T>(_ query: String, parameters: sending [CassandraClient.Statement.Value] = [], pageSize: Int32, options: CassandraClient.Statement.Options = .init(), logger: Logger? = .none) async throws -> AsyncThrowingMapSequence<CassandraClient.PaginatedRows, T> where T : Decodable, T : Sendable
    /// Query large data-sets where the number of rows fetched at a time is limited by `pageSize`,
    /// decoding each row into `model` as the sequence is iterated.
    ///
    /// This is equivalent to the sibling `query(...)` overload that infers `T` purely from the return type,
    /// but spells out the decoded type explicitly at the call site, e.g.
    /// `try await session.query("select ...", pageSize: 100, withModelType: Model.self)`.
    ///
    /// A non-positive `pageSize` fails the call with ``CassandraClient/Error/badParams(_:)``.
    @available(macOS 12, watchOS 8, iOS 15, tvOS 15, *)
    public func query<T>(_ query: String, parameters: sending [CassandraClient.Statement.Value] = [], pageSize: Int32, options: CassandraClient.Statement.Options = .init(), logger: Logger? = .none, withModelType model: T.Type) async throws -> AsyncThrowingMapSequence<CassandraClient.PaginatedRows, T> where T : Decodable, T : Sendable
    /// Query large data-sets where the number of rows fetched at a time is limited by `pageSize`.
    ///
    /// A non-positive `pageSize` fails the call with ``CassandraClient/Error/badParams(_:)``.
    ///
    /// If `eventLoop` is `nil`, a new one will get created through the `EventLoopGroup` provided during initialization.
    public func query(_ query: String, parameters: sending [CassandraClient.Statement.Value] = [], pageSize: Int32, options: CassandraClient.Statement.Options = .init(), on eventLoop: EventLoop? = .none, logger: Logger? = .none) -> EventLoopFuture<CassandraClient.PaginatedRows>
    /// Run  insert / update / delete or DDL commands where no result is expected
    @available(macOS 12, watchOS 8, iOS 15, tvOS 15, *)
    public func run(_ command: String, parameters: [CassandraClient.Statement.Value] = [], options: CassandraClient.Statement.Options = .init(), logger: Logger? = .none) async throws
    /// Run insert / update / delete or DDL command where no result is expected.
    ///
    /// If `eventLoop` is `nil`, a new one will get created through the `EventLoopGroup` provided during initialization.
    public func run(_ command: String, parameters: sending [CassandraClient.Statement.Value] = [], options: CassandraClient.Statement.Options = .init(), on eventLoop: EventLoop? = .none, logger: Logger? = .none) -> EventLoopFuture<Void>
    /// Shutdown the client.
    ///
    /// - Note: It is required to call this method before terminating the program. `CassandraClient` will assert it was cleanly shut down as part of its deinitializer.
    public func shutdown() throws
    @available(macOS 12, watchOS 8, iOS 15, tvOS 15, *)
    public func shutdownAsync() async throws
    /// Create a new ``CassandraSession`` for the given or configured keyspace then invoke the closure.
    ///
    /// - Parameters:
    ///   - keyspace: If `nil`, the client's default keyspace is used.
    ///   - logger: If `nil`, the client's default `Logger` is used.
    ///   - closure: The closure to invoke, passing in the newly created session.
    @available(macOS 12, watchOS 8, iOS 15, tvOS 15, *)
    public func withSession(keyspace: String?, logger: Logger? = .none, closure: (CassandraSession) async throws -> Void) async throws
    /// Create a new ``CassandraSession`` for the given or configured keyspace then invoke the closure.
    ///
    /// - Parameters:
    ///   - keyspace: If `nil`, the client's default keyspace is used.
    ///   - logger: If `nil`, the client's default `Logger` is used.
    ///   - handler: The closure to invoke, passing in the newly created session.
    public func withSession(keyspace: String?, logger: Logger? = .none, handler: (CassandraSession) throws -> Void) rethrows
    /// Create a new ``CassandraSession`` for the given or configured keyspace then invoke the closure and return its `EventLoopFuture` result.
    ///
    /// - Parameters:
    ///   - keyspace: If `nil`, the client's default keyspace is used.
    ///   - logger: If `nil`, the client's default `Logger` is used.
    ///   - handler: The closure to invoke, passing in the newly created session.
    ///
    /// - Returns: The resulting `EventLoopFuture` of the closure.
    public func withSession<T>(keyspace: String?, logger: Logger? = .none, handler: (CassandraSession) -> EventLoopFuture<T>) -> EventLoopFuture<T>
    /// Create a new ``CassandraSession`` for the given or configured keyspace then invoke the closure and return its result.
    ///
    /// - Parameters:
    ///   - keyspace: If `nil`, the client's default keyspace is used.
    ///   - logger: If `nil`, the client's default `Logger` is used.
    ///   - handler: The closure to invoke, passing in the newly created session.
    ///
    /// - Returns: The result of the closure.
    @available(macOS 12, watchOS 8, iOS 15, tvOS 15, *)
    public func withSession<T>(keyspace: String?, logger: Logger? = .none, handler: (CassandraSession) async throws -> T) async throws -> T

    /// A custom SASL authenticator, for mechanisms beyond username/password (e.g. AWS SigV4 for
    /// Amazon Keyspaces, or Kerberos).
    ///
    /// The client drives a SASL (Simple Authentication and Security Layer) handshake: it produces an
    /// initial response, answers any server challenges, then observes success. Set an instance on
    /// ``Configuration/authenticator`` to use it; when set, it takes precedence over
    /// ``Configuration/username``/``Configuration/password``.
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
        /// Called once the server reports success, with any final token it sent (always present, may be empty).
        func onSuccess(_ token: [UInt8]) throws
    }

    /// Configuration for the ``CassandraClient``.
    public struct Configuration : CustomStringConvertible, Sendable {
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
        ///   - logger: Logger for configuration warnings, such as SSL properties set while SSL is disabled
        @available(macOS 15.0, watchOS 11.0, iOS 18.0, visionOS 2.0, tvOS 18.0, *)
        public init(configReader: ConfigReader, contactPointsProvider: @escaping @Sendable (@escaping @Sendable (Result<ContactPoints, Swift.Error>) -> Void) -> Void, logger: Logger) throws
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
        @available(macOS 15.0, watchOS 11.0, iOS 18.0, visionOS 2.0, tvOS 18.0, *)
        public init(configReader: ConfigReader, logger: Logger) throws
        @preconcurrency public init(contactPointsProvider: @escaping @Sendable (@escaping @Sendable (Result<ContactPoints, Swift.Error>) -> Void) -> Void, port: Int32, protocolVersion: ProtocolVersion)

        /// A custom SASL authenticator. When set, it takes precedence over ``username``/``password``.
        /// The instance is shared across all connections and invoked concurrently; see
        /// ``CassandraClient/Authenticator``.
        public var authenticator: (any CassandraClient.Authenticator)?
        public var compact: Bool?
        public var connectTimeoutMillis: UInt32?
        public var connectionHeartbeatInterval: UInt32?
        public var connectionIdleTimeout: UInt32?
        /// Sets the cluster's consistency level. Default is `.localOne`.
        public var consistency: CassandraClient.Consistency?
        /// Provides the initial `ContactPoints` of the Cassandra cluster.
        /// This can be a subset since each Cassandra instance is capable of discovering its peers.
        public var contactPointsProvider: @Sendable (@escaping @Sendable (Result<ContactPoints, Swift.Error>) -> Void) -> Void
        public var coreConnectionsPerHost: UInt32?
        public var description: String { get }
        /// Registered encryption schemas.
        @available(macOS 15.0, iOS 18.0, visionOS 2.0, *)
        public var encryptionSchemas: [String : EncryptionSchema] { get set }
        /// Encryptor for transparent column encryption.
        @available(macOS 15.0, iOS 18.0, visionOS 2.0, *)
        public var encryptor: Encryptor? { get set }
        public var hostnameResolution: Bool?
        public var keyspace: String?
        /// The load balancing strategy to use. Default is `nil` which uses ``LoadBalancingStrategy/dataCenterAware(_:)``.
        public var loadBalancingStrategy: LoadBalancingStrategy?
        /// Includes bound parameter values in request logs when `true`. Off by default — values are potential PII.
        public var logBoundValues: Bool
        /// Enables driver metrics emission. Default `false` (off).
        /// When enabled, the session polls the driver's snapshot and pushes gauges to swift-metrics.
        public var metricsEnabled: Bool
        /// Poller cadence in milliseconds. Default `10000` (10s). `nil` or `0` disables the poller
        /// while leaving ``metricsEnabled`` on; a `0` interval would busy-loop the poller.
        /// Requires macOS 12 / iOS 15 or newer; on older platforms the poller does not start.
        public var metricsPollIntervalMillis: UInt32?
        /// Optional session name attached as a `session` dimension on every emitted metric.
        /// Set this to disambiguate metrics when more than one metrics-enabled session runs in a
        /// process, otherwise their identically-named gauges overwrite each other. `nil` = no dimension.
        public var metricsSessionName: String?
        public var numIOThreads: UInt32?
        public var password: String?
        public var port: Int32
        public var prepareStrategy: PrepareStrategy?
        public var protocolVersion: ProtocolVersion
        public var randomizedContactPoints: Bool?
        public var requestTimeoutMillis: UInt32?
        public var resolveTimeoutMillis: UInt32?
        public var schema: Bool?
        /// Sets the cluster's serial consistency level for LWT operations.
        /// Default is `.serial`.
        public var serialConsistency: CassandraClient.SerialConsistency?
        /// Logs a successful query at `.debug` when its latency reaches this threshold (ms). `nil` disables
        /// the check; `0` logs every success.
        public var slowQueryThresholdMillis: UInt32?
        public var speculativeExecutionPolicy: SpeculativeExecutionPolicy?
        public var ssl: SSL?
        public var tcpKeepalive: Bool?
        public var tcpKeepaliveDelaySeconds: UInt32
        public var tcpNodelay: Bool?
        public var username: String?

        public typealias ContactPoints = [String]
        /// Register an encryption schema for automatic context building during decoding.
        @available(macOS 15.0, iOS 18.0, visionOS 2.0, *)
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
            @available(macOS 15.0, watchOS 11.0, iOS 18.0, visionOS 2.0, tvOS 18.0, *)
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

        public enum PrepareStrategy : Hashable, RawRepresentable, Sendable {
            case allHosts
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

        public enum ProtocolVersion : CaseIterable, Hashable, RawRepresentable, Sendable {
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

        public struct SSL : Sendable {
            public init()
            /// Initializes SSL configuration from a `ConfigReader`.
            ///
            /// ## Configuration keys:
            /// - `enabled` (bool, optional, default: false): Whether SSL is enabled. If `false`, the initializer
            ///   returns `nil`.
            /// - `trustedCertificates` (string array, optional): PEM encoded certificates used to verify the peer.
            /// - `verifyFlag` (string, optional, default: "peerIdentity"): Verification performed on the peer's
            ///   certificate, one of "none", "peerCert", "peerIdentity" or "peerIdentityDNS".
            /// - `cert` (string, optional): PEM encoded client certificate chain.
            /// - `privateKey` (string, optional, secret): PEM encoded client private key.
            /// - `privateKeyPassword` (string, secret): Password for `privateKey`. Required when `privateKey` is set.
            ///
            /// - Throws: If `verifyFlag` is not one of the accepted values, or `privateKey` is set without `privateKeyPassword`.
            @available(macOS 15.0, watchOS 11.0, iOS 18.0, visionOS 2.0, tvOS 18.0, *)
            public init?(configReader: ConfigReader) throws

            public var cert: String?
            public var privateKey: (key: String, password: String)?
            public var trustedCertificates: [String]?
            public var verifyFlag: VerifyFlag

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
            public enum VerifyFlag : CaseIterable, Hashable, RawRepresentable, Sendable {
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
        }

        public enum SpeculativeExecutionPolicy : Hashable, Sendable {
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
            @available(macOS 15.0, watchOS 11.0, iOS 18.0, visionOS 2.0, tvOS 18.0, *)
            public init?(configReader: ConfigReader) throws

            case constant(delayInMillseconds: Int64, maxExecutions: Int32)
            case disabled

            // [inherited from Equatable] 1 member
            static func != (lhs: Self, rhs: Self) -> Bool

            // [reconstructed] 1 member guaranteed by a conformance above that the symbol graph does not emit
            public static func == (lhs: Self, rhs: Self) -> Bool
        }
    }

    /// A `EventLoopGroupProvider` defines how the underlying `EventLoopGroup` used to create the `EventLoop` is provided.
    ///
    /// When `shared`, the `EventLoopGroup` is provided externally and its lifecycle will be managed by the caller.
    /// When `createNew`, the library will create a new `EventLoopGroup` and manage its lifecycle.
    public enum EventLoopGroupProvider : Sendable {
        case createNew
        case shared(EventLoopGroup)
    }
}

/// API for executing statements against Cassandra.
@preconcurrency public protocol CassandraSession : Sendable {
    /// Registered encrypted column schemas for automatic context building.
    @available(macOS 15.0, iOS 18.0, visionOS 2.0, *)
    var encryptionSchemas: [String : CassandraClient.EncryptionSchema] { get }
    /// Encryptor for transparent column encryption.
    @available(macOS 15.0, iOS 18.0, visionOS 2.0, *)
    var encryptor: CassandraClient.Encryptor? { get }
    var eventLoopGroup: EventLoopGroup { get }
    /// The default keyspace for this session, used to resolve unqualified table names.
    var keyspace: String? { get }
    /// The default `Logger` for this session/client, used when a call site passes no explicit logger.
    var logger: Logger { get }

    @available(macOS 12, watchOS 8, iOS 15, tvOS 15, *)
    func batch(configuration: CassandraClient.Batch.Configuration, logger: Logger?, _ build: (inout CassandraClient.Batch) async throws -> Void) async throws
    /// Execute a batch of statements.
    ///
    /// - Parameters:
    ///   - configuration: Options to apply to the batch.
    ///   - eventLoop: The `EventLoop` to use, or create a new one.
    ///   - logger: If `nil`, the client's default `Logger` is used.
    ///   - build: Closure that adds statements to the batch.
    func batch(configuration: CassandraClient.Batch.Configuration, on eventLoop: EventLoop?, logger: Logger?, _ build: (inout CassandraClient.Batch) throws -> Void) -> EventLoopFuture<Void>
    /// Execute a prepared statement with bound parameters.
    ///
    /// - Parameters:
    ///   - prepared: The ``CassandraClient/PreparedStatement`` to execute.
    ///   - parameters: The values to bind to the statement's `?` placeholders.
    ///   - options: Statement options (consistency, timeout, encryption context).
    ///   - logger: The `Logger` to use. Optional.
    ///
    /// - Returns: The resulting ``CassandraClient/Rows``.
    @available(macOS 12, watchOS 8, iOS 15, tvOS 15, *)
    func execute(prepared: CassandraClient.PreparedStatement, parameters: [CassandraClient.Statement.Value], options: CassandraClient.Statement.Options, logger: Logger?) async throws -> CassandraClient.Rows
    /// Execute a prepared statement with bound parameters.
    ///
    /// **All** rows are returned.
    ///
    /// - Parameters:
    ///   - prepared: The ``CassandraClient/PreparedStatement`` to execute.
    ///   - parameters: The values to bind to the statement's `?` placeholders.
    ///   - options: Statement options (consistency, timeout, encryption context).
    ///   - eventLoop: The `EventLoop` to use. Optional.
    ///   - logger: The `Logger` to use. Optional.
    ///
    /// - Returns: The resulting ``CassandraClient/Rows``.
    func execute(prepared: CassandraClient.PreparedStatement, parameters: sending [CassandraClient.Statement.Value], options: CassandraClient.Statement.Options, on eventLoop: EventLoop?, logger: Logger?) -> EventLoopFuture<CassandraClient.Rows>
    /// Execute a prepared statement.
    ///
    /// **All** rows are returned, unless the statement sets a page size with
    /// ``CassandraClient/Statement/setPagingSize(_:)``, which limits the result to a single page.
    ///
    /// - Parameters:
    ///   - statement: The ``CassandraClient/Statement`` to execute.
    ///   - logger: The `Logger` to use. Optional.
    ///
    /// - Returns: The resulting ``CassandraClient/Rows``.
    @available(macOS 12, watchOS 8, iOS 15, tvOS 15, *)
    func execute(statement: CassandraClient.Statement, logger: Logger?) async throws -> CassandraClient.Rows
    /// Execute a prepared statement.
    ///
    /// **All** rows are returned, unless the statement sets a page size with
    /// ``CassandraClient/Statement/setPagingSize(_:)``, which limits the result to a single page.
    ///
    /// - Parameters:
    ///   - statement: The ``CassandraClient/Statement`` to execute.
    ///   - eventLoop: The `EventLoop` to use. Optional.
    ///   - logger: The `Logger` to use. Optional.
    ///
    /// - Returns: The resulting ``CassandraClient/Rows``.
    func execute(statement: sending CassandraClient.Statement, on eventLoop: EventLoop?, logger: Logger?) -> EventLoopFuture<CassandraClient.Rows>
    /// Execute a prepared statement.
    ///
    /// Resulting rows are paginated.
    ///
    /// - Parameters:
    ///   - statement: The ``CassandraClient/Statement`` to execute.
    ///   - pageSize: The maximum number of rows returned per page. Must be positive; a
    ///     non-positive size fails the call with ``CassandraClient/Error/badParams(_:)``.
    ///   - logger: The `Logger` to use. Optional.
    ///
    /// - Returns: The resulting ``CassandraClient/PaginatedRows``.
    @available(macOS 12, watchOS 8, iOS 15, tvOS 15, *)
    func execute(statement: sending CassandraClient.Statement, pageSize: Int32, logger: Logger?) async throws -> CassandraClient.PaginatedRows
    /// Execute a prepared statement.
    ///
    /// Resulting rows are paginated.
    ///
    /// - Parameters:
    ///   - statement: The ``CassandraClient/Statement`` to execute.
    ///   - pageSize: The maximum number of rows returned per page. Must be positive; a
    ///     non-positive size fails the call with ``CassandraClient/Error/badParams(_:)``.
    ///   - eventLoop: The `EventLoop` to use. Optional.
    ///   - logger: The `Logger` to use. Optional.
    ///
    /// - Returns: The resulting ``CassandraClient/PaginatedRows``.
    func execute(statement: sending CassandraClient.Statement, pageSize: Int32, on eventLoop: EventLoop?, logger: Logger?) -> EventLoopFuture<CassandraClient.PaginatedRows>
    /// Get metrics for this session.
    func getMetrics() -> CassandraMetrics
    /// Prepare a CQL query for repeated execution.
    ///
    /// The server parses and validates the query once. The returned ``CassandraClient/PreparedStatement``
    /// can then be bound with different parameters and executed multiple times without re-parsing.
    ///
    /// - Parameters:
    ///   - query: The CQL query string with `?` placeholders.
    ///   - encryptionTable: The table name for encryption context resolution. If provided, PK column names are looked up at prepare time.
    ///   - logger: The `Logger` to use. Optional.
    ///
    /// - Returns: A ``CassandraClient/PreparedStatement``.
    @available(macOS 12, watchOS 8, iOS 15, tvOS 15, *)
    func prepare(_ query: String, encryptionTable: String?, logger: Logger?) async throws -> CassandraClient.PreparedStatement
    /// Prepare a CQL query for repeated execution.
    ///
    /// The server parses and validates the query once. The returned ``CassandraClient/PreparedStatement``
    /// can then be bound with different parameters and executed multiple times without re-parsing.
    ///
    /// - Parameters:
    ///   - query: The CQL query string with `?` placeholders.
    ///   - encryptionTable: The table name for encryption context resolution. If provided, PK column names are looked up at prepare time.
    ///   - eventLoop: The `EventLoop` to use. Optional.
    ///   - logger: The `Logger` to use. Optional.
    ///
    /// - Returns: A ``CassandraClient/PreparedStatement``.
    func prepare(_ query: String, encryptionTable: String?, on eventLoop: EventLoop?, logger: Logger?) -> EventLoopFuture<CassandraClient.PreparedStatement>
    /// Terminate the session and free resources.
    func shutdown() throws
    /// Terminate the session and free resources.
    @available(macOS 12, watchOS 8, iOS 15, tvOS 15, *)
    func shutdownAsync() async throws
}


// Supplied by the module, not required of a conformer: default implementations and extension members.
extension CassandraClient.Authenticator {
    /// Default: a single-token mechanism (e.g. SASL PLAIN) sends nothing further after its initial response.
    public func evaluateChallenge(_ challenge: [UInt8]) throws -> [UInt8]?
    /// Default: nothing to do on success.
    public func onSuccess(_ token: [UInt8]) throws
}

// Supplied by the module, not required of a conformer: default implementations and extension members.
extension CassandraSession {
    /// Fallback for conformers that don't provide their own logger. `CassandraClient` and `Session` witness
    /// this with their configured logger, so this only applies to third-party conformances.
    public var logger: Logger { get }

    /// Execute a prepared statement with bound parameters.
    @available(macOS 12, watchOS 8, iOS 15, tvOS 15, *)
    public func execute(prepared: CassandraClient.PreparedStatement, parameters: [CassandraClient.Statement.Value] = [], options: CassandraClient.Statement.Options = .init(), logger: Logger? = .none) async throws -> CassandraClient.Rows
    /// Execute a prepared statement and decode each row into a `Decodable` type.
    @available(macOS 12, watchOS 8, iOS 15, tvOS 15, *)
    public func execute<T>(prepared: CassandraClient.PreparedStatement, parameters: [CassandraClient.Statement.Value] = [], options: CassandraClient.Statement.Options = .init(), logger: Logger? = .none) async throws -> [T] where T : Decodable
    /// Execute a prepared statement, decoding each row into `model`.
    ///
    /// This is equivalent to the sibling `execute(...)` overload that infers `T` purely from the return type,
    /// but spells out the decoded type explicitly at the call site, e.g.
    /// `try await session.execute(prepared: statement, withModelType: Model.self)`.
    @available(macOS 12, watchOS 8, iOS 15, tvOS 15, *)
    public func execute<T>(prepared: CassandraClient.PreparedStatement, parameters: [CassandraClient.Statement.Value] = [], options: CassandraClient.Statement.Options = .init(), logger: Logger? = .none, withModelType model: T.Type) async throws -> [T] where T : Decodable
    /// Execute a prepared statement and decode each row into a `Decodable` type.
    ///
    /// If `eventLoop` is `nil`, a new one will get created through the `EventLoopGroup` provided during initialization.
    @preconcurrency public func execute<T>(prepared: CassandraClient.PreparedStatement, parameters: sending [CassandraClient.Statement.Value] = [], options: CassandraClient.Statement.Options = .init(), on eventLoop: EventLoop? = .none, logger: Logger? = .none) -> EventLoopFuture<[T]> where T : Decodable, T : Sendable
    /// Execute a prepared statement with bound parameters.
    ///
    /// If `eventLoop` is `nil`, a new one will get created through the `EventLoopGroup` provided during initialization.
    public func execute(prepared: CassandraClient.PreparedStatement, parameters: sending [CassandraClient.Statement.Value] = [], options: CassandraClient.Statement.Options = .init(), on eventLoop: EventLoop? = .none, logger: Logger? = .none) -> EventLoopFuture<CassandraClient.Rows>
    /// Execute a prepared statement, decoding each row into `model`.
    ///
    /// This is equivalent to the sibling `execute(...)` overload that infers `T` purely from the return type,
    /// but spells out the decoded type explicitly at the call site, e.g.
    /// `session.execute(prepared: statement, withModelType: Model.self)`.
    ///
    /// If `eventLoop` is `nil`, a new one will get created through the `EventLoopGroup` provided during initialization.
    @preconcurrency public func execute<T>(prepared: CassandraClient.PreparedStatement, parameters: sending [CassandraClient.Statement.Value] = [], options: CassandraClient.Statement.Options = .init(), on eventLoop: EventLoop? = .none, logger: Logger? = .none, withModelType model: T.Type) -> EventLoopFuture<[T]> where T : Decodable, T : Sendable
    /// Prepare a CQL query for repeated execution.
    @available(macOS 12, watchOS 8, iOS 15, tvOS 15, *)
    public func prepare(_ query: String, encryptionTable: String? = nil, logger: Logger? = .none) async throws -> CassandraClient.PreparedStatement
    /// Prepare a CQL query for repeated execution.
    ///
    /// If `eventLoop` is `nil`, a new one will get created through the `EventLoopGroup` provided during initialization.
    public func prepare(_ query: String, encryptionTable: String? = nil, on eventLoop: EventLoop? = .none, logger: Logger? = .none) -> EventLoopFuture<CassandraClient.PreparedStatement>
    /// Query large data-sets where using an interator helps control memory usage.
    ///
    /// - Important:
    ///   - Advancing the iterator invalidates values retrieved by the previous iteration.
    ///   - Attempting to wrap the ``CassandraClient/Rows`` sequence in a list will not work, use the transformer variant instead.
    @available(macOS 12, watchOS 8, iOS 15, tvOS 15, *)
    public func query(_ query: String, parameters: [CassandraClient.Statement.Value] = [], options: CassandraClient.Statement.Options = .init(), logger: Logger? = .none) async throws -> CassandraClient.Rows
    /// Query small data-sets that fit into memory. Only use this when it's safe to buffer the entire data-set into memory.
    @available(macOS 12, watchOS 8, iOS 15, tvOS 15, *)
    public func query<T>(_ query: String, parameters: [CassandraClient.Statement.Value] = [], options: CassandraClient.Statement.Options = .init(), logger: Logger? = .none) async throws -> [T] where T : Decodable
    /// Query small data-sets that fit into memory. Only use this when it's safe to buffer the entire data-set into memory.
    @available(macOS 12, watchOS 8, iOS 15, tvOS 15, *)
    public func query<T>(_ query: String, parameters: [CassandraClient.Statement.Value] = [], options: CassandraClient.Statement.Options = .init(), logger: Logger? = .none, transform: @escaping (CassandraClient.Row) -> T?) async throws -> [T]
    /// Query small data-sets that fit into memory, decoding each row into `model`.
    ///
    /// This is equivalent to the sibling `query(...)` overload that infers `T` purely from the return type,
    /// but spells out the decoded type explicitly at the call site, e.g.
    /// `try await session.query("select ...", withModelType: Model.self)`.
    @available(macOS 12, watchOS 8, iOS 15, tvOS 15, *)
    public func query<T>(_ query: String, parameters: [CassandraClient.Statement.Value] = [], options: CassandraClient.Statement.Options = .init(), logger: Logger? = .none, withModelType model: T.Type) async throws -> [T] where T : Decodable
    /// Query small data-sets that fit into memory. Only use this when it's safe to buffer the entire data-set into memory.
    ///
    /// If `eventLoop` is `nil`, a new one will get created through the `EventLoopGroup` provided during initialization.
    @preconcurrency public func query<T>(_ query: String, parameters: sending [CassandraClient.Statement.Value] = [], options: CassandraClient.Statement.Options = .init(), on eventLoop: EventLoop? = .none, logger: Logger? = .none) -> EventLoopFuture<[T]> where T : Decodable, T : Sendable
    /// Query large data-sets where using an interator helps control memory usage.
    ///
    /// If `eventLoop` is `nil`, a new one will get created through the `EventLoopGroup` provided during initialization.
    ///
    /// - Important:
    ///   - Advancing the iterator invalidates values retrieved by the previous iteration.
    ///   - Attempting to wrap the ``CassandraClient/Rows`` sequence in a list will not work, use the transformer variant instead.
    public func query(_ query: String, parameters: sending [CassandraClient.Statement.Value] = [], options: CassandraClient.Statement.Options = .init(), on eventLoop: EventLoop? = .none, logger: Logger? = .none) -> EventLoopFuture<CassandraClient.Rows>
    /// Query small data-sets that fit into memory. Only use this when it is safe to buffer the entire data-set into memory.
    ///
    /// If `eventLoop` is `nil`, a new one will get created through the `EventLoopGroup` provided during initialization.
    @preconcurrency public func query<T>(_ query: String, parameters: sending [CassandraClient.Statement.Value] = [], options: CassandraClient.Statement.Options = .init(), on eventLoop: EventLoop? = .none, logger: Logger? = .none, transform: @escaping @Sendable (CassandraClient.Row) -> T?) -> EventLoopFuture<[T]>
    /// Query small data-sets that fit into memory, decoding each row into `model`.
    ///
    /// This is equivalent to the sibling `query(...)` overload that infers `T` purely from the return type,
    /// but spells out the decoded type explicitly at the call site, e.g.
    /// `session.query("select ...", withModelType: Model.self)`.
    ///
    /// If `eventLoop` is `nil`, a new one will get created through the `EventLoopGroup` provided during initialization.
    @preconcurrency public func query<T>(_ query: String, parameters: sending [CassandraClient.Statement.Value] = [], options: CassandraClient.Statement.Options = .init(), on eventLoop: EventLoop? = .none, logger: Logger? = .none, withModelType model: T.Type) -> EventLoopFuture<[T]> where T : Decodable, T : Sendable
    /// Query large data-sets where the number of rows fetched at a time is limited by `pageSize`.
    ///
    /// A non-positive `pageSize` fails the call with ``CassandraClient/Error/badParams(_:)``.
    @available(macOS 12, watchOS 8, iOS 15, tvOS 15, *)
    public func query(_ query: String, parameters: sending [CassandraClient.Statement.Value] = [], pageSize: Int32, options: CassandraClient.Statement.Options = .init(), logger: Logger? = .none) async throws -> CassandraClient.PaginatedRows
    /// Query large data-sets where the number of rows fetched at a time is limited by `pageSize`,
    /// decoding each row into `T` as the sequence is iterated, rather than materializing the whole
    /// decoded result set as an array.
    ///
    /// A non-positive `pageSize` fails the call with ``CassandraClient/Error/badParams(_:)``.
    ///
    /// - Note: Unlike the raw ``query(_:parameters:pageSize:options:logger:)`` sequence, decoded values
    ///   are independent Swift values and remain valid after the sequence is advanced.
    @available(macOS 12, watchOS 8, iOS 15, tvOS 15, *)
    public func query<T>(_ query: String, parameters: sending [CassandraClient.Statement.Value] = [], pageSize: Int32, options: CassandraClient.Statement.Options = .init(), logger: Logger? = .none) async throws -> AsyncThrowingMapSequence<CassandraClient.PaginatedRows, T> where T : Decodable, T : Sendable
    /// Query large data-sets where the number of rows fetched at a time is limited by `pageSize`,
    /// decoding each row into `model` as the sequence is iterated.
    ///
    /// This is equivalent to the sibling `query(...)` overload that infers `T` purely from the return type,
    /// but spells out the decoded type explicitly at the call site, e.g.
    /// `try await session.query("select ...", pageSize: 100, withModelType: Model.self)`.
    ///
    /// A non-positive `pageSize` fails the call with ``CassandraClient/Error/badParams(_:)``.
    @available(macOS 12, watchOS 8, iOS 15, tvOS 15, *)
    public func query<T>(_ query: String, parameters: sending [CassandraClient.Statement.Value] = [], pageSize: Int32, options: CassandraClient.Statement.Options = .init(), logger: Logger? = .none, withModelType model: T.Type) async throws -> AsyncThrowingMapSequence<CassandraClient.PaginatedRows, T> where T : Decodable, T : Sendable
    /// Query large data-sets where the number of rows fetched at a time is limited by `pageSize`.
    ///
    /// A non-positive `pageSize` fails the call with ``CassandraClient/Error/badParams(_:)``.
    ///
    /// If `eventLoop` is `nil`, a new one will get created through the `EventLoopGroup` provided during initialization.
    public func query(_ query: String, parameters: sending [CassandraClient.Statement.Value] = [], pageSize: Int32, options: CassandraClient.Statement.Options = .init(), on eventLoop: EventLoop? = .none, logger: Logger? = .none) -> EventLoopFuture<CassandraClient.PaginatedRows>
    /// Run  insert / update / delete or DDL commands where no result is expected
    @available(macOS 12, watchOS 8, iOS 15, tvOS 15, *)
    public func run(_ command: String, parameters: [CassandraClient.Statement.Value] = [], options: CassandraClient.Statement.Options = .init(), logger: Logger? = .none) async throws
    /// Run insert / update / delete or DDL command where no result is expected.
    ///
    /// If `eventLoop` is `nil`, a new one will get created through the `EventLoopGroup` provided during initialization.
    public func run(_ command: String, parameters: sending [CassandraClient.Statement.Value] = [], options: CassandraClient.Statement.Options = .init(), on eventLoop: EventLoop? = .none, logger: Logger? = .none) -> EventLoopFuture<Void>
}
````
