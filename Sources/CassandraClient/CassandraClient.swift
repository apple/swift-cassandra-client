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

import Atomics
internal import CDataStaxDriver
import Logging
import NIO
import NIOConcurrencyHelpers
import ServiceLifecycle

/// `CassandraClient` is a wrapper around the [Datastax Cassandra C++ Driver](https://github.com/datastax/cpp-driver)
///  and can be used to run queries against a Cassandra database.
///
/// Use ``withClient(eventLoopGroup:configuration:_:)`` for work with a bounded scope. To own a client for longer,
/// create it with ``init(eventLoopGroup:configuration:)`` and run ``run()``, for example as a service in a
/// `ServiceGroup`.
public final class CassandraClient: Sendable {
    let eventLoopGroup: EventLoopGroup

    var encryptor: CassandraClient.Encryptor? {
        self.configuration.encryptor
    }

    var encryptionSchemas: [String: CassandraClient.EncryptionSchema] {
        self.configuration.encryptionSchemas
    }

    var keyspace: String? {
        self.configuration.keyspace
    }

    private let configuration: Configuration
    private let defaultSession: Session
    private let isShutdown = ManagedAtomic<Bool>(false)
    private let isRunning = ManagedAtomic<Bool>(false)

    /// Create a new instance of `CassandraClient`.
    ///
    /// The client connects lazily, on its first request. Metrics are polled only while ``run()`` runs.
    ///
    /// - Parameters:
    ///   - eventLoopGroup: The `EventLoopGroup` to use. Defaults to the process-wide shared
    ///     `MultiThreadedEventLoopGroup.singleton`. The client never shuts the group down; its owner does.
    ///   - configuration: The  client's ``Configuration``.
    public init(
        eventLoopGroup: EventLoopGroup = MultiThreadedEventLoopGroup.singleton,
        configuration: Configuration
    ) {
        self.configuration = configuration
        self.eventLoopGroup = eventLoopGroup
        self.defaultSession = Session(configuration: self.configuration, eventLoopGroup: eventLoopGroup)
    }

    // Debug builds only: in release the driver closes the session when it is freed.
    deinit {
        assert(
            self.isShutdown.load(ordering: .relaxed),
            "Client not shut down before the deinit. Please call client.shutdown() when no longer needed."
        )
    }

    /// Create a client, run it for the duration of `body`, then shut it down, whether `body` returns or throws.
    ///
    /// If `body` throws, its error is rethrown even when shutting the client down also fails.
    ///
    /// - Parameters:
    ///   - eventLoopGroup: The `EventLoopGroup` to use. Defaults to the process-wide shared
    ///     `MultiThreadedEventLoopGroup.singleton`.
    ///   - configuration: The client's ``Configuration``.
    ///   - body: The closure to invoke with the client.
    ///
    /// - Returns: The result of `body`.
    public static func withClient<T>(
        eventLoopGroup: EventLoopGroup = MultiThreadedEventLoopGroup.singleton,
        configuration: Configuration,
        _ body: (CassandraClient) async throws -> T
    ) async throws -> T {
        let client = CassandraClient(eventLoopGroup: eventLoopGroup, configuration: configuration)
        // Claimed before `body` starts, so a `run()` inside `body` throws rather than waiting on a
        // cancellation that only comes once `body` returns.
        try client.claimRun()
        return try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask {
                try await client.runClaimed()
            }
            let result: Result<T, any Swift.Error>
            do {
                result = .success(try await body(client))
            } catch {
                result = .failure(error)
            }
            // Cancelling ends `run()`, which shuts the client down.
            group.cancelAll()
            do {
                try await group.waitForAll()
            } catch {
                // An error from `body` takes precedence over one from shutting the client down.
                if case .failure = result {
                    return try result.get()
                }
                throw error
            }
            return try result.get()
        }
    }

    /// Run the client's background work until graceful shutdown is triggered or the calling task is cancelled,
    /// then shut the client down.
    ///
    /// The background work is the metrics poller, when ``Configuration/metricsEnabled`` is set. Requests do not
    /// need `run()` to be running.
    ///
    /// - Throws: ``CassandraClient/Error/disconnected`` if the client is already shut down, and
    ///   ``CassandraClient/Error/invalidState(_:)`` if `run()` has already been called.
    public func run() async throws {
        try self.claimRun()
        try await self.runClaimed()
    }

    private func claimRun() throws {
        guard !self.isShutdown.load(ordering: .relaxed) else {
            throw CassandraClient.Error.disconnected
        }
        guard self.isRunning.compareExchange(expected: false, desired: true, ordering: .relaxed).exchanged else {
            throw CassandraClient.Error.invalidState("run() has already been called on this client")
        }
    }

    private func runClaimed() async throws {
        let session = self.defaultSession
        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask {
                await session.runMetricsPoller()
            }
            do {
                try await gracefulShutdown()
            } catch is CancellationError {
                // Cancellation ends the run the same way graceful shutdown does.
            }
            group.cancelAll()
            try await group.waitForAll()
        }
        try await self.shutdownAsync()
    }

    /// Shutdown the client.
    ///
    /// - Note: It is required to call this method before terminating the program. `CassandraClient` asserts it
    ///   was cleanly shut down as part of its deinitializer in debug builds.
    @available(*, noasync, message: "Can block indefinitely, prefer shutdownAsync()", renamed: "shutdownAsync()")
    public func shutdown() throws {
        if !self.isShutdown.compareExchange(expected: false, desired: true, ordering: .relaxed)
            .exchanged
        {
            return
        }

        try self.defaultSession.shutdown()
    }

    /// Shutdown the client.
    public func shutdownAsync() async throws {
        if !self.isShutdown.compareExchange(expected: false, desired: true, ordering: .relaxed)
            .exchanged
        {
            return
        }

        try await self.defaultSession.shutdownAsync()
    }

    /// Get the driver's metrics snapshot.
    public func getMetrics() -> CassandraMetrics {
        self.defaultSession.getMetrics()
    }
}

extension CassandraClient: Service {}

// MARK: - Statements

extension CassandraClient {
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
    public func execute(statement: Statement, logger: Logger = Logger.current) async throws -> Rows {
        try await self.defaultSession.execute(statement: statement, logger: logger)
    }

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
    public func execute(
        statement: sending Statement,
        pageSize: Int32,
        logger: Logger = Logger.current
    ) async throws -> PaginatedRows {
        try await self.defaultSession.execute(statement: statement, pageSize: pageSize, logger: logger)
    }

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
    public func prepare(
        _ query: String,
        encryptionTable: String? = nil,
        logger: Logger = Logger.current
    ) async throws -> PreparedStatement {
        try await self.defaultSession.prepare(query, encryptionTable: encryptionTable, logger: logger)
    }

    /// Execute a ``PreparedStatement`` with bound parameters.
    ///
    /// - Parameters:
    ///   - prepared: The ``PreparedStatement`` to execute.
    ///   - parameters: The values to bind to the statement's `?` placeholders.
    ///   - options: Statement options (consistency, timeout, encryption context).
    ///   - logger: The `Logger` to use. Defaults to the task-local `Logger.current`.
    ///
    /// - Returns: The resulting ``Rows``.
    public func execute(
        prepared: PreparedStatement,
        parameters: [Statement.Value] = [],
        options: Statement.Options = .init(),
        logger: Logger = Logger.current
    ) async throws -> Rows {
        try await self.defaultSession.execute(
            prepared: prepared,
            parameters: parameters,
            options: options,
            logger: logger
        )
    }

    /// Execute a ``PreparedStatement`` and decode each row into a `Decodable` type.
    ///
    /// - Parameters:
    ///   - prepared: The ``PreparedStatement`` to execute.
    ///   - parameters: The values to bind to the statement's `?` placeholders.
    ///   - options: Statement options (consistency, timeout, encryption context).
    ///   - logger: The `Logger` to use. Defaults to the task-local `Logger.current`.
    ///
    /// - Returns: The decoded rows.
    public func execute<T: Decodable>(
        prepared: PreparedStatement,
        parameters: [Statement.Value] = [],
        options: Statement.Options = .init(),
        logger: Logger = Logger.current
    ) async throws -> [T] {
        try await self.defaultSession.execute(
            prepared: prepared,
            parameters: parameters,
            options: options,
            logger: logger
        )
    }

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
    public func execute<T: Decodable>(
        prepared: PreparedStatement,
        parameters: [Statement.Value] = [],
        options: Statement.Options = .init(),
        logger: Logger = Logger.current,
        withModelType model: T.Type
    ) async throws -> [T] {
        try await self.defaultSession.execute(
            prepared: prepared,
            parameters: parameters,
            options: options,
            logger: logger
        )
    }

    /// Execute a batch of statements.
    ///
    /// - Parameters:
    ///   - configuration: Options to apply to the batch.
    ///   - logger: The `Logger` to use. Defaults to the task-local `Logger.current`.
    ///   - build: Closure that adds statements to the batch.
    public func batch(
        configuration: Batch.Configuration = .init(),
        logger: Logger = Logger.current,
        _ build: (inout Batch) async throws -> Void
    ) async throws {
        try await self.defaultSession.batch(configuration: configuration, logger: logger, build)
    }
}

// MARK: - Queries

extension CassandraClient {
    /// Execute insert / update / delete or DDL commands where no result is expected.
    public func execute(
        _ command: String,
        parameters: [Statement.Value] = [],
        options: Statement.Options = .init(),
        logger: Logger = Logger.current
    ) async throws {
        try await self.defaultSession.execute(command, parameters: parameters, options: options, logger: logger)
    }

    @available(*, unavailable, renamed: "execute(_:parameters:options:logger:)")
    public func run(
        _ command: String,
        parameters: [Statement.Value] = [],
        options: Statement.Options = .init(),
        logger: Logger = Logger.current
    ) async throws {
        fatalError("unavailable")
    }

    /// Query small data-sets that fit into memory. Only use this when it's safe to buffer the entire data-set into memory.
    public func query<T>(
        _ query: String,
        parameters: [Statement.Value] = [],
        options: Statement.Options = .init(),
        logger: Logger = Logger.current,
        transform: @escaping (Row) -> T?
    ) async throws -> [T] {
        try await self.defaultSession.query(
            query,
            parameters: parameters,
            options: options,
            logger: logger,
            transform: transform
        )
    }

    /// Query small data-sets that fit into memory. Only use this when it's safe to buffer the entire data-set into memory.
    public func query<T: Decodable>(
        _ query: String,
        parameters: [Statement.Value] = [],
        options: Statement.Options = .init(),
        logger: Logger = Logger.current
    ) async throws -> [T] {
        try await self.defaultSession.query(query, parameters: parameters, options: options, logger: logger)
    }

    /// Query small data-sets that fit into memory, decoding each row into `model`.
    ///
    /// This is equivalent to the sibling `query(...)` overload that infers `T` purely from the return type,
    /// but spells out the decoded type explicitly at the call site, e.g.
    /// `try await cassandraClient.query("select ...", withModelType: Model.self)`.
    public func query<T: Decodable>(
        _ query: String,
        parameters: [Statement.Value] = [],
        options: Statement.Options = .init(),
        logger: Logger = Logger.current,
        withModelType model: T.Type
    ) async throws -> [T] {
        try await self.defaultSession.query(query, parameters: parameters, options: options, logger: logger)
    }

    /// Query large data-sets where using an interator helps control memory usage.
    ///
    /// - Important:
    ///   - Advancing the iterator invalidates values retrieved by the previous iteration.
    ///   - Attempting to wrap the ``Rows`` sequence in a list will not work, use the transformer variant instead.
    public func query(
        _ query: String,
        parameters: [Statement.Value] = [],
        options: Statement.Options = .init(),
        logger: Logger = Logger.current
    ) async throws -> Rows {
        try await self.defaultSession.query(query, parameters: parameters, options: options, logger: logger)
    }

    /// Query large data-sets where the number of rows fetched at a time is limited by `pageSize`.
    ///
    /// A non-positive `pageSize` fails the call with ``CassandraClient/Error/badParams(_:)``.
    public func query(
        _ query: String,
        parameters: sending [Statement.Value] = [],
        pageSize: Int32,
        options: Statement.Options = .init(),
        logger: Logger = Logger.current
    ) async throws -> PaginatedRows {
        try await self.defaultSession.query(
            query,
            parameters: parameters,
            pageSize: pageSize,
            options: options,
            logger: logger
        )
    }

    /// Query large data-sets where the number of rows fetched at a time is limited by `pageSize`,
    /// decoding each row into `T` as the sequence is iterated, rather than materializing the whole
    /// decoded result set as an array.
    ///
    /// A non-positive `pageSize` fails the call with ``CassandraClient/Error/badParams(_:)``.
    ///
    /// - Note: Unlike the raw ``query(_:parameters:pageSize:options:logger:)`` sequence, decoded values
    ///   are independent Swift values and remain valid after the sequence is advanced.
    public func query<T: Decodable & Sendable>(
        _ query: String,
        parameters: sending [Statement.Value] = [],
        pageSize: Int32,
        options: Statement.Options = .init(),
        logger: Logger = Logger.current
    ) async throws -> AsyncThrowingMapSequence<PaginatedRows, T> {
        try await self.defaultSession.query(
            query,
            parameters: parameters,
            pageSize: pageSize,
            options: options,
            logger: logger
        )
    }

    /// Query large data-sets where the number of rows fetched at a time is limited by `pageSize`,
    /// decoding each row into `model` as the sequence is iterated.
    ///
    /// This is equivalent to the sibling `query(...)` overload that infers `T` purely from the return type,
    /// but spells out the decoded type explicitly at the call site, e.g.
    /// `try await cassandraClient.query("select ...", pageSize: 100, withModelType: Model.self)`.
    ///
    /// A non-positive `pageSize` fails the call with ``CassandraClient/Error/badParams(_:)``.
    public func query<T: Decodable & Sendable>(
        _ query: String,
        parameters: sending [Statement.Value] = [],
        pageSize: Int32,
        options: Statement.Options = .init(),
        logger: Logger = Logger.current,
        withModelType model: T.Type
    ) async throws -> AsyncThrowingMapSequence<PaginatedRows, T> {
        try await self.defaultSession.query(
            query,
            parameters: parameters,
            pageSize: pageSize,
            options: options,
            logger: logger
        )
    }
}
