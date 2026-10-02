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

/// `CassandraClient` is a wrapper around the [Datastax Cassandra C++ Driver](https://github.com/datastax/cpp-driver)
///  and can be used to run queries against a Cassandra database.
public final class CassandraClient: CassandraSession, Sendable {
    public let eventLoopGroup: EventLoopGroup

    public var encryptor: CassandraClient.Encryptor? {
        self.configuration.encryptor
    }

    public var encryptionSchemas: [String: CassandraClient.EncryptionSchema] {
        self.configuration.encryptionSchemas
    }

    public var keyspace: String? {
        self.configuration.keyspace
    }

    private let configuration: Configuration
    public let logger: Logger
    private let defaultSession: Session
    private let isShutdown = ManagedAtomic<Bool>(false)

    /// Create a new instance of `CassandraClient`.
    ///
    /// - Parameters:
    ///   - eventLoopGroup: The `EventLoopGroup` to use. Defaults to the process-wide shared
    ///     `MultiThreadedEventLoopGroup.singleton`. The client never shuts the group down; its owner does.
    ///   - configuration: The  client's ``Configuration``.
    ///   - logger: The client's default `Logger`.
    public init(
        eventLoopGroup: EventLoopGroup = MultiThreadedEventLoopGroup.singleton,
        configuration: Configuration,
        logger: Logger? = nil
    ) {
        self.configuration = configuration
        self.logger = logger ?? Logger(label: "com.apple.cassandra")
        self.eventLoopGroup = eventLoopGroup
        self.defaultSession = Session(
            configuration: self.configuration,
            logger: self.logger,
            eventLoopGroup: eventLoopGroup
        )
    }

    deinit {
        precondition(
            self.isShutdown.load(ordering: .relaxed),
            "Client not shut down before the deinit. Please call client.shutdown() when no longer needed."
        )
    }

    /// Shutdown the client.
    ///
    /// - Note: It is required to call this method before terminating the program. `CassandraClient` will assert it was cleanly shut down as part of its deinitializer.
    @available(*, noasync, message: "Can block indefinitely, prefer shutdownAsync()", renamed: "shutdownAsync()")
    public func shutdown() throws {
        if !self.isShutdown.compareExchange(expected: false, desired: true, ordering: .relaxed)
            .exchanged
        {
            return
        }

        try self.defaultSession.shutdown()
    }

    public func shutdownAsync() async throws {
        if !self.isShutdown.compareExchange(expected: false, desired: true, ordering: .relaxed)
            .exchanged
        {
            return
        }

        try await self.defaultSession.shutdownAsync()
    }

    /// Create a new ``CassandraSession`` that can be used to perform queries on the given or configured keyspace.
    ///
    /// - Parameters:
    ///   - keyspace: If `nil`, the client's default keyspace is used.
    ///   - logger: If `nil`, the client's default `Logger` is used.
    ///
    /// - Returns: The newly created session.
    public func makeSession(keyspace: String?, logger: Logger? = .none) -> CassandraSession {
        var configuration = self.configuration
        configuration.keyspace = keyspace
        let logger = logger ?? self.logger
        return Session(
            configuration: configuration,
            logger: logger,
            eventLoopGroup: self.eventLoopGroup
        )
    }

    /// Create a new ``CassandraSession`` for the given or configured keyspace then invoke the closure.
    ///
    /// - Parameters:
    ///   - keyspace: If `nil`, the client's default keyspace is used.
    ///   - logger: If `nil`, the client's default `Logger` is used.
    ///   - handler: The closure to invoke, passing in the newly created session.
    public func withSession(
        keyspace: String?,
        logger: Logger? = .none,
        handler: (CassandraSession) throws -> Void
    ) rethrows {
        let session = self.makeSession(keyspace: keyspace, logger: logger)
        defer {
            do {
                try session.shutdown()
            } catch {
                self.logger.warning("shutdown error: \(error)")
            }
        }
        try handler(session)
    }

    public func getMetrics() -> CassandraMetrics {
        self.defaultSession.getMetrics()
    }

    /// Prepare a CQL query for repeated execution using the default ``CassandraSession``.
    ///
    /// - Parameters:
    ///   - query: The CQL query string with `?` placeholders.
    ///   - encryptionTable: The table name for encryption context resolution. If provided, PK column names are looked up at prepare time.
    ///   - logger: If `nil`, the client's default `Logger` is used.
    ///
    /// - Returns: A ``PreparedStatement``.
    public func prepare(
        _ query: String,
        encryptionTable: String? = nil,
        logger: Logger? = .none
    ) async throws -> PreparedStatement {
        try await self.defaultSession.prepare(query, encryptionTable: encryptionTable, logger: logger)
    }

    /// Execute a ``PreparedStatement`` with bound parameters using the default ``CassandraSession``.
    ///
    /// - Parameters:
    ///   - prepared: The ``PreparedStatement`` to execute.
    ///   - parameters: The values to bind to the statement's `?` placeholders.
    ///   - options: Statement options (consistency, timeout, encryption context).
    ///   - logger: If `nil`, the client's default `Logger` is used.
    ///
    /// - Returns: The resulting ``Rows``.
    public func execute(
        prepared: PreparedStatement,
        parameters: [Statement.Value] = [],
        options: Statement.Options = .init(),
        logger: Logger? = .none
    ) async throws -> Rows {
        try await self.defaultSession.execute(
            prepared: prepared,
            parameters: parameters,
            options: options,
            logger: logger
        )
    }

    /// Execute a ``PreparedStatement`` and decode each row into a `Decodable` type using the default ``CassandraSession``.
    ///
    /// - Parameters:
    ///   - prepared: The ``PreparedStatement`` to execute.
    ///   - parameters: The values to bind to the statement's `?` placeholders.
    ///   - options: Statement options (consistency, timeout, encryption context).
    ///   - logger: If `nil`, the client's default `Logger` is used.
    ///
    /// - Returns: The decoded rows.
    public func execute<T: Decodable>(
        prepared: PreparedStatement,
        parameters: [Statement.Value] = [],
        options: Statement.Options = .init(),
        logger: Logger? = .none
    ) async throws -> [T] {
        try await self.defaultSession.execute(
            prepared: prepared,
            parameters: parameters,
            options: options,
            logger: logger
        )
    }

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
    public func execute<T: Decodable>(
        prepared: PreparedStatement,
        parameters: [Statement.Value] = [],
        options: Statement.Options = .init(),
        logger: Logger? = .none,
        withModelType model: T.Type
    ) async throws -> [T] {
        try await self.defaultSession.execute(
            prepared: prepared,
            parameters: parameters,
            options: options,
            logger: logger
        )
    }
}

extension CassandraClient {
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
    public func execute(statement: Statement, logger: Logger? = .none) async throws -> Rows {
        try await self.defaultSession.execute(statement: statement, logger: logger)
    }

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
    public func execute(
        statement: sending Statement,
        pageSize: Int32,
        logger: Logger? = .none
    ) async throws
        -> PaginatedRows
    {
        try await self.defaultSession.execute(
            statement: statement,
            pageSize: pageSize,
            logger: logger
        )
    }

    /// Execute a batch of statements.
    ///
    /// - Parameters:
    ///   - configuration: Options to apply to the batch.
    ///   - logger: If `nil`, the client's default `Logger` is used.
    ///   - build: Closure that adds statements to the batch.
    public func batch(
        configuration: Batch.Configuration = .init(),
        logger: Logger? = .none,
        _ build: (inout Batch) async throws -> Void
    ) async throws {
        try await self.defaultSession.batch(configuration: configuration, logger: logger, build)
    }

    /// Create a new ``CassandraSession`` for the given or configured keyspace then invoke the closure.
    ///
    /// - Parameters:
    ///   - keyspace: If `nil`, the client's default keyspace is used.
    ///   - logger: If `nil`, the client's default `Logger` is used.
    ///   - closure: The closure to invoke, passing in the newly created session.
    public func withSession(
        keyspace: String?,
        logger: Logger? = .none,
        closure: (CassandraSession) async throws -> Void
    ) async throws {
        try await self.withSession(keyspace: keyspace, logger: logger, handler: closure)
    }

    /// Create a new ``CassandraSession`` for the given or configured keyspace then invoke the closure and return its result.
    ///
    /// - Parameters:
    ///   - keyspace: If `nil`, the client's default keyspace is used.
    ///   - logger: If `nil`, the client's default `Logger` is used.
    ///   - handler: The closure to invoke, passing in the newly created session.
    ///
    /// - Returns: The result of the closure.
    public func withSession<T>(
        keyspace: String?,
        logger: Logger? = .none,
        handler: (CassandraSession) async throws -> T
    ) async throws -> T {
        let session = self.makeSession(keyspace: keyspace, logger: logger)
        let result: Result<T, any Swift.Error>
        do {
            result = try await .success(handler(session))
        } catch {
            result = .failure(error)
        }
        do {
            try await session.shutdownAsync()
        } catch {
            self.logger.warning("shutdown error: \(error)")
        }
        return try result.get()
    }
}
