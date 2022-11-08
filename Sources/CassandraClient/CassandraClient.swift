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
@_implementationOnly import CDataStaxDriver
import Logging
import NIO
import NIOConcurrencyHelpers

/// `CassandraClient` is a wrapper around the [Datastax Cassandra C++ Driver](https://github.com/datastax/cpp-driver)
///  and can be used to run queries against a Cassandra database.
public class CassandraClient: CassandraSession {
    private let eventLoopGroupContainer: EventLoopGroupConainer
    public var eventLoopGroup: EventLoopGroup {
        self.eventLoopGroupContainer.value
    }

    private let configuration: Configuration
    private let logger: Logger
    private let defaultSession: Session
    private let isShutdown = ManagedAtomic<Bool>(false)

    /// Create a new instance of `CassandraClient`.
    ///
    /// - Parameters:
    ///   - eventLoopGroupProvider: The ``EventLoopGroupProvider`` to use, uses ``EventLoopGroupProvider/createNew`` strategy by default.
    ///   - configuration: The  client's ``Configuration``.
    ///   - logger: The client's default `Logger`.
    public init(eventLoopGroupProvider: EventLoopGroupProvider = .createNew, configuration: Configuration, logger: Logger? = nil) {
        self.configuration = configuration
        self.logger = logger ?? Logger(label: "com.apple.cassandra")
        switch eventLoopGroupProvider {
        case .createNew:
            self.eventLoopGroupContainer = (value: MultiThreadedEventLoopGroup(numberOfThreads: System.coreCount), managed: true)
        case .shared(let eventLoopGroup):
            self.eventLoopGroupContainer = (value: eventLoopGroup, managed: false)
        }
        self.defaultSession = Session(configuration: self.configuration, logger: self.logger, eventLoopGroupContainer: self.eventLoopGroupContainer)
    }

    deinit {
        precondition(self.isShutdown.load(ordering: .relaxed), "Client not shut down before the deinit. Please call client.shutdown() when no longer needed.")
    }

    /// Shutdown the client.
    ///
    /// - Note: It is required to call this method before terminating the program. `CassandraClient` will assert it was cleanly shut down as part of its deinitializer.
    public func shutdown() throws {
        if !self.isShutdown.compareExchange(expected: false, desired: true, ordering: .relaxed).exchanged {
            return
        }

        var lastError: Swift.Error?

        do {
            try self.defaultSession.shutdown()
        } catch {
            lastError = error
        }
        if self.eventLoopGroupContainer.managed {
            do {
                try self.eventLoopGroup.syncShutdownGracefully()
            } catch {
                lastError = error
            }
        }
        if let error = lastError {
            throw error
        }
    }

    /// Execute a ``Statement`` using the default ``CassandraSession`` on the given `EventLoop` or create a new one.
    ///
    /// **All** rows are returned.
    ///
    /// - Parameters:
    ///   - statement: The ``Statement`` to execute.
    ///   - eventLoop: The `EventLoop` to use, or create a new one.
    ///   - logger: If `nil`, the client's default `Logger` is used.
    ///
    /// - Returns: The resulting ``Rows``.
    public func execute(statement: Statement, on eventLoop: EventLoop?, logger: Logger? = .none) -> EventLoopFuture<Rows> {
        self.defaultSession.execute(statement: statement, on: eventLoop, logger: logger)
    }

    /// Execute a ``Statement`` using the default ``CassandraSession`` on the given `EventLoop` or create a new one.
    ///
    /// Resulting rows are paginated.
    ///
    /// - Parameters:
    ///   - statement: The ``Statement`` to execute.
    ///   - pageSize: The maximum number of rows returned per page.
    ///   - eventLoop: The `EventLoop` to use, or create a new one.
    ///   - logger: If `nil`, the client's default `Logger` is used.
    ///
    /// - Returns: The ``PaginatedRows``.
    public func execute(statement: Statement, pageSize: Int32, on eventLoop: EventLoop?, logger: Logger? = .none) -> EventLoopFuture<PaginatedRows> {
        self.defaultSession.execute(statement: statement, pageSize: pageSize, on: eventLoop, logger: logger)
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
        return Session(configuration: configuration, logger: logger, eventLoopGroupContainer: self.eventLoopGroupContainer)
    }

    /// Create a new ``CassandraSession`` for the given or configured keyspace then invoke the closure.
    ///
    /// - Parameters:
    ///   - keyspace: If `nil`, the client's default keyspace is used.
    ///   - logger: If `nil`, the client's default `Logger` is used.
    ///   - handler: The closure to invoke, passing in the newly created session.
    public func withSession(keyspace: String?, logger: Logger? = .none, handler: (CassandraSession) throws -> Void) rethrows {
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

    /// Create a new ``CassandraSession`` for the given or configured keyspace then invoke the closure and return its `EventLoopFuture` result.
    ///
    /// - Parameters:
    ///   - keyspace: If `nil`, the client's default keyspace is used.
    ///   - logger: If `nil`, the client's default `Logger` is used.
    ///   - handler: The closure to invoke, passing in the newly created session.
    ///
    /// - Returns: The resulting `EventLoopFuture` of the closure.
    public func withSession<T>(keyspace: String?, logger: Logger? = .none, handler: (CassandraSession) -> EventLoopFuture<T>) -> EventLoopFuture<T> {
        let session = self.makeSession(keyspace: keyspace, logger: logger)
        return handler(session).always { _ in
            do {
                try session.shutdown()
            } catch {
                self.logger.warning("shutdown error: \(error)")
            }
        }
    }

    public func getMetrics() -> CassandraMetrics {
        self.defaultSession.getMetrics()
    }

    /// A `EventLoopGroupProvider` defines how the underlying `EventLoopGroup` used to create the `EventLoop` is provided.
    ///
    /// When `shared`, the `EventLoopGroup` is provided externally and its lifecycle will be managed by the caller.
    /// When `createNew`, the library will create a new `EventLoopGroup` and manage its lifecycle.
    public enum EventLoopGroupProvider {
        case shared(EventLoopGroup)
        case createNew
    }
}

#if compiler(>=5.5) && canImport(_Concurrency)
public extension CassandraClient {
    /// Execute a ``Statement`` using the default ``CassandraSession``.
    ///
    /// **All** rows are returned.
    ///
    /// - Parameters:
    ///   - statement: The ``Statement`` to execute.
    ///   - logger: If `nil`, the client's default `Logger` is used.
    ///
    /// - Returns: The resulting ``Rows``.
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    func execute(statement: Statement, logger: Logger? = .none) async throws -> Rows {
        try await self.defaultSession.execute(statement: statement, logger: logger)
    }

    /// Execute a ``Statement`` using the default ``CassandraSession``.
    ///
    /// Resulting rows are paginated.
    ///
    /// - Parameters:
    ///   - statement: The ``Statement`` to execute.
    ///   - pageSize: The maximum number of rows returned per page.
    ///   - logger: If `nil`, the client's default `Logger` is used.
    ///
    /// - Returns: The ``PaginatedRows``.
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    func execute(statement: Statement, pageSize: Int32, logger: Logger? = .none) async throws -> PaginatedRows {
        try await self.defaultSession.execute(statement: statement, pageSize: pageSize, logger: logger)
    }

    /// Create a new ``CassandraSession`` for the given or configured keyspace then invoke the closure.
    ///
    /// - Parameters:
    ///   - keyspace: If `nil`, the client's default keyspace is used.
    ///   - logger: If `nil`, the client's default `Logger` is used.
    ///   - closure: The closure to invoke, passing in the newly created session.
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    func withSession(keyspace: String?, logger: Logger? = .none, closure: (CassandraSession) async throws -> Void) async throws {
        let session = self.makeSession(keyspace: keyspace, logger: logger)
        defer {
            do {
                try session.shutdown()
            } catch {
                self.logger.warning("shutdown error: \(error)")
            }
        }
        try await closure(session)
    }

    /// Create a new ``CassandraSession`` for the given or configured keyspace then invoke the closure and return its result.
    ///
    /// - Parameters:
    ///   - keyspace: If `nil`, the client's default keyspace is used.
    ///   - logger: If `nil`, the client's default `Logger` is used.
    ///   - handler: The closure to invoke, passing in the newly created session.
    ///
    /// - Returns: The result of the closure.
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    func withSession<T>(keyspace: String?, logger: Logger? = .none, handler: (CassandraSession) async throws -> T) async throws -> T {
        let session = self.makeSession(keyspace: keyspace, logger: logger)
        defer {
            do {
                try session.shutdown()
            } catch {
                self.logger.warning("shutdown error: \(error)")
            }
        }
        return try await handler(session)
    }
}
#endif

internal typealias EventLoopGroupConainer = (value: EventLoopGroup, managed: Bool)
