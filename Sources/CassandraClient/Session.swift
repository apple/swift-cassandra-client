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

@_implementationOnly import CDataStaxDriver
import Dispatch
import Foundation
import Logging
import NIO
import NIOConcurrencyHelpers
import NIOCore  // for async-await bridge

/// API for executing statements against Cassandra.
public protocol CassandraSession {
    var eventLoopGroup: EventLoopGroup { get }

    /// Encryptor for transparent column encryption.
    @available(macOS 15.0, iOS 18.0, visionOS 2.0, *)
    var encryptor: CassandraClient.Encryptor? { get }

    /// Registered encrypted column schemas for automatic context building.
    @available(macOS 15.0, iOS 18.0, visionOS 2.0, *)
    var encryptionSchemas: [String: CassandraClient.EncryptionSchema] { get }

    /// The default keyspace for this session, used to resolve unqualified table names.
    var keyspace: String? { get }

    /// Execute a prepared statement.
    ///
    /// **All** rows are returned.
    ///
    /// - Parameters:
    ///   - statement: The ``CassandraClient/Statement`` to execute.
    ///   - eventLoop: The `EventLoop` to use. Optional.
    ///   - logger: The `Logger` to use. Optional.
    ///
    /// - Returns: The resulting ``CassandraClient/Rows``.
    func execute(
        statement: CassandraClient.Statement,
        on eventLoop: EventLoop?,
        logger: Logger?
    )
        -> EventLoopFuture<CassandraClient.Rows>

    /// Execute a prepared statement.
    ///
    /// Resulting rows are paginated.
    ///
    /// - Parameters:
    ///   - statement: The ``CassandraClient/Statement`` to execute.
    ///   - pageSize: The maximum number of rows returned per page.
    ///   - eventLoop: The `EventLoop` to use. Optional.
    ///   - logger: The `Logger` to use. Optional.
    ///
    /// - Returns: The resulting ``CassandraClient/PaginatedRows``.
    func execute(
        statement: CassandraClient.Statement,
        pageSize: Int32,
        on eventLoop: EventLoop?,
        logger: Logger?
    ) -> EventLoopFuture<CassandraClient.PaginatedRows>

    /// Execute a prepared statement.
    ///
    /// **All** rows are returned.
    ///
    /// - Parameters:
    ///   - statement: The ``CassandraClient/Statement`` to execute.
    ///   - logger: The `Logger` to use. Optional.
    ///
    /// - Returns: The resulting ``CassandraClient/Rows``.
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    func execute(
        statement: CassandraClient.Statement,
        logger: Logger?
    ) async throws
        -> CassandraClient.Rows

    /// Execute a prepared statement.
    ///
    /// Resulting rows are paginated.
    ///
    /// - Parameters:
    ///   - statement: The ``CassandraClient/Statement`` to execute.
    ///   - pageSize: The maximum number of rows returned per page.
    ///   - logger: The `Logger` to use. Optional.
    ///
    /// - Returns: The resulting ``CassandraClient/PaginatedRows``.
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    func execute(
        statement: CassandraClient.Statement,
        pageSize: Int32,
        logger: Logger?
    )
        async throws -> CassandraClient.PaginatedRows

    /// Terminate the session and free resources.
    func shutdown() throws

    /// Get metrics for this session.
    func getMetrics() -> CassandraMetrics
}

private let encryptionLogger = Logger(label: "cassandra.encryption")

extension CassandraSession {
    private func logDecryptedRows(count: Int, options: CassandraClient.Statement.Options, logger: Logger?) {
        if count > 0, options.hasEncryptionOptions {
            (logger ?? encryptionLogger).debug(
                "Decrypted rows",
                metadata: [
                    CassandraClient.EncryptionLogKey.rowsDecrypted: "\(count)"
                ]
            )
        }
    }

    /// Execute a prepared statement.
    ///
    /// **All** rows are returned.
    ///
    /// - Parameters:
    ///   - statement: The ``CassandraClient/Statement`` to execute.
    ///   - logger: The `Logger` to use. Optional.
    ///
    /// - Returns: The resulting ``CassandraClient/Rows``.
    internal func execute(
        statement: CassandraClient.Statement,
        logger: Logger? = .none
    )
        -> EventLoopFuture<CassandraClient.Rows>
    {
        self.execute(statement: statement, on: nil, logger: logger)
    }
}

extension CassandraSession {
    private func makeDecoder(
        row: CassandraClient.Row,
        options: CassandraClient.Statement.Options
    ) throws -> CassandraClient.RowDecoder {
        if #available(macOS 15.0, iOS 18.0, visionOS 2.0, *),
            let builder = options.encryptionContextBuilder,
            let encryptor = self.encryptor
        {
            let ctx = try builder(row)
            return CassandraClient.RowDecoder(
                row: row,
                encryptor: encryptor,
                rowContext: ctx
            )
        }
        if #available(macOS 15.0, iOS 18.0, visionOS 2.0, *),
            let tableName = options.encryptionTable,
            let encryptor = self.encryptor
        {
            let ctx = try self.buildEncryptionContext(
                row: row,
                tableName: tableName,
                encryptor: encryptor
            )
            return CassandraClient.RowDecoder(
                row: row,
                encryptor: encryptor,
                rowContext: ctx
            )
        }
        return CassandraClient.RowDecoder(row: row)
    }

    /// Creates a Statement with the session's encryptor injected from Configuration.
    @available(macOS 15.0, iOS 18.0, visionOS 2.0, *)
    private func makeStatement(
        query: String,
        parameters: [CassandraClient.Statement.Value],
        options: CassandraClient.Statement.Options
    ) throws -> CassandraClient.Statement {
        try self.validateEncryptionBindings(parameters: parameters, options: options)
        return try CassandraClient.Statement(
            query: query,
            parameters: parameters,
            options: options,
            _encryptor: self.encryptor
        )
    }

    /// Run insert / update / delete or DDL command where no result is expected.
    ///
    /// If `eventLoop` is `nil`, a new one will get created through the `EventLoopGroup` provided during initialization.
    public func run(
        _ command: String,
        parameters: [CassandraClient.Statement.Value] = [],
        options: CassandraClient.Statement.Options = .init(),
        on eventLoop: EventLoop? = .none,
        logger: Logger? = .none
    ) -> EventLoopFuture<Void> {
        self.query(command, parameters: parameters, options: options, on: eventLoop, logger: logger).map { _ in () }
    }

    /// Query small data-sets that fit into memory. Only use this when it is safe to buffer the entire data-set into memory.
    ///
    /// If `eventLoop` is `nil`, a new one will get created through the `EventLoopGroup` provided during initialization.
    public func query<T>(
        _ query: String,
        parameters: [CassandraClient.Statement.Value] = [],
        options: CassandraClient.Statement.Options = .init(),
        on eventLoop: EventLoop? = .none,
        logger: Logger? = .none,
        transform: @escaping (CassandraClient.Row) -> T?
    ) -> EventLoopFuture<[T]> {
        self.query(query, parameters: parameters, options: options, on: eventLoop, logger: logger).map {
            rows in
            rows.compactMap(transform)
        }
    }

    /// Query small data-sets that fit into memory. Only use this when it's safe to buffer the entire data-set into memory.
    ///
    /// If `eventLoop` is `nil`, a new one will get created through the `EventLoopGroup` provided during initialization.
    public func query<T: Decodable>(
        _ query: String,
        parameters: [CassandraClient.Statement.Value] = [],
        options: CassandraClient.Statement.Options = .init(),
        on eventLoop: EventLoop? = .none,
        logger: Logger? = .none
    ) -> EventLoopFuture<[T]> {
        self.query(query, parameters: parameters, options: options, on: eventLoop, logger: logger)
            .flatMapThrowing { rows in
                let result = try rows.map { row in
                    try T(from: self.makeDecoder(row: row, options: options))
                }
                self.logDecryptedRows(count: result.count, options: options, logger: logger)
                return result
            }
    }

    /// Query large data-sets where using an interator helps control memory usage.
    ///
    /// If `eventLoop` is `nil`, a new one will get created through the `EventLoopGroup` provided during initialization.
    ///
    /// - Important:
    ///   - Advancing the iterator invalidates values retrieved by the previous iteration.
    ///   - Attempting to wrap the ``CassandraClient/Rows`` sequence in a list will not work, use the transformer variant instead.
    public func query(
        _ query: String,
        parameters: [CassandraClient.Statement.Value] = [],
        options: CassandraClient.Statement.Options = .init(),
        on eventLoop: EventLoop? = .none,
        logger: Logger? = .none
    ) -> EventLoopFuture<CassandraClient.Rows> {
        do {
            let statement: CassandraClient.Statement
            if #available(macOS 15.0, iOS 18.0, visionOS 2.0, *) {
                statement = try self.makeStatement(query: query, parameters: parameters, options: options)
            } else {
                statement = try CassandraClient.Statement(query: query, parameters: parameters, options: options)
            }
            return self.execute(statement: statement, on: eventLoop, logger: logger)
        } catch {
            let eventLoop = eventLoop ?? eventLoopGroup.next()
            return eventLoop.makeFailedFuture(error)
        }
    }

    /// Query large data-sets where the number of rows fetched at a time is limited by `pageSize`.
    ///
    /// If `eventLoop` is `nil`, a new one will get created through the `EventLoopGroup` provided during initialization.
    public func query(
        _ query: String,
        parameters: [CassandraClient.Statement.Value] = [],
        pageSize: Int32,
        options: CassandraClient.Statement.Options = .init(),
        on eventLoop: EventLoop? = .none,
        logger: Logger? = .none
    ) -> EventLoopFuture<CassandraClient.PaginatedRows> {
        do {
            let statement: CassandraClient.Statement
            if #available(macOS 15.0, iOS 18.0, visionOS 2.0, *) {
                statement = try self.makeStatement(query: query, parameters: parameters, options: options)
            } else {
                statement = try CassandraClient.Statement(query: query, parameters: parameters, options: options)
            }
            return self.execute(statement: statement, pageSize: pageSize, on: eventLoop, logger: logger)
        } catch {
            let eventLoop = eventLoop ?? eventLoopGroup.next()
            return eventLoop.makeFailedFuture(error)
        }
    }
}

final class CassFuture<T>: Sendable {
    /// This can be nonisolated because the docs state that a CassFuture is thread-safe.
    /// See Sources/CDataStaxDriver/datastax-cpp-driver/topics/README.md
    private nonisolated(unsafe) let rawPointer: OpaquePointer
    private let mapper: @Sendable (OpaquePointer?) -> T

    init(rawPointer: OpaquePointer, mapper: @escaping @Sendable (OpaquePointer?) -> T) {
        self.rawPointer = rawPointer
        self.mapper = mapper
    }

    func complete(to promise: EventLoopPromise<T>) {
        setResultCallback { result in
            promise.completeWith(result)
        }
    }

    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    func await() async throws -> T {
        try await withCheckedThrowingContinuation { continuation in
            setResultCallback { result in
                continuation.resume(with: result)
            }
        }
    }

    func syncWait() throws -> T {
        var result: Result<T, any Error>?
        let semaphore = DispatchSemaphore(value: 0)
        self.setResultCallback {
            result = $0
            semaphore.signal()
        }
        semaphore.wait()
        return try result!.get()
    }

    private func setResultCallback(
        completion: @escaping (Result<T, any Error>) -> Void
    ) {
        let closure = unmanagedRetainedClosure {
            DispatchQueue.global().async {
                let result = self.result()
                cass_future_free(self.rawPointer)
                completion(result)
            }
        }
        cass_future_set_callback(self.rawPointer, { _, data in callAndReleaseUnmanagedClosure(data!) }, closure)
    }

    private func result() -> Result<T, any Error> {
        let resultCode = cass_future_error_code(self.rawPointer)
        if resultCode == CASS_OK {
            let resultPointer = cass_future_get_result(self.rawPointer)
            return .success(self.mapper(resultPointer))
        } else {
            var messageRaw: UnsafePointer<CChar>?
            var messageLength = Int()
            cass_future_error_message(self.rawPointer, &messageRaw, &messageLength)
            let message = messageRaw.map { String(cString: $0) }
            let error = CassandraClient.Error(resultCode, message: message)
            return .failure(error)
        }
    }
}

extension CassFuture where T == Void {
    convenience init(rawPointer: OpaquePointer) {
        self.init(rawPointer: rawPointer, mapper: { _ in })
    }
}

struct CassSession: Sendable, ~Copyable {
    /// This can be nonisolated because the docs state that a CassSession is thread-safe.
    /// See Sources/CDataStaxDriver/datastax-cpp-driver/topics/README.md
    private nonisolated(unsafe) let rawPointer: OpaquePointer

    init() {
        self.rawPointer = cass_session_new()
    }

    deinit {
        cass_session_free(self.rawPointer)
    }

    func connect(cluster: Cluster, keyspace: String?) -> CassFuture<Void> {
        let futurePointer =
            if let keyspace {
                cass_session_connect_keyspace(self.rawPointer, cluster.rawPointer, keyspace)
            } else {
                cass_session_connect(self.rawPointer, cluster.rawPointer)
            }
        return CassFuture(rawPointer: futurePointer!)
    }

    func execute(statement: CassandraClient.Statement) -> CassFuture<CassandraClient.Rows> {
        let futurePointer = cass_session_execute(self.rawPointer, statement.rawPointer)
        return CassFuture(rawPointer: futurePointer!, mapper: { CassandraClient.Rows($0!) })
    }

    func getMetrics() -> CassandraMetrics {
        var metrics = CDataStaxDriver.CassMetrics()
        cass_session_get_metrics(self.rawPointer, &metrics)
        return CassandraMetrics(metrics: metrics)
    }

    func close() -> CassFuture<Void> {
        let futurePointer = cass_session_close(self.rawPointer)
        return CassFuture(rawPointer: futurePointer!)
    }

}

extension CassandraClient {
    internal final class Session: CassandraSession {
        private let eventLoopGroupContainer: EventLoopGroupContainer
        public var eventLoopGroup: EventLoopGroup {
            self.eventLoopGroupContainer.value
        }

        @available(macOS 15.0, iOS 18.0, visionOS 2.0, *)
        public var encryptor: CassandraClient.Encryptor? {
            self.configuration.encryptor
        }

        @available(macOS 15.0, iOS 18.0, visionOS 2.0, *)
        public var encryptionSchemas: [String: CassandraClient.EncryptionSchema] {
            self.configuration.encryptionSchemas
        }

        public var keyspace: String? {
            self.configuration.keyspace
        }

        private let configuration: Configuration
        private let logger: Logger
        private var state = State.idle
        private let lock = Lock()

        private let underlying: CassSession

        private enum State {
            case idle
            case connectingFuture(EventLoopFuture<Void>)
            case connecting(ConnectionTask)
            case connected
            case disconnected
        }

        internal init(
            configuration: Configuration,
            logger: Logger,
            eventLoopGroupContainer: EventLoopGroupContainer
        ) {
            self.configuration = configuration
            self.logger = logger
            self.eventLoopGroupContainer = eventLoopGroupContainer
            self.underlying = .init()
        }

        deinit {
            guard case .disconnected = (self.lock.withLock { self.state }) else {
                preconditionFailure(
                    "Session not shut down before the deinit. Please call session.shutdown() when no longer needed."
                )
            }
        }

        func shutdown() throws {
            self.lock.lock()
            defer {
                self.state = .disconnected
                self.lock.unlock()
            }
            switch self.state {
            case .connected:
                try self.disconnect()
            default:
                break
            }
        }

        func execute(
            statement: Statement,
            on eventLoop: EventLoop?,
            logger: Logger? = .none
        )
            -> EventLoopFuture<Rows>
        {
            let eventLoop = eventLoop ?? self.eventLoopGroup.next()
            let logger = logger ?? self.logger

            self.lock.lock()
            switch self.state {
            case .idle:
                let future = self.connect(on: eventLoop, logger: logger)
                self.state = .connectingFuture(future)
                self.lock.unlock()
                return future.flatMap { _ in
                    self.lock.withLock {
                        self.state = .connected
                    }
                    return self.execute(statement: statement, on: eventLoop, logger: logger)
                }
            case .connectingFuture(let future):
                self.lock.unlock()
                return future.flatMap { _ in
                    self.execute(statement: statement, on: eventLoop, logger: logger)
                }
            case .connecting(let task):
                self.lock.unlock()
                let promise = eventLoop.makePromise(of: Rows.self)
                if #available(macOS 12, iOS 15, tvOS 15, watchOS 8, *) {
                    promise.completeWithTask {
                        try await task.task.value
                        return try await self.execute(statement: statement, logger: logger)
                    }
                }
                return promise.futureResult
            case .connected:
                self.lock.unlock()
                logger.debug("executing: \(statement.query)")
                logger.trace("\(statement.parameters)")
                let promise = eventLoop.makePromise(of: Rows.self)
                let future = self.underlying.execute(statement: statement)
                future.complete(to: promise)
                return promise.futureResult
            case .disconnected:
                self.lock.unlock()
                if self.eventLoopGroupContainer.managed {
                    // eventloop *is* shutdown now
                    preconditionFailure("client is disconnected")
                }
                return eventLoop.makeFailedFuture(Error.disconnected)
            }
        }

        func execute(
            statement: Statement,
            pageSize: Int32,
            on eventLoop: EventLoop?,
            logger: Logger? = .none
        ) -> EventLoopFuture<CassandraClient.PaginatedRows> {
            let eventLoop = eventLoop ?? self.eventLoopGroup.next()

            do {
                try statement.setPagingSize(pageSize)
            } catch {
                return eventLoop.makeFailedFuture(error)
            }

            return eventLoop.makeSucceededFuture(
                PaginatedRows(session: self, statement: statement, on: eventLoop, logger: logger)
            )
        }

        private func connect(on eventLoop: EventLoop, logger: Logger) -> EventLoopFuture<Void> {
            logger.debug("connecting to: \(self.configuration)")

            return self.configuration.makeCluster(on: eventLoop)
                .flatMap { cluster in
                    let promise = eventLoop.makePromise(of: Void.self)
                    let future = self.underlying.connect(cluster: cluster, keyspace: self.configuration.keyspace)
                    future.complete(to: promise)
                    return promise.futureResult
                }
        }

        private func disconnect() throws {
            let future = self.underlying.close()
            try future.syncWait()
        }

        func getMetrics() -> CassandraMetrics {
            self.underlying.getMetrics()
        }

        private struct ConnectionTask {
            private let _task: Any

            @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
            var task: Task<Void, Swift.Error> {
                self._task as! Task<Void, Swift.Error>
            }

            @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
            init(_ task: Task<Void, Swift.Error>) {
                self._task = task
            }
        }
    }
}

// MARK: - Cassandra session with async-await support

extension CassandraSession {
    /// Run  insert / update / delete or DDL commands where no result is expected
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    public func run(
        _ command: String,
        parameters: [CassandraClient.Statement.Value] = [],
        options: CassandraClient.Statement.Options = .init(),
        logger: Logger? = .none
    ) async throws {
        _ = try await self.query(command, parameters: parameters, options: options, logger: logger)
    }

    /// Query small data-sets that fit into memory. Only use this when it's safe to buffer the entire data-set into memory.
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    public func query<T>(
        _ query: String,
        parameters: [CassandraClient.Statement.Value] = [],
        options: CassandraClient.Statement.Options = .init(),
        logger: Logger? = .none,
        transform: @escaping (CassandraClient.Row) -> T?
    ) async throws -> [T] {
        let rows = try await self.query(
            query,
            parameters: parameters,
            options: options,
            logger: logger
        )
        return rows.compactMap(transform)
    }

    /// Query small data-sets that fit into memory. Only use this when it's safe to buffer the entire data-set into memory.
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    public func query<T: Decodable>(
        _ query: String,
        parameters: [CassandraClient.Statement.Value] = [],
        options: CassandraClient.Statement.Options = .init(),
        logger: Logger? = .none
    ) async throws -> [T] {
        let rows = try await self.query(
            query,
            parameters: parameters,
            options: options,
            logger: logger
        )
        let result = try rows.map { row in
            try T(from: self.makeDecoder(row: row, options: options))
        }
        self.logDecryptedRows(count: result.count, options: options, logger: logger)
        return result
    }

    /// Query large data-sets where using an interator helps control memory usage.
    ///
    /// - Important:
    ///   - Advancing the iterator invalidates values retrieved by the previous iteration.
    ///   - Attempting to wrap the ``CassandraClient/Rows`` sequence in a list will not work, use the transformer variant instead.
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    public func query(
        _ query: String,
        parameters: [CassandraClient.Statement.Value] = [],
        options: CassandraClient.Statement.Options = .init(),
        logger: Logger? = .none
    ) async throws -> CassandraClient.Rows {
        let statement: CassandraClient.Statement
        if #available(macOS 15.0, iOS 18.0, visionOS 2.0, *) {
            statement = try self.makeStatement(query: query, parameters: parameters, options: options)
        } else {
            statement = try CassandraClient.Statement(query: query, parameters: parameters, options: options)
        }
        return try await self.execute(statement: statement, logger: logger)
    }

    /// Query large data-sets where the number of rows fetched at a time is limited by `pageSize`.
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    public func query(
        _ query: String,
        parameters: [CassandraClient.Statement.Value] = [],
        pageSize: Int32,
        options: CassandraClient.Statement.Options = .init(),
        logger: Logger? = .none
    ) async throws -> CassandraClient.PaginatedRows {
        let statement: CassandraClient.Statement
        if #available(macOS 15.0, iOS 18.0, visionOS 2.0, *) {
            statement = try self.makeStatement(query: query, parameters: parameters, options: options)
        } else {
            statement = try CassandraClient.Statement(query: query, parameters: parameters, options: options)
        }
        return try await self.execute(statement: statement, pageSize: pageSize, logger: logger)
    }
}

extension CassandraClient.Session {
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    func execute(
        statement: CassandraClient.Statement,
        logger: Logger? = .none
    ) async throws
        -> CassandraClient.Rows
    {
        let logger = logger ?? self.logger

        lock.lock()
        switch state {
        case .idle:
            let task = self.connect(logger: logger)
            state = .connecting(ConnectionTask(task))
            lock.unlock()

            try await task.value
            lock.withLock {
                self.state = .connected
            }
            return try await self.execute(statement: statement, logger: logger)
        case .connectingFuture(let future):
            lock.unlock()
            try await future.get()
            return try await self.execute(statement: statement, logger: logger)
        case .connecting(let task):
            lock.unlock()
            try await task.task.value
            return try await self.execute(statement: statement, logger: logger)
        case .connected:
            lock.unlock()
            logger.debug("executing: \(statement.query)")
            logger.trace("\(statement.parameters)")
            let future = self.underlying.execute(statement: statement)
            return try await future.await()
        case .disconnected:
            lock.unlock()
            if eventLoopGroupContainer.managed {
                // eventloop *is* shutdown now
                preconditionFailure("client is disconnected")
            }
            throw CassandraClient.Error.disconnected
        }
    }

    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    func execute(
        statement: CassandraClient.Statement,
        pageSize: Int32,
        logger: Logger? = .none
    )
        async throws -> CassandraClient.PaginatedRows
    {
        try statement.setPagingSize(pageSize)
        return CassandraClient.PaginatedRows(session: self, statement: statement, logger: logger)
    }

    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    private func connect(logger: Logger) -> Task<Void, Swift.Error> {
        Task {
            logger.debug("connecting to: \(self.configuration)")

            let cluster = try await self.configuration.makeCluster()
            let future = self.underlying.connect(cluster: cluster, keyspace: self.configuration.keyspace)
            return try await future.await()
        }
    }
}

// MARK: - Helpers

// Convert closure to an unmanaged pointer with an unbalanced retain
private func unmanagedRetainedClosure(_ closure: @escaping () -> Void) -> UnsafeMutableRawPointer {
    let closureBoxed = Box(closure)
    return Unmanaged.passRetained(closureBoxed).toOpaque()
}

// Convert unmanaged pointer to a closure and consume unbalanced retain
private func callAndReleaseUnmanagedClosure(_ opaque: UnsafeRawPointer) {
    let unmanaged = Unmanaged<Box<() -> Void>>.fromOpaque(opaque)
    let closure = unmanaged.takeRetainedValue()
    closure.value()
}

private final class Box<T> {
    public let value: T

    public init(_ value: T) {
        self.value = value
    }
}
