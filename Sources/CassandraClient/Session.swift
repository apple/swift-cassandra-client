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
    func prepare(
        _ query: String,
        encryptionTable: String?,
        on eventLoop: EventLoop?,
        logger: Logger?
    ) -> EventLoopFuture<CassandraClient.PreparedStatement>

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
    func execute(
        prepared: CassandraClient.PreparedStatement,
        parameters: [CassandraClient.Statement.Value],
        options: CassandraClient.Statement.Options,
        on eventLoop: EventLoop?,
        logger: Logger?
    ) -> EventLoopFuture<CassandraClient.Rows>

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
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    func prepare(
        _ query: String,
        encryptionTable: String?,
        logger: Logger?
    ) async throws -> CassandraClient.PreparedStatement

    /// Execute a prepared statement with bound parameters.
    ///
    /// - Parameters:
    ///   - prepared: The ``CassandraClient/PreparedStatement`` to execute.
    ///   - parameters: The values to bind to the statement's `?` placeholders.
    ///   - options: Statement options (consistency, timeout, encryption context).
    ///   - logger: The `Logger` to use. Optional.
    ///
    /// - Returns: The resulting ``CassandraClient/Rows``.
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    func execute(
        prepared: CassandraClient.PreparedStatement,
        parameters: [CassandraClient.Statement.Value],
        options: CassandraClient.Statement.Options,
        logger: Logger?
    ) async throws -> CassandraClient.Rows

    /// Execute a batch of statements.
    ///
    /// - Parameters:
    ///   - configuration: Options to apply to the batch.
    ///   - eventLoop: The `EventLoop` to use, or create a new one.
    ///   - logger: If `nil`, the client's default `Logger` is used.
    ///   - build: Closure that adds statements to the batch.
    func batch(
        configuration: CassandraClient.Batch.Configuration,
        on eventLoop: EventLoop?,
        logger: Logger?,
        _ build: (inout CassandraClient.Batch) throws -> Void
    ) -> EventLoopFuture<Void>

    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    func batch(
        configuration: CassandraClient.Batch.Configuration,
        logger: Logger?,
        _ build: (inout CassandraClient.Batch) async throws -> Void
    ) async throws

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

    /// Prepare a CQL query for repeated execution.
    ///
    /// If `eventLoop` is `nil`, a new one will get created through the `EventLoopGroup` provided during initialization.
    public func prepare(
        _ query: String,
        encryptionTable: String? = nil,
        on eventLoop: EventLoop? = .none,
        logger: Logger? = .none
    ) -> EventLoopFuture<CassandraClient.PreparedStatement> {
        self.prepare(query, encryptionTable: encryptionTable, on: eventLoop, logger: logger)
    }

    /// Execute a prepared statement with bound parameters.
    ///
    /// If `eventLoop` is `nil`, a new one will get created through the `EventLoopGroup` provided during initialization.
    public func execute(
        prepared: CassandraClient.PreparedStatement,
        parameters: [CassandraClient.Statement.Value] = [],
        options: CassandraClient.Statement.Options = .init(),
        on eventLoop: EventLoop? = .none,
        logger: Logger? = .none
    ) -> EventLoopFuture<CassandraClient.Rows> {
        do {
            let statement: CassandraClient.Statement
            if #available(macOS 15.0, iOS 18.0, visionOS 2.0, *) {
                try self.validateEncryptionBindings(
                    prepared: prepared,
                    parameters: parameters,
                    options: options
                )
                statement = try CassandraClient.Statement(
                    preparedRawPointer: prepared.bind(),
                    parameters: parameters,
                    options: options,
                    _encryptor: self.encryptor
                )
            } else {
                statement = try CassandraClient.Statement(
                    preparedRawPointer: prepared.bind(),
                    parameters: parameters,
                    options: options,
                    _encryptor: nil
                )
            }
            return self.execute(statement: statement, on: eventLoop, logger: logger)
        } catch {
            let eventLoop = eventLoop ?? eventLoopGroup.next()
            return eventLoop.makeFailedFuture(error)
        }
    }

    /// Execute a prepared statement and decode each row into a `Decodable` type.
    ///
    /// If `eventLoop` is `nil`, a new one will get created through the `EventLoopGroup` provided during initialization.
    public func execute<T: Decodable>(
        prepared: CassandraClient.PreparedStatement,
        parameters: [CassandraClient.Statement.Value] = [],
        options: CassandraClient.Statement.Options = .init(),
        on eventLoop: EventLoop? = .none,
        logger: Logger? = .none
    ) -> EventLoopFuture<[T]> {
        var effectiveOptions = options
        if #available(macOS 15.0, iOS 18.0, visionOS 2.0, *) {
            if effectiveOptions.encryptionTable == nil {
                effectiveOptions.encryptionTable = prepared.encryptionTable
            }
        }
        return self.execute(
            prepared: prepared,
            parameters: parameters,
            options: effectiveOptions,
            on: eventLoop,
            logger: logger
        ).flatMapThrowing { rows in
            let result = try rows.map { row in
                try T(from: self.makeDecoder(row: row, options: effectiveOptions))
            }
            self.logDecryptedRows(count: result.count, options: effectiveOptions, logger: logger)
            return result
        }
    }
}

final class CassFuture<T>: Sendable {
    /// This can be nonisolated because the docs state that a CassFuture is thread-safe.
    /// See Sources/CDataStaxDriver/datastax-cpp-driver/topics/README.md
    private nonisolated(unsafe) let rawPointer: OpaquePointer
    /// Extracts the value pointer from the completed future. This keeps the choice of C
    /// extraction API (`cass_future_get_result` vs `cass_future_get_prepared`) out of the
    /// mapper, which only ever sees the already-extracted value — never the future itself.
    private let extract: @Sendable (OpaquePointer) -> OpaquePointer?
    private let mapper: @Sendable (OpaquePointer?) -> T

    private init(
        rawPointer: OpaquePointer,
        extract: @escaping @Sendable (OpaquePointer) -> OpaquePointer?,
        mapper: @escaping @Sendable (OpaquePointer?) -> T
    ) {
        self.rawPointer = rawPointer
        self.extract = extract
        self.mapper = mapper
    }

    func asNIOFuture(eventLoop: any EventLoop) -> EventLoopFuture<T> {
        let promise = eventLoop.makePromise(of: T.self)
        self.setResultCallback { result in
            promise.completeWith(result)
        }
        return promise.futureResult
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
        let error = cass_future_set_callback(
            self.rawPointer,
            { _, data in callAndReleaseUnmanagedClosure(data!) },
            closure
        )
        if error == CASS_ERROR_LIB_CALLBACK_ALREADY_SET {
            assertionFailure("setResultCallback called multiple times. Only the first callback will be registered")
        }
    }

    private func result() -> Result<T, any Error> {
        let resultCode = cass_future_error_code(self.rawPointer)
        if resultCode == CASS_OK {
            return .success(self.mapper(self.extract(self.rawPointer)))
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

extension CassFuture {
    /// Wraps a future whose value is a query result (extracted via `cass_future_get_result`).
    convenience init(rawPointer: OpaquePointer, mapper: @escaping @Sendable (OpaquePointer?) -> T) {
        self.init(rawPointer: rawPointer, extract: { cass_future_get_result($0) }, mapper: mapper)
    }
}

extension CassFuture where T == OpaquePointer {
    /// Wraps a future whose value is a prepared statement (extracted via `cass_future_get_prepared`).
    convenience init(preparedFrom rawPointer: OpaquePointer) {
        self.init(rawPointer: rawPointer, extract: { cass_future_get_prepared($0) }, mapper: { $0! })
    }
}

extension CassFuture where T == Void {
    convenience init(rawPointer: OpaquePointer) {
        self.init(rawPointer: rawPointer, extract: { _ in nil }, mapper: { _ in })
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

    func prepare(query: String) -> CassFuture<OpaquePointer> {
        let futurePointer = cass_session_prepare(self.rawPointer, query)
        return CassFuture(preparedFrom: futurePointer!)
    }

    func execute(statement: CassandraClient.Statement) -> CassFuture<CassandraClient.Rows> {
        let futurePointer = cass_session_execute(self.rawPointer, statement.rawPointer)
        return CassFuture(rawPointer: futurePointer!, mapper: { CassandraClient.Rows($0!) })
    }

    func execute(batch: consuming CassandraClient.Batch) -> CassFuture<Void> {
        let futurePointer = cass_session_execute_batch(self.rawPointer, batch.rawPointer)
        return CassFuture(rawPointer: futurePointer!)
    }

    func getMetrics() -> CassandraMetrics {
        var metrics = CDataStaxDriver.CassMetrics()
        cass_session_get_metrics(self.rawPointer, &metrics)
        return CassandraMetrics(metrics: metrics)
    }

    func getSchemaMeta() -> OpaquePointer? {
        cass_session_get_schema_meta(self.rawPointer)
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
        private let _state = NIOLockedValueBox(State.idle)

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
            guard case .disconnected = (self._state.withLockedValue { $0 }) else {
                preconditionFailure(
                    "Session not shut down before the deinit. Please call session.shutdown() when no longer needed."
                )
            }
        }

        func shutdown() throws {
            try self._state.withLockedValue { state in
                defer {
                    state = .disconnected
                }
                switch state {
                case .connected:
                    try self.disconnect()
                default:
                    break
                }
            }
        }

        /// What `withConnection` should do after inspecting the connection state.
        private enum SyncConnectionAction {
            /// We started the connection; await it, then mark the session connected.
            case startedConnecting(EventLoopFuture<Void>)
            /// Someone else started the connection as a future; just await it.
            case awaitConnectingFuture(EventLoopFuture<Void>)
            /// Someone else started the connection as a task; just await it.
            case awaitConnecting(ConnectionTask)
            /// Already connected.
            case ready
            /// Session has been shut down.
            case disconnected
        }

        /// Ensure the session is connected, then invoke `body` on the given event loop.
        private func withConnection<Result>(
            on eventLoop: EventLoop?,
            logger: Logger?,
            _ body: @escaping (EventLoop, Logger) -> EventLoopFuture<Result>
        ) -> EventLoopFuture<Result> {
            let eventLoop = eventLoop ?? self.eventLoopGroup.next()
            let logger = logger ?? self.logger

            let action: SyncConnectionAction = self._state.withLockedValue { state in
                switch state {
                case .idle:
                    let future = self.connect(on: eventLoop, logger: logger)
                    state = .connectingFuture(future)
                    return .startedConnecting(future)
                case .connectingFuture(let future):
                    return .awaitConnectingFuture(future)
                case .connecting(let task):
                    return .awaitConnecting(task)
                case .connected:
                    return .ready
                case .disconnected:
                    return .disconnected
                }
            }

            switch action {
            case .startedConnecting(let future):
                return future.flatMap { _ in
                    self._state.withLockedValue { $0 = .connected }
                    return body(eventLoop, logger)
                }
            case .awaitConnectingFuture(let future):
                return future.flatMap { _ in
                    body(eventLoop, logger)
                }
            case .awaitConnecting(let task):
                let promise = eventLoop.makePromise(of: Result.self)
                if #available(macOS 12, iOS 15, tvOS 15, watchOS 8, *) {
                    promise.completeWithTask {
                        try await task.task.value
                        return try await body(eventLoop, logger).get()
                    }
                }
                return promise.futureResult
            case .ready:
                return body(eventLoop, logger)
            case .disconnected:
                if self.eventLoopGroupContainer.managed {
                    preconditionFailure("client is disconnected")
                }
                return eventLoop.makeFailedFuture(Error.disconnected)
            }
        }

        func execute(
            statement: Statement,
            on eventLoop: EventLoop?,
            logger: Logger? = .none
        ) -> EventLoopFuture<Rows> {
            self.withConnection(on: eventLoop, logger: logger) { eventLoop, logger in
                logger.debug("executing: \(statement.query)")
                logger.trace("\(statement.parameters)")
                let future = self.underlying.execute(statement: statement)
                return future.asNIOFuture(eventLoop: eventLoop)
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
                try statement.setPagingSize(Int(pageSize))
            } catch {
                return eventLoop.makeFailedFuture(error)
            }

            return eventLoop.makeSucceededFuture(
                PaginatedRows(session: self, statement: statement, on: eventLoop, logger: logger)
            )
        }

        func prepare(
            _ query: String,
            encryptionTable: String? = nil,
            on eventLoop: EventLoop?,
            logger: Logger? = .none
        ) -> EventLoopFuture<CassandraClient.PreparedStatement> {
            self.withConnection(on: eventLoop, logger: logger) { eventLoop, logger in
                logger.debug("preparing: \(query)")
                let future = self.underlying.prepare(query: query)
                return future.asNIOFuture(eventLoop: eventLoop).flatMapThrowing { rawPointer in
                    let pkColumns: [String]
                    if let tableName = encryptionTable {
                        pkColumns = try self.lookupPrimaryKeyColumnNames(tableName: tableName)
                    } else {
                        pkColumns = []
                    }
                    let prepared = CassandraClient.PreparedStatement(
                        rawPointer: rawPointer,
                        encryptionTable: encryptionTable,
                        primaryKeyColumnNames: pkColumns
                    )
                    return prepared
                }
            }
        }

        private func lookupPrimaryKeyColumnNames(tableName: String) throws -> [String] {
            let keyspace: String
            let table: String
            if let dotIndex = tableName.firstIndex(of: ".") {
                keyspace = String(tableName[tableName.startIndex..<dotIndex])
                table = String(tableName[tableName.index(after: dotIndex)...])
            } else {
                guard let sessionKeyspace = self.keyspace else {
                    throw CassandraClient.Error.encryptionConfigError(
                        "encryptionTable '\(tableName)' has no keyspace qualifier and session has no default keyspace"
                    )
                }
                keyspace = sessionKeyspace
                table = tableName
            }

            guard let schemaMeta = self.underlying.getSchemaMeta() else {
                throw CassandraClient.Error.encryptionConfigError(
                    "Cannot retrieve schema metadata from session"
                )
            }
            defer { cass_schema_meta_free(schemaMeta) }

            guard let keyspaceMeta = cass_schema_meta_keyspace_by_name(schemaMeta, keyspace) else {
                throw CassandraClient.Error.encryptionConfigError(
                    "Keyspace '\(keyspace)' not found in schema metadata"
                )
            }

            guard let tableMeta = cass_keyspace_meta_table_by_name(keyspaceMeta, table) else {
                throw CassandraClient.Error.encryptionConfigError(
                    "Table '\(table)' not found in keyspace '\(keyspace)' schema metadata"
                )
            }

            var names: [String] = []

            let partitionKeyCount = cass_table_meta_partition_key_count(tableMeta)
            for i in 0..<partitionKeyCount {
                guard let colMeta = cass_table_meta_partition_key(tableMeta, i) else { continue }
                var namePtr: UnsafePointer<CChar>?
                var nameLength = Int()
                cass_column_meta_name(colMeta, &namePtr, &nameLength)
                if let namePtr = namePtr {
                    let name = String(cString: namePtr).prefix(nameLength)
                    names.append(String(name))
                }
            }

            let clusteringKeyCount = cass_table_meta_clustering_key_count(tableMeta)
            for i in 0..<clusteringKeyCount {
                guard let colMeta = cass_table_meta_clustering_key(tableMeta, i) else { continue }
                var namePtr: UnsafePointer<CChar>?
                var nameLength = Int()
                cass_column_meta_name(colMeta, &namePtr, &nameLength)
                if let namePtr = namePtr {
                    let name = String(cString: namePtr).prefix(nameLength)
                    names.append(String(name))
                }
            }

            return names
        }

        /// Resolve encryption contexts for parameters that have `context: nil` using driver schema metadata.
        /// Discovers PK columns from Cassandra's metadata cache rather than requiring EncryptionSchema registration.
        @available(macOS 15.0, iOS 18.0, visionOS 2.0, *)
        private func resolveEncryptionContexts(
            prepared: CassandraClient.PreparedStatement,
            parameters: [CassandraClient.Statement.Value],
            options: CassandraClient.Statement.Options
        ) throws -> [CassandraClient.Statement.Value] {
            let tableName = options.encryptionTable ?? prepared.encryptionTable
            guard let tableName else { return parameters }

            let needsResolution = parameters.contains { $0.isEncrypted && $0.encryptionContext == nil }
            guard needsResolution else { return parameters }

            // Parse keyspace and table from encryptionTable option.
            let keyspace: String
            let table: String
            if let dotIndex = tableName.firstIndex(of: ".") {
                keyspace = String(tableName[tableName.startIndex..<dotIndex])
                table = String(tableName[tableName.index(after: dotIndex)...])
            } else {
                guard let sessionKeyspace = self.keyspace else {
                    throw CassandraClient.Error.encryptionConfigError(
                        "encryptionTable '\(tableName)' has no keyspace qualifier and session has no default keyspace"
                    )
                }
                keyspace = sessionKeyspace
                table = tableName
            }

            let pkColumnNames: [String]
            if !prepared.primaryKeyColumnNames.isEmpty {
                pkColumnNames = prepared.primaryKeyColumnNames
            } else {
                pkColumnNames = try self.lookupPrimaryKeyColumnNames(tableName: tableName)
            }

            // Build a map of parameter name → index for the prepared statement.
            var paramIndexByName: [String: Int] = [:]
            for i in 0..<parameters.count {
                if let name = prepared.parameterName(at: i) {
                    paramIndexByName[name] = i
                }
            }

            // Extract PK values from parameters.
            var keyComponents: [CassandraClient.KeyComponent] = []
            for pkCol in pkColumnNames {
                guard let paramIdx = paramIndexByName[pkCol] else {
                    throw CassandraClient.Error.encryptionConfigError(
                        "Cannot auto-infer encryption context: key column '\(pkCol)' is not present in the prepared statement parameters. Provide context manually."
                    )
                }
                let component = try Self.extractKeyComponent(from: parameters[paramIdx], columnName: pkCol)
                keyComponents.append(component)
            }

            let primaryKey = CassandraClient.PrimaryKey(from: keyComponents)
            let baseContext = CassandraClient.EncryptionContext.Base(
                keyspace: keyspace,
                table: table,
                primaryKey: primaryKey
            )

            // Replace context-less encrypted values with context-resolved ones.
            var resolved = parameters
            for i in 0..<resolved.count {
                guard resolved[i].isEncrypted, resolved[i].encryptionContext == nil else { continue }
                guard let columnName = prepared.parameterName(at: i) else {
                    throw CassandraClient.Error.encryptionConfigError(
                        "Cannot auto-infer encryption context: no column name for parameter at index \(i)"
                    )
                }
                resolved[i] = resolved[i].withContext(baseContext.forColumn(columnName))
            }

            return resolved
        }

        /// Extract a KeyComponent from a Statement.Value by inspecting its type.
        @available(macOS 15.0, iOS 18.0, visionOS 2.0, *)
        private static func extractKeyComponent(
            from value: CassandraClient.Statement.Value,
            columnName: String
        ) throws -> CassandraClient.KeyComponent {
            switch value {
            case .string(let v): return .string(v)
            case .uuid(let v): return .uuid(v)
            case .int32(let v): return .int32(v)
            case .int64(let v): return .int64(v)
            case .bytes(let v): return .data(Data(v))
            case .date(let v): return .date(v)
            default:
                throw CassandraClient.Error.encryptionConfigError(
                    "Cannot extract key component for column '\(columnName)': unsupported value type for primary key"
                )
            }
        }

        func execute(
            prepared: CassandraClient.PreparedStatement,
            parameters: [CassandraClient.Statement.Value] = [],
            options: CassandraClient.Statement.Options = .init(),
            on eventLoop: EventLoop? = .none,
            logger: Logger? = .none
        ) -> EventLoopFuture<CassandraClient.Rows> {
            do {
                let statement: CassandraClient.Statement
                if #available(macOS 15.0, iOS 18.0, visionOS 2.0, *) {
                    let resolvedParameters = try self.resolveEncryptionContexts(
                        prepared: prepared,
                        parameters: parameters,
                        options: options
                    )
                    try self.validateEncryptionBindings(
                        prepared: prepared,
                        parameters: resolvedParameters,
                        options: options
                    )
                    statement = try CassandraClient.Statement(
                        preparedRawPointer: prepared.bind(),
                        parameters: resolvedParameters,
                        options: options,
                        _encryptor: self.encryptor
                    )
                } else {
                    statement = try CassandraClient.Statement(
                        preparedRawPointer: prepared.bind(),
                        parameters: parameters,
                        options: options,
                        _encryptor: nil
                    )
                }
                return self.execute(statement: statement, on: eventLoop, logger: logger)
            } catch {
                let eventLoop = eventLoop ?? self.eventLoopGroup.next()
                return eventLoop.makeFailedFuture(error)
            }
        }

        func execute(
            batch: consuming Batch,
            on eventLoop: EventLoop?,
            logger: Logger?
        ) -> EventLoopFuture<Void> {
            // Use optionalBatch to prove to compiler that we only take it once
            var optionalBatch: CassandraClient.Batch? = batch
            return self.withConnection(on: eventLoop, logger: logger) { eventLoop, logger in
                logger.debug("executing batch")
                let future = self.underlying.execute(batch: optionalBatch.take()!)
                return future.asNIOFuture(eventLoop: eventLoop)
            }
        }

        /// Execute a batch of statements.
        ///
        /// - Parameters:
        ///   - configuration: Options to apply to the batch.
        ///   - eventLoop: The `EventLoop` to use, or create a new one.
        ///   - logger: If `nil`, the client's default `Logger` is used.
        ///   - build: Closure that adds statements to the batch.
        public func batch(
            configuration: Batch.Configuration = .init(),
            on eventLoop: EventLoop? = .none,
            logger: Logger? = .none,
            _ build: (inout Batch) throws -> Void
        ) -> EventLoopFuture<Void> {
            do {
                let resolver:
                    (
                        (
                            CassandraClient.PreparedStatement, [CassandraClient.Statement.Value],
                            CassandraClient.Statement.Options
                        ) throws -> CassandraClient.Statement
                    )?
                if #available(macOS 15.0, iOS 18.0, visionOS 2.0, *) {
                    resolver = { [self] prepared, parameters, options in
                        let resolvedParameters = try self.resolveEncryptionContexts(
                            prepared: prepared,
                            parameters: parameters,
                            options: options
                        )
                        try self.validateEncryptionBindings(
                            prepared: prepared,
                            parameters: resolvedParameters,
                            options: options
                        )
                        return try CassandraClient.Statement(
                            preparedRawPointer: prepared.bind(),
                            parameters: resolvedParameters,
                            options: options,
                            _encryptor: self.encryptor
                        )
                    }
                } else {
                    resolver = nil
                }
                var batch = try Batch(configuration: configuration, resolver: resolver)
                try build(&batch)
                return self.execute(batch: batch, on: eventLoop, logger: logger)
            } catch {
                let eventLoop = eventLoop ?? eventLoopGroup.next()
                return eventLoop.makeFailedFuture(error)
            }
        }

        private func connect(on eventLoop: EventLoop, logger: Logger) -> EventLoopFuture<Void> {
            logger.debug("connecting to: \(self.configuration)")

            return self.configuration.makeCluster(on: eventLoop)
                .flatMap { cluster in
                    let future = self.underlying.connect(cluster: cluster, keyspace: self.configuration.keyspace)
                    return future.asNIOFuture(eventLoop: eventLoop)
                }
        }

        private func disconnect() throws {
            let future = self.underlying.close()
            try future.syncWait()
        }

        func getMetrics() -> CassandraMetrics {
            self.underlying.getMetrics()
        }

        fileprivate struct ConnectionTask {
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

    /// Prepare a CQL query for repeated execution.
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    public func prepare(
        _ query: String,
        encryptionTable: String? = nil,
        logger: Logger? = .none
    ) async throws -> CassandraClient.PreparedStatement {
        try await self.prepare(query, encryptionTable: encryptionTable, logger: logger)
    }

    /// Execute a prepared statement with bound parameters.
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    public func execute(
        prepared: CassandraClient.PreparedStatement,
        parameters: [CassandraClient.Statement.Value] = [],
        options: CassandraClient.Statement.Options = .init(),
        logger: Logger? = .none
    ) async throws -> CassandraClient.Rows {
        try await self.execute(prepared: prepared, parameters: parameters, options: options, logger: logger)
    }

    /// Execute a prepared statement and decode each row into a `Decodable` type.
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    public func execute<T: Decodable>(
        prepared: CassandraClient.PreparedStatement,
        parameters: [CassandraClient.Statement.Value] = [],
        options: CassandraClient.Statement.Options = .init(),
        logger: Logger? = .none
    ) async throws -> [T] {
        var effectiveOptions = options
        if #available(macOS 15.0, iOS 18.0, visionOS 2.0, *) {
            if effectiveOptions.encryptionTable == nil {
                effectiveOptions.encryptionTable = prepared.encryptionTable
            }
        }
        let rows = try await self.execute(
            prepared: prepared,
            parameters: parameters,
            options: effectiveOptions,
            logger: logger
        )
        let result = try rows.map { row in
            try T(from: self.makeDecoder(row: row, options: effectiveOptions))
        }
        self.logDecryptedRows(count: result.count, options: effectiveOptions, logger: logger)
        return result
    }
}

extension CassandraClient.Session {
    /// What `withConnection should do after inspecting the connection state
    private enum AsyncConnectionAction {
        /// We started the connection; await it, then mark the session connected.
        case startedConnecting(ConnectionTask)
        /// Someone else started the connection as a task; just await it.
        case awaitConnecting(ConnectionTask)
        /// Someone else started the connection as a future; just await it.
        case awaitConnectingFuture(EventLoopFuture<Void>)
        /// Already connected.
        case ready
        /// Session has been shut down.
        case disconnected
    }

    /// Ensure the session is connected, then invoke `body`.
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    private func withConnection<T>(
        logger: Logger?,
        _ body: (Logger) async throws -> T
    ) async throws -> T {
        let logger = logger ?? self.logger

        let action: AsyncConnectionAction = self._state.withLockedValue { state in
            switch state {
            case .idle:
                let connectionTask = ConnectionTask(self.connect(logger: logger))
                state = .connecting(connectionTask)
                return .startedConnecting(connectionTask)
            case .connecting(let task):
                return .awaitConnecting(task)
            case .connectingFuture(let future):
                return .awaitConnectingFuture(future)
            case .connected:
                return .ready
            case .disconnected:
                return .disconnected
            }
        }

        switch action {
        case .startedConnecting(let task):
            try await task.task.value
            self._state.withLockedValue { $0 = .connected }
        case .awaitConnecting(let task):
            try await task.task.value
        case .awaitConnectingFuture(let future):
            try await future.get()
        case .ready:
            break
        case .disconnected:
            if eventLoopGroupContainer.managed {
                preconditionFailure("client is disconnected")
            }
            throw CassandraClient.Error.disconnected
        }
        return try await body(logger)
    }

    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    func execute(
        statement: CassandraClient.Statement,
        logger: Logger? = .none
    ) async throws
        -> CassandraClient.Rows
    {
        try await self.withConnection(logger: logger) { logger in
            logger.debug("executing: \(statement.query)")
            logger.trace("\(statement.parameters)")
            let future = self.underlying.execute(statement: statement)
            return try await future.await()
        }
    }

    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    func execute(
        statement: CassandraClient.Statement,
        pageSize: Int32,
        logger: Logger? = .none
    ) async throws -> CassandraClient.PaginatedRows {
        try statement.setPagingSize(pageSize)
        return CassandraClient.PaginatedRows(session: self, statement: statement, logger: logger)
    }

    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    func execute(
        batch: consuming CassandraClient.Batch,
        logger: Logger?
    ) async throws {
        // Use optionalBatch to prove to compiler that we only take it once
        var optionalBatch: CassandraClient.Batch? = batch
        try await self.withConnection(logger: logger) { logger in
            logger.debug("executing batch")
            let future = self.underlying.execute(batch: optionalBatch.take()!)
            try await future.await()
        }
    }

    /// Execute a batch of statements.
    ///
    /// - Parameters:
    ///   - configuration: Options to apply to the batch.
    ///   - logger: If `nil`, the client's default `Logger` is used.
    ///   - build: Closure that adds statements to the batch.
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    public func batch(
        configuration: CassandraClient.Batch.Configuration = .init(),
        logger: Logger? = .none,
        _ build: (inout CassandraClient.Batch) async throws -> Void
    ) async throws {
        let resolver:
            (
                (
                    CassandraClient.PreparedStatement, [CassandraClient.Statement.Value],
                    CassandraClient.Statement.Options
                ) throws -> CassandraClient.Statement
            )?
        if #available(macOS 15.0, iOS 18.0, visionOS 2.0, *) {
            resolver = { [self] prepared, parameters, options in
                let resolvedParameters = try self.resolveEncryptionContexts(
                    prepared: prepared,
                    parameters: parameters,
                    options: options
                )
                try self.validateEncryptionBindings(
                    prepared: prepared,
                    parameters: resolvedParameters,
                    options: options
                )
                return try CassandraClient.Statement(
                    preparedRawPointer: prepared.bind(),
                    parameters: resolvedParameters,
                    options: options,
                    _encryptor: self.encryptor
                )
            }
        } else {
            resolver = nil
        }
        var batch = try CassandraClient.Batch(configuration: configuration, resolver: resolver)
        try await build(&batch)
        try await self.execute(batch: batch, logger: logger)
    }

    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    func prepare(
        _ query: String,
        encryptionTable: String? = nil,
        logger: Logger? = .none
    ) async throws -> CassandraClient.PreparedStatement {
        let preparedRawPointer: OpaquePointer = try await self.withConnection(logger: logger) { logger in
            logger.debug("preparing: \(query)")
            return try await self.underlying.prepare(query: query).await()
        }
        let pkColumns: [String]
        if let tableName = encryptionTable {
            pkColumns = try self.lookupPrimaryKeyColumnNames(tableName: tableName)
        } else {
            pkColumns = []
        }
        return CassandraClient.PreparedStatement(
            rawPointer: preparedRawPointer,
            encryptionTable: encryptionTable,
            primaryKeyColumnNames: pkColumns
        )
    }

    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    func execute(
        prepared: CassandraClient.PreparedStatement,
        parameters: [CassandraClient.Statement.Value] = [],
        options: CassandraClient.Statement.Options = .init(),
        logger: Logger? = .none
    ) async throws -> CassandraClient.Rows {
        let statement: CassandraClient.Statement
        if #available(macOS 15.0, iOS 18.0, visionOS 2.0, *) {
            let resolvedParameters = try self.resolveEncryptionContexts(
                prepared: prepared,
                parameters: parameters,
                options: options
            )
            try self.validateEncryptionBindings(
                prepared: prepared,
                parameters: resolvedParameters,
                options: options
            )
            statement = try CassandraClient.Statement(
                preparedRawPointer: prepared.bind(),
                parameters: resolvedParameters,
                options: options,
                _encryptor: self.encryptor
            )
        } else {
            statement = try CassandraClient.Statement(
                preparedRawPointer: prepared.bind(),
                parameters: parameters,
                options: options,
                _encryptor: nil
            )
        }
        return try await self.execute(statement: statement, logger: logger)
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
