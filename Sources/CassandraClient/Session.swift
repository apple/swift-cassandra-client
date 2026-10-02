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

extension CassandraClient.Session {
    private func logDecryptedRows(count: Int, options: CassandraClient.Statement.Options, logger: Logger) {
        if count > 0, options.hasEncryptionOptions {
            logger.debug(
                "Decrypted rows",
                metadata: [
                    CassandraClient.EncryptionLogKey.rowsDecrypted: "\(count)"
                ]
            )
        }
    }

}

extension CassandraClient.Session {
    private func makeDecoder(
        row: CassandraClient.Row,
        options: CassandraClient.Statement.Options
    ) throws -> CassandraClient.RowDecoder {
        if let builder = options.encryptionContextBuilder,
            let encryptor = self.encryptor
        {
            let ctx = try builder(row)
            return CassandraClient.RowDecoder(
                row: row,
                encryptor: encryptor,
                rowContext: ctx
            )
        }
        if let tableName = options.encryptionTable,
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
            encryptor: self.encryptor
        )
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

    func await() async throws -> T where T: Sendable {
        try await withCheckedThrowingContinuation { continuation in
            setResultCallback { result in
                continuation.resume(with: result)
            }
        }
    }

    @available(*, noasync, message: "Can block indefinitely, prefer await()", renamed: "await()")
    func syncWait() throws -> T where T: Sendable {
        let resultBox = NIOLockedValueBox<Result<T, any Error>?>(nil)
        let semaphore = DispatchSemaphore(value: 0)
        self.setResultCallback { result in
            resultBox.withLockedValue { $0 = result }
            semaphore.signal()
        }
        semaphore.wait()
        return try resultBox.withLockedValue { $0! }.get()
    }

    private func setResultCallback(
        completion: @escaping @Sendable (Result<T, any Error>) -> Void
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

extension CassFuture where T == CassPrepared {
    /// Wraps a future whose value is a prepared statement (extracted via `cass_future_get_prepared`).
    convenience init(preparedFrom rawPointer: OpaquePointer) {
        self.init(
            rawPointer: rawPointer,
            extract: { cass_future_get_prepared($0) },
            mapper: { CassPrepared(rawPointer: $0!) }
        )
    }
}

extension CassFuture where T == Void {
    convenience init(rawPointer: OpaquePointer) {
        self.init(rawPointer: rawPointer, extract: { _ in nil }, mapper: { _ in })
    }
}

/// A `Sendable` wrapper around a `CassPrepared*` pointer. The docs state a prepared statement is
/// read-only and "thread-safe to concurrently bind", so the pointer is safe to cross concurrency boundaries.
/// See Sources/CDataStaxDriver/datastax-cpp-driver/include/cassandra.h
final class CassPrepared: Sendable {
    private nonisolated(unsafe) let rawPointer: OpaquePointer

    init(rawPointer: OpaquePointer) {
        self.rawPointer = rawPointer
    }

    func parameterName(count: Int, namePtr: inout UnsafePointer<CChar>?, nameLength: inout Int) -> CassError {
        cass_prepared_parameter_name(self.rawPointer, count, &namePtr, &nameLength)
    }

    func bind() -> OpaquePointer {
        cass_prepared_bind(self.rawPointer)
    }

    deinit {
        cass_prepared_free(self.rawPointer)
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

    func prepare(query: String) -> CassFuture<CassPrepared> {
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
    internal final class Session: Sendable {
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
        private let _state = NIOLockedValueBox(State.idle)
        /// Set by `stopMetricsPoller()` when shutdown begins; a poller tick records gauges only while it is
        /// `false`. See `runMetricsPoller()`.
        private let _pollerStopped = NIOLockedValueBox(false)

        private let underlying: CassSession

        private enum State {
            case idle
            case connecting(ConnectionTask)
            case connected
            case disconnecting(ConnectionTask)
            case disconnectingFuture(EventLoopFuture<Void>)
            case disconnected
        }

        internal init(
            configuration: Configuration,
            eventLoopGroup: EventLoopGroup
        ) {
            self.configuration = configuration
            self.eventLoopGroup = eventLoopGroup
            self.underlying = .init()
        }

        // Debug builds only: in release the driver closes the session when `CassSession` frees it.
        deinit {
            if case .disconnected = (self._state.withLockedValue { $0 }) {
                return
            }
            assertionFailure("Session not shut down before the deinit. Please call shutdown() when no longer needed.")
        }

        @available(*, noasync, message: "Can block indefinitely, prefer shutdownAsync()", renamed: "shutdownAsync()")
        func shutdown() throws {
            enum Action {
                case alreadyShut
                case disconnectThenMarkShut(EventLoopPromise<Void>)
                case wait(EventLoopFuture<Void>)
            }
            let action: Action = self._state.withLockedValue { state in
                switch state {
                case .connected:
                    let el = self.eventLoopGroup.next()
                    let promise = el.makePromise(of: Void.self)
                    state = .disconnectingFuture(promise.futureResult)
                    return .disconnectThenMarkShut(promise)
                case .disconnecting:
                    preconditionFailure("Cannot call sync shutdown after async shutdown")
                case .disconnectingFuture(let existing):
                    return .wait(existing)
                case .idle, .connecting, .disconnected:
                    state = .disconnected
                    return .alreadyShut
                }
            }
            switch action {
            case .alreadyShut:
                return
            case .wait(let future):
                try future.wait()
            case .disconnectThenMarkShut(let promise):
                self.stopMetricsPoller()
                let future = self.underlying.close()
                do {
                    try future.syncWait()
                    promise.succeed()
                } catch {
                    promise.fail(error)
                }
                self._state.withLockedValue { state in
                    switch state {
                    case .disconnectingFuture:
                        state = .disconnected
                    default:
                        preconditionFailure("State changed from disconnecting to not disconnecting unexpectedly")
                    }
                }
            }
        }

        func shutdownAsync() async throws {
            enum Action {
                case alreadyShut
                case waitThenMarkShut(ConnectionTask)
                case wait(ConnectionTask)
                case waitFuture(EventLoopFuture<Void>)
            }
            let action: Action = self._state.withLockedValue { state in
                switch state {
                case .connected:
                    let task = ConnectionTask(
                        Task {
                            try await self.disconnect()
                        }
                    )
                    state = .disconnecting(task)
                    return .waitThenMarkShut(task)
                case .disconnecting(let existing):
                    return .wait(existing)
                case .disconnectingFuture(let existing):
                    return .waitFuture(existing)
                case .idle, .connecting, .disconnected:
                    state = .disconnected
                    return .alreadyShut
                }
            }
            switch action {
            case .alreadyShut:
                return
            case .waitThenMarkShut(let task):
                // Defer state change: always change the state to disconnected even if task.value throws
                defer {
                    self._state.withLockedValue { state in
                        switch state {
                        case .disconnecting, .disconnectingFuture:
                            state = .disconnected
                        default:
                            preconditionFailure("State changed from disconnecting to not disconnecting unexpectedly")
                        }
                    }
                }
                try await task.task.value
            case .waitFuture(let future):
                try await future.get()
            case .wait(let task):
                try await task.task.value
            }
        }

        private func handleConnectionSucceeded() throws {
            try self._state.withLockedValue { state in
                switch state {
                case .connecting:
                    state = .connected
                case .disconnected, .disconnecting, .disconnectingFuture:
                    // Shut down while connecting, stay disconnected
                    throw Error.disconnected
                case .idle, .connected:
                    // Unreachable: exactly one request moves `.idle` -> connecting and is
                    // the sole caller here, so the state is always a connecting state or
                    // `.disconnected`.
                    assertionFailure("handleConnectionSucceeded called in unexpected state \(state)")
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

        private func disconnect() async throws {
            self.stopMetricsPoller()
            let future = self.underlying.close()
            try await future.await()
        }

        func getMetrics() -> CassandraMetrics {
            self.underlying.getMetrics()
        }

        /// Poll the driver's metrics snapshot on the configured cadence until the calling task is cancelled.
        /// Returns at once when metrics are disabled or the interval is `nil` or not positive.
        ///
        /// A tick records gauges only while the session is connected and shutdown has not begun. The snapshot
        /// is read inside the `_pollerStopped` lock, and `stopMetricsPoller()` sets the flag under that lock
        /// before `close()`, so no tick overlaps `close()` and none records once shutdown begins. `_state` and
        /// `_pollerStopped` are never held together.
        func runMetricsPoller() async {
            guard self.configuration.metricsEnabled,
                let interval = self.configuration.metricsPollInterval,
                interval > .zero
            else { return }

            // Created on the first recording tick, so no gauge exists before the session connects.
            var gauges: SnapshotGauges?
            while !Task.isCancelled {
                do {
                    try await Task.sleep(for: interval)
                } catch {
                    return
                }
                let isConnected = self._state.withLockedValue { state in
                    if case .connected = state { return true }
                    return false
                }
                guard isConnected else { continue }
                self._pollerStopped.withLockedValue { stopped in
                    guard !stopped else { return }
                    let recorder = gauges ?? SnapshotGauges(sessionName: self.configuration.metricsSessionName)
                    recorder.record(self.getMetrics())
                    gauges = recorder
                }
            }
        }

        /// Stop the metrics poller before the session closes. Acquiring the lock waits for an in-flight tick.
        private func stopMetricsPoller() {
            self._pollerStopped.withLockedValue { $0 = true }
        }

        fileprivate struct ConnectionTask: Sendable {
            let task: Task<Void, Swift.Error>

            init(_ task: Task<Void, Swift.Error>) {
                self.task = task
            }
        }
    }
}

// MARK: - Queries

extension CassandraClient.Session {
    /// Execute insert / update / delete or DDL commands where no result is expected.
    func execute(
        _ command: String,
        parameters: [CassandraClient.Statement.Value] = [],
        options: CassandraClient.Statement.Options = .init(),
        logger: Logger
    ) async throws {
        _ = try await self.query(command, parameters: parameters, options: options, logger: logger)
    }

    /// Query small data-sets that fit into memory. Only use this when it's safe to buffer the entire data-set into memory.
    func query<T>(
        _ query: String,
        parameters: [CassandraClient.Statement.Value] = [],
        options: CassandraClient.Statement.Options = .init(),
        logger: Logger,
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
    func query<T: Decodable>(
        _ query: String,
        parameters: [CassandraClient.Statement.Value] = [],
        options: CassandraClient.Statement.Options = .init(),
        logger: Logger
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

    /// Query small data-sets that fit into memory, decoding each row into `model`.
    ///
    /// This is equivalent to the sibling `query(...)` overload that infers `T` purely from the return type,
    /// but spells out the decoded type explicitly at the call site, e.g.
    /// `try await session.query("select ...", withModelType: Model.self)`.
    func query<T: Decodable>(
        _ query: String,
        parameters: [CassandraClient.Statement.Value] = [],
        options: CassandraClient.Statement.Options = .init(),
        logger: Logger,
        withModelType model: T.Type
    ) async throws -> [T] {
        try await self.query(query, parameters: parameters, options: options, logger: logger)
    }

    /// Query large data-sets where using an interator helps control memory usage.
    ///
    /// - Important:
    ///   - Advancing the iterator invalidates values retrieved by the previous iteration.
    ///   - Attempting to wrap the ``CassandraClient/Rows`` sequence in a list will not work, use the transformer variant instead.
    func query(
        _ query: String,
        parameters: [CassandraClient.Statement.Value] = [],
        options: CassandraClient.Statement.Options = .init(),
        logger: Logger
    ) async throws -> CassandraClient.Rows {
        let statement: CassandraClient.Statement
        statement = try self.makeStatement(query: query, parameters: parameters, options: options)
        return try await self.execute(statement: statement, logger: logger)
    }

    /// Query large data-sets where the number of rows fetched at a time is limited by `pageSize`.
    ///
    /// A non-positive `pageSize` fails the call with ``CassandraClient/Error/badParams(_:)``.
    func query(
        _ query: String,
        parameters: sending [CassandraClient.Statement.Value] = [],
        pageSize: Int32,
        options: CassandraClient.Statement.Options = .init(),
        logger: Logger
    ) async throws -> CassandraClient.PaginatedRows {
        let statement: CassandraClient.Statement
        statement = try self.makeStatement(query: query, parameters: parameters, options: options)
        return try await self.execute(statement: statement, pageSize: pageSize, logger: logger)
    }

    /// Query large data-sets where the number of rows fetched at a time is limited by `pageSize`,
    /// decoding each row into `T` as the sequence is iterated, rather than materializing the whole
    /// decoded result set as an array.
    ///
    /// A non-positive `pageSize` fails the call with ``CassandraClient/Error/badParams(_:)``.
    ///
    /// - Note: Unlike the raw ``query(_:parameters:pageSize:options:logger:)`` sequence, decoded values
    ///   are independent Swift values and remain valid after the sequence is advanced.
    func query<T: Decodable & Sendable>(
        _ query: String,
        parameters: sending [CassandraClient.Statement.Value] = [],
        pageSize: Int32,
        options: CassandraClient.Statement.Options = .init(),
        logger: Logger
    ) async throws -> AsyncThrowingMapSequence<CassandraClient.PaginatedRows, T> {
        let paginatedRows = try await self.query(
            query,
            parameters: parameters,
            pageSize: pageSize,
            options: options,
            logger: logger
        )
        return paginatedRows.map { row in
            try T(from: self.makeDecoder(row: row, options: options))
        }
    }

    /// Query large data-sets where the number of rows fetched at a time is limited by `pageSize`,
    /// decoding each row into `model` as the sequence is iterated.
    ///
    /// This is equivalent to the sibling `query(...)` overload that infers `T` purely from the return type,
    /// but spells out the decoded type explicitly at the call site, e.g.
    /// `try await session.query("select ...", pageSize: 100, withModelType: Model.self)`.
    ///
    /// A non-positive `pageSize` fails the call with ``CassandraClient/Error/badParams(_:)``.
    func query<T: Decodable & Sendable>(
        _ query: String,
        parameters: sending [CassandraClient.Statement.Value] = [],
        pageSize: Int32,
        options: CassandraClient.Statement.Options = .init(),
        logger: Logger,
        withModelType model: T.Type
    ) async throws -> AsyncThrowingMapSequence<CassandraClient.PaginatedRows, T> {
        try await self.query(
            query,
            parameters: parameters,
            pageSize: pageSize,
            options: options,
            logger: logger
        )
    }

    /// Execute a prepared statement and decode each row into a `Decodable` type.
    func execute<T: Decodable>(
        prepared: CassandraClient.PreparedStatement,
        parameters: [CassandraClient.Statement.Value] = [],
        options: CassandraClient.Statement.Options = .init(),
        logger: Logger
    ) async throws -> [T] {
        var effectiveOptions = options
        if effectiveOptions.encryptionTable == nil {
            effectiveOptions.encryptionTable = prepared.encryptionTable
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

    /// Execute a prepared statement, decoding each row into `model`.
    ///
    /// This is equivalent to the sibling `execute(...)` overload that infers `T` purely from the return type,
    /// but spells out the decoded type explicitly at the call site, e.g.
    /// `try await session.execute(prepared: statement, withModelType: Model.self)`.
    func execute<T: Decodable>(
        prepared: CassandraClient.PreparedStatement,
        parameters: [CassandraClient.Statement.Value] = [],
        options: CassandraClient.Statement.Options = .init(),
        logger: Logger,
        withModelType model: T.Type
    ) async throws -> [T] {
        try await self.execute(
            prepared: prepared,
            parameters: parameters,
            options: options,
            logger: logger
        )
    }
}

extension CassandraClient.Session {
    /// What `withConnection should do after inspecting the connection state
    private enum AsyncConnectionAction {
        /// We started the connection; await it, then mark the session connected.
        case startedConnecting(ConnectionTask)
        /// Someone else started the connection as a task; just await it.
        case awaitConnecting(ConnectionTask)
        /// Already connected.
        case ready
        /// Session has been shut down.
        case disconnected
    }

    /// Ensure the session is connected, then invoke `body`.
    private func withConnection<T>(
        logger: Logger,
        _ body: (Logger) async throws -> T
    ) async throws -> T {
        let action: AsyncConnectionAction = self._state.withLockedValue { state in
            switch state {
            case .idle:
                let connectionTask = ConnectionTask(self.connect(logger: logger))
                state = .connecting(connectionTask)
                return .startedConnecting(connectionTask)
            case .connecting(let task):
                return .awaitConnecting(task)
            case .connected:
                return .ready
            case .disconnected, .disconnecting, .disconnectingFuture:
                return .disconnected
            }
        }

        switch action {
        case .startedConnecting(let task):
            try await task.task.value
            try self.handleConnectionSucceeded()
        case .awaitConnecting(let task):
            try await task.task.value
        case .ready:
            break
        case .disconnected:
            throw CassandraClient.Error.disconnected
        }
        return try await body(logger)
    }

    // `statement` need not be `sending`: it's non-Sendable, so the compiler already blocks running two
    // executes over one statement concurrently, and this preserves the safe execute-await-reuse pattern.
    func execute(
        statement: CassandraClient.Statement,
        logger: Logger
    ) async throws
        -> CassandraClient.Rows
    {
        try await self.withConnection(logger: logger) { logger in
            logger.debug("executing: \(statement.query)")
            if self.configuration.logBoundValues {
                logger.trace("\(statement.parameters)")
            }
            let query = statement.query
            let consistency = statement.options.consistency ?? self.configuration.consistency
            let boundValues =
                self.configuration.logBoundValues
                ? CassandraClient.RequestLog.formatValues(statement.parameters) : nil
            let startedAt = DispatchTime.now()
            return try await CassandraClient.RequestTrace.traced(
                .execute,
                query: query,
                consistency: consistency,
                keyspace: self.configuration.keyspace
            ) {
                try await CassandraClient.RequestLog.instrumented(
                    startedAt: startedAt,
                    query: query,
                    consistency: consistency,
                    threshold: self.configuration.slowQueryThreshold,
                    boundValues: boundValues,
                    logger: logger
                ) {
                    try await self.underlying.execute(statement: statement).await()
                }
            }
        }
    }

    func execute(
        statement: sending CassandraClient.Statement,
        pageSize: Int32,
        logger: Logger
    ) async throws -> CassandraClient.PaginatedRows {
        do {
            try statement.setPagingSize(Int(pageSize))
        } catch {
            if let cassError = error as? CassandraClient.Error {
                CassandraClient.RequestLog.logFailure(
                    cassError,
                    query: statement.query,
                    consistency: nil,
                    startedAt: nil,
                    logger: logger
                )
            }
            throw error
        }
        return CassandraClient.PaginatedRows(session: self, statement: statement, logger: logger)
    }

    func execute(
        batch: consuming CassandraClient.Batch,
        logger: Logger
    ) async throws {
        // Use optionalBatch to prove to compiler that we only take it once
        var optionalBatch: CassandraClient.Batch? = batch
        try await self.withConnection(logger: logger) { logger in
            logger.debug("executing batch")
            let startedAt = DispatchTime.now()
            try await CassandraClient.RequestTrace.traced(
                .batch,
                query: "batch",
                consistency: nil,
                keyspace: self.configuration.keyspace
            ) {
                try await CassandraClient.RequestLog.instrumented(
                    startedAt: startedAt,
                    query: "batch",
                    consistency: nil,
                    threshold: self.configuration.slowQueryThreshold,
                    logger: logger
                ) {
                    try await self.underlying.execute(batch: optionalBatch.take()!).await()
                }
            }
        }
    }

    /// Execute a batch of statements.
    ///
    /// - Parameters:
    ///   - configuration: Options to apply to the batch.
    ///   - logger: If `nil`, the client's default `Logger` is used.
    ///   - build: Closure that adds statements to the batch.
    func batch(
        configuration: CassandraClient.Batch.Configuration = .init(),
        logger: Logger,
        _ build: (inout CassandraClient.Batch) async throws -> Void
    ) async throws {
        let resolver:
            (
                (
                    CassandraClient.PreparedStatement, [CassandraClient.Statement.Value],
                    CassandraClient.Statement.Options
                ) throws -> CassandraClient.Statement
            )?
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
                query: prepared.query,
                parameters: resolvedParameters,
                options: options,
                encryptor: self.encryptor
            )
        }
        let batch: CassandraClient.Batch
        do {
            var built = try CassandraClient.Batch(configuration: configuration, resolver: resolver)
            try await build(&built)
            batch = built
        } catch {
            if let cassError = error as? CassandraClient.Error {
                CassandraClient.RequestLog.logFailure(
                    cassError,
                    query: "batch",
                    consistency: nil,
                    startedAt: nil,
                    logger: logger
                )
            }
            throw error
        }
        try await self.execute(batch: batch, logger: logger)
    }

    func prepare(
        _ query: String,
        encryptionTable: String? = nil,
        logger: Logger
    ) async throws -> CassandraClient.PreparedStatement {
        let prepared: CassPrepared = try await self.withConnection(logger: logger) { logger in
            logger.debug("preparing: \(query)")
            let startedAt = DispatchTime.now()
            return try await CassandraClient.RequestTrace.traced(
                .prepare,
                query: query,
                consistency: nil,
                keyspace: self.configuration.keyspace
            ) {
                try await CassandraClient.RequestLog.instrumented(
                    startedAt: startedAt,
                    query: query,
                    consistency: nil,
                    threshold: self.configuration.slowQueryThreshold,
                    logger: logger
                ) {
                    try await self.underlying.prepare(query: query).await()
                }
            }
        }
        let pkColumns: [String]
        if let tableName = encryptionTable {
            do {
                pkColumns = try self.lookupPrimaryKeyColumnNames(tableName: tableName)
            } catch {
                if let cassError = error as? CassandraClient.Error {
                    CassandraClient.RequestLog.logFailure(
                        cassError,
                        query: query,
                        consistency: nil,
                        startedAt: nil,
                        logger: logger
                    )
                }
                throw error
            }
        } else {
            pkColumns = []
        }
        return CassandraClient.PreparedStatement(
            rawPointer: prepared,
            query: query,
            encryptionTable: encryptionTable,
            primaryKeyColumnNames: pkColumns
        )
    }

    func execute(
        prepared: CassandraClient.PreparedStatement,
        parameters: [CassandraClient.Statement.Value] = [],
        options: CassandraClient.Statement.Options = .init(),
        logger: Logger
    ) async throws -> CassandraClient.Rows {
        let statement: CassandraClient.Statement
        do {
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
                query: prepared.query,
                parameters: resolvedParameters,
                options: options,
                encryptor: self.encryptor
            )
        } catch {
            if let cassError = error as? CassandraClient.Error {
                CassandraClient.RequestLog.logFailure(
                    cassError,
                    query: prepared.query,
                    consistency: nil,
                    startedAt: nil,
                    logger: logger
                )
            }
            throw error
        }
        return try await self.execute(statement: statement, logger: logger)
    }

    private func connect(logger: Logger) -> Task<Void, Swift.Error> {
        Task {
            logger.debug("connecting to: \(self.configuration)")
            if let warning = self.configuration.insecureSSLWarning {
                logger.warning("\(warning)")
            }
            let startedAt = DispatchTime.now()
            // Instrument makeCluster + connect together so cluster-build failures are logged too.
            return try await CassandraClient.RequestLog.instrumented(
                startedAt: startedAt,
                query: nil,
                consistency: nil,
                threshold: nil,
                logger: logger
            ) {
                let cluster = try await self.configuration.makeCluster()
                return try await self.underlying.connect(cluster: cluster, keyspace: self.configuration.keyspace)
                    .await()
            }
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
