@_implementationOnly import CDataStaxDriver
import Dispatch
import Logging
import NIO
import NIOConcurrencyHelpers
import NIOCore // for async-await bridge

public protocol CassandraSession {
    var eventLoopGroup: EventLoopGroup { get }

    /// Runs prepared statement
    func execute(statement: CassandraClient.Statement, on eventLoop: EventLoop?, logger: Logger?) -> EventLoopFuture<CassandraClient.Rows>

    /// Runs prepared statement and returns paginated result
    func execute(statement: CassandraClient.Statement, pageSize: Int32, on eventLoop: EventLoop?, logger: Logger?) -> EventLoopFuture<CassandraClient.PaginatedRows>

    #if compiler(>=5.5) && canImport(_Concurrency)
    /// Runs prepared statement
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    func execute(statement: CassandraClient.Statement, logger: Logger?) async throws -> CassandraClient.Rows

    /// Runs prepared statement and returns paginated result
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    func execute(statement: CassandraClient.Statement, pageSize: Int32, logger: Logger?) async throws -> CassandraClient.PaginatedRows
    #endif

    /// Frees resources
    func shutdown() throws
}

internal extension CassandraSession {
    /// Run prepared statement
    func execute(statement: CassandraClient.Statement, logger: Logger? = nil) -> EventLoopFuture<CassandraClient.Rows> {
        self.execute(statement: statement, on: nil, logger: logger)
    }
}

public extension CassandraSession {
    /// Run insert / update / detete  or DDL commands where no result is expected.
    ///
    /// If `eventLoop` is `nil`, a new one will get created through the `EventLoopGroup` provided during initialization.
    func run(_ command: String,
             parameters: [CassandraClient.Statement.Value] = [],
             options: CassandraClient.Statement.Options = .init(),
             on eventLoop: EventLoop? = nil,
             logger: Logger? = nil) -> EventLoopFuture<Void> {
        self.query(command, parameters: parameters, options: options, on: eventLoop, logger: logger).map { _ in () }
    }

    /// Query small data-sets that fit into memory. Only use this when its safe to buffer the entire data-set into memory.
    ///
    /// If `eventLoop` is `nil`, a new one will get created through the `EventLoopGroup` provided during initialization.
    func query<T>(_ query: String,
                  parameters: [CassandraClient.Statement.Value] = [],
                  options: CassandraClient.Statement.Options = .init(),
                  on eventLoop: EventLoop? = nil,
                  logger: Logger? = nil,
                  transform: @escaping (CassandraClient.Row) -> T?) -> EventLoopFuture<[T]> {
        self.query(query, parameters: parameters, options: options, on: eventLoop, logger: logger).map { rows in
            rows.compactMap(transform)
        }
    }

    /// Query small data-sets that fit into memory. Only use this when its safe to buffer the entire data-set into memory.
    ///
    /// If `eventLoop` is `nil`, a new one will get created through the `EventLoopGroup` provided during initialization.
    func query<T: Decodable>(_ query: String,
                             parameters: [CassandraClient.Statement.Value] = [],
                             options: CassandraClient.Statement.Options = .init(),
                             on eventLoop: EventLoop? = nil,
                             logger: Logger? = nil) -> EventLoopFuture<[T]> {
        self.query(query, parameters: parameters, options: options, on: eventLoop, logger: logger).flatMapThrowing { rows in
            try rows.map { row in
                try T(from: CassandraClient.RowDecoder(row: row))
            }
        }
    }

    /// Query large data-sets where using an interator helps control memory usage.
    ///
    /// If `eventLoop` is `nil`, a new one will get created through the `EventLoopGroup` provided during initialization.
    ///
    /// - Important:
    ///   - Advancing the iterator invalidates values retrieved by the previous iteration.
    ///   - Attempting to wrap the `Rows` sequence in an `Array` will not work, use the transformer variant instead.
    func query(_ query: String,
               parameters: [CassandraClient.Statement.Value] = [],
               options: CassandraClient.Statement.Options = .init(),
               on eventLoop: EventLoop? = nil,
               logger: Logger? = nil) -> EventLoopFuture<CassandraClient.Rows> {
        do {
            let statement = try CassandraClient.Statement(query: query, parameters: parameters, options: options)
            return self.execute(statement: statement, on: eventLoop, logger: logger)
        } catch {
            let eventLoop = eventLoop ?? self.eventLoopGroup.next()
            return eventLoop.makeFailedFuture(error)
        }
    }

    /// Query large data-sets where the number of rows fetch at a time is limited by `pageSize`.
    ///
    /// If `eventLoop` is `nil`, a new one will get created through the `EventLoopGroup` provided during initialization.
    func query(_ query: String,
               parameters: [CassandraClient.Statement.Value] = [],
               pageSize: Int32,
               options: CassandraClient.Statement.Options = .init(),
               on eventLoop: EventLoop? = nil,
               logger: Logger? = nil) -> EventLoopFuture<CassandraClient.PaginatedRows> {
        do {
            let statement = try CassandraClient.Statement(query: query, parameters: parameters, options: options)
            return self.execute(statement: statement, pageSize: pageSize, on: eventLoop, logger: logger)
        } catch {
            let eventLoop = eventLoop ?? self.eventLoopGroup.next()
            return eventLoop.makeFailedFuture(error)
        }
    }
}

internal extension CassandraClient {
    final class Session: CassandraSession {
        private let eventLoopGroupContainer: EventLoopGroupConainer
        public var eventLoopGroup: EventLoopGroup {
            self.eventLoopGroupContainer.value
        }

        private let configuration: Configuration
        private let logger: Logger
        private var state = State.idle
        private let lock = Lock()

        private let rawPointer: OpaquePointer

        private enum State {
            case idle
            case connectingFuture(EventLoopFuture<Void>)
            #if compiler(>=5.5) && canImport(_Concurrency)
            case connecting(ConnectionTask)
            #endif
            case connected
            case disconnected
        }

        internal init(configuration: Configuration, logger: Logger, eventLoopGroupContainer: EventLoopGroupConainer) {
            self.configuration = configuration
            self.logger = logger
            self.eventLoopGroupContainer = eventLoopGroupContainer
            self.rawPointer = cass_session_new()
        }

        deinit {
            guard case .disconnected = (self.lock.withLock { self.state }) else {
                preconditionFailure("Session not shut down before the deinit. Please call session.shutdown() when no longer needed.")
            }
            cass_session_free(self.rawPointer)
        }

        public func shutdown() throws {
            self.lock.lock()
            defer {
                self.state = .disconnected
                self.lock.unlock()
            }
            switch self.state {
            case .connected:
                try self.disconect()
            default:
                break
            }
        }

        public func execute(statement: Statement, on eventLoop: EventLoop?, logger: Logger? = nil) -> EventLoopFuture<Rows> {
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
            #if compiler(>=5.5) && canImport(_Concurrency)
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
            #endif
            case .connected:
                self.lock.unlock()
                logger.debug("executing: \(statement.query)")
                logger.trace("\(statement.parameters)")
                let promise = eventLoop.makePromise(of: Rows.self)
                let future = cass_session_execute(self.rawPointer, statement.rawPointer)
                futureSetResultCallback(future!) { result in
                    promise.completeWith(result)
                }
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

        public func execute(statement: Statement, pageSize: Int32, on eventLoop: EventLoop?, logger: Logger? = nil) -> EventLoopFuture<CassandraClient.PaginatedRows> {
            let eventLoop = eventLoop ?? self.eventLoopGroup.next()

            do {
                try statement.setPagingSize(pageSize)
            } catch {
                return eventLoop.makeFailedFuture(error)
            }

            return eventLoop.makeSucceededFuture(PaginatedRows(session: self, statement: statement, on: eventLoop, logger: logger))
        }

        private func connect(on eventLoop: EventLoop, logger: Logger) -> EventLoopFuture<Void> {
            logger.debug("connecting to: \(self.configuration)")

            return self.configuration.makeCluster(on: eventLoop)
                .flatMap { cluster in
                    let promise = eventLoop.makePromise(of: Void.self)

                    let future: OpaquePointer
                    if let keyspace = self.configuration.keyspace {
                        future = cass_session_connect_keyspace(self.rawPointer, cluster.rawPointer, keyspace)
                    } else {
                        future = cass_session_connect(self.rawPointer, cluster.rawPointer)
                    }

                    futureSetCallback(future) { result in
                        promise.completeWith(result)
                    }

                    return promise.futureResult
                }
        }

        private func disconect() throws {
            var error: Swift.Error?
            let semaphore = DispatchSemaphore(value: 0)
            let future = cass_session_close(self.rawPointer)
            futureSetCallback(future!) { result in
                defer { semaphore.signal() }
                if case .failure(let e) = result {
                    error = e
                }
            }
            semaphore.wait()
            if let error = error {
                throw error
            }
        }
        
        #if compiler(>=5.5) && canImport(_Concurrency)
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
        #endif
    }
}

// MARK: - Cassandra session with async-await support

#if compiler(>=5.5) && canImport(_Concurrency)
public extension CassandraSession {
    /// Run  insert / update / detete  or DDL commands where no result is expected
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    func run(_ command: String,
             parameters: [CassandraClient.Statement.Value] = [],
             options: CassandraClient.Statement.Options = .init(),
             logger: Logger? = nil) async throws {
        _ = try await self.query(command, parameters: parameters, options: options, logger: logger)
    }

    /// Query small data-sets that fit into memory. Only use this when its safe to buffer the entire data-set into memory.
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    func query<T>(_ query: String,
                  parameters: [CassandraClient.Statement.Value] = [],
                  options: CassandraClient.Statement.Options = .init(),
                  logger: Logger? = nil,
                  transform: @escaping (CassandraClient.Row) -> T?) async throws -> [T] {
        let rows = try await self.query(query, parameters: parameters, options: options, logger: logger)
        return rows.compactMap(transform)
    }

    /// Query small data-sets that fit into memory. Only use this when its safe to buffer the entire data-set into memory.
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    func query<T: Decodable>(_ query: String,
                             parameters: [CassandraClient.Statement.Value] = [],
                             options: CassandraClient.Statement.Options = .init(),
                             logger: Logger? = nil) async throws -> [T] {
        let rows = try await self.query(query, parameters: parameters, options: options, logger: logger)
        return try rows.map { row in
            try T(from: CassandraClient.RowDecoder(row: row))
        }
    }

    /// Query large data-sets where using an interator helps control memory usage.
    /// Important Note:  Advancing the iterator invalidates values retrieved by the previous iteration.
    /// Attempting to wrap the `Rows` sequence in an `Array` will not work, use the transformer variant instead.
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    func query(_ query: String,
               parameters: [CassandraClient.Statement.Value] = [],
               options: CassandraClient.Statement.Options = .init(),
               logger: Logger? = nil) async throws -> CassandraClient.Rows {
        let statement = try CassandraClient.Statement(query: query, parameters: parameters, options: options)
        return try await self.execute(statement: statement, logger: logger)
    }

    /// Query large data-sets where the number of rows fetch at a time is limited by `pageSize`.
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    func query(_ query: String,
               parameters: [CassandraClient.Statement.Value] = [],
               pageSize: Int32,
               options: CassandraClient.Statement.Options = .init(),
               logger: Logger? = nil) async throws -> CassandraClient.PaginatedRows {
        let statement = try CassandraClient.Statement(query: query, parameters: parameters, options: options)
        return try await self.execute(statement: statement, pageSize: pageSize, logger: logger)
    }
}

extension CassandraClient.Session {
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    public func execute(statement: CassandraClient.Statement, logger: Logger? = nil) async throws -> CassandraClient.Rows {
        let logger = logger ?? self.logger
        
        self.lock.lock()
        switch self.state {
        case .idle:
            let task = self.connect(logger: logger)
            self.state = .connecting(ConnectionTask(task))
            self.lock.unlock()

            try await task.value
            self.lock.withLock {
                self.state = .connected
            }
            return try await self.execute(statement: statement, logger: logger)
        case .connectingFuture(let future):
            self.lock.unlock()
            try await future.get()
            return try await self.execute(statement: statement, logger: logger)
        case .connecting(let task):
            self.lock.unlock()
            try await task.task.value
            return try await self.execute(statement: statement, logger: logger)
        case .connected:
            self.lock.unlock()
            logger.debug("executing: \(statement.query)")
            logger.trace("\(statement.parameters)")
            let future = cass_session_execute(self.rawPointer, statement.rawPointer)
            return try await withCheckedThrowingContinuation { continuation in
                futureSetResultCallback(future!) { result in
                    continuation.resume(with: result)
                }
            }
        case .disconnected:
            self.lock.unlock()
            if self.eventLoopGroupContainer.managed {
                // eventloop *is* shutdown now
                preconditionFailure("client is disconnected")
            }
            throw CassandraClient.Error.disconnected
        }
    }

    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    public func execute(statement: CassandraClient.Statement, pageSize: Int32, logger: Logger? = nil) async throws -> CassandraClient.PaginatedRows {
        try statement.setPagingSize(pageSize)
        return CassandraClient.PaginatedRows(session: self, statement: statement, logger: logger)
    }

    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    private func connect(logger: Logger) -> Task<Void, Swift.Error> {
        Task {
            logger.debug("connecting to: \(self.configuration)")

            let cluster = try await self.configuration.makeCluster()

            let future: OpaquePointer
            if let keyspace = self.configuration.keyspace {
                future = cass_session_connect_keyspace(self.rawPointer, cluster.rawPointer, keyspace)
            } else {
                future = cass_session_connect(self.rawPointer, cluster.rawPointer)
            }

            try await withCheckedThrowingContinuation { continuation in
                futureSetCallback(future) { result in
                    continuation.resume(with: result)
                }
            }
        }
    }
}
#endif

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

private func futureSetCallback(_ future: OpaquePointer, completion: @escaping (Result<Void, Error>) -> Void) {
    let closure = unmanagedRetainedClosure {
        DispatchQueue.global().async {
            let resultCode = cass_future_error_code(future)
            let result: Result<Void, Error> = resultCode == CASS_OK ?
                .success(()) :
                .failure(CassandraClient.Error(future))
            cass_future_free(future)
            completion(result)
        }
    }
    cass_future_set_callback(future, { _, data in callAndReleaseUnmanagedClosure(data!) }, closure)
}

private func futureSetResultCallback(_ future: OpaquePointer, completion: @escaping (Result<CassandraClient.Rows, Error>) -> Void) {
    let closure = unmanagedRetainedClosure {
        DispatchQueue.global().async {
            let resultCode = cass_future_error_code(future)
            let result: Result<CassandraClient.Rows, Error> = resultCode == CASS_OK ?
                .success(CassandraClient.Rows(future)) :
                .failure(CassandraClient.Error(future))
            cass_future_free(future)
            completion(result)
        }
    }
    cass_future_set_callback(future, { _, data in callAndReleaseUnmanagedClosure(data!) }, closure)
}

private final class Box<T> {
    public let value: T

    public init(_ value: T) {
        self.value = value
    }
}
