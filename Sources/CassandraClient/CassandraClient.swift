@_implementationOnly import CDataStaxDriver
import Logging
import NIO
import NIOConcurrencyHelpers

public class CassandraClient: CassandraSession {
    private let eventLoopGroupContainer: EventLoopGroupConainer
    public var eventLoopGroup: EventLoopGroup {
        self.eventLoopGroupContainer.value
    }

    private let configuration: Configuration
    private let logger: Logger
    private let defaultSession: Session
    private let isShutdown = NIOAtomic<Bool>.makeAtomic(value: false)

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
        precondition(self.isShutdown.load(), "Client not shut down before the deinit. Please call client.shutdown() when no longer needed.")
    }

    public func shutdown() throws {
        if !self.isShutdown.compareAndExchange(expected: false, desired: true) {
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

    public func execute(statement: Statement, on eventLoop: EventLoop?, logger: Logger? = nil) -> EventLoopFuture<Rows> {
        self.defaultSession.execute(statement: statement, on: eventLoop, logger: logger)
    }

    public func execute(statement: Statement, pageSize: Int32, on eventLoop: EventLoop?, logger: Logger? = nil) -> EventLoopFuture<PaginatedRows> {
        self.defaultSession.execute(statement: statement, pageSize: pageSize, on: eventLoop, logger: logger)
    }

    public func makeSession(keyspace: String?, logger: Logger? = nil) -> CassandraSession {
        var configuration = self.configuration
        configuration.keyspace = keyspace
        let logger = logger ?? self.logger
        return Session(configuration: configuration, logger: logger, eventLoopGroupContainer: self.eventLoopGroupContainer)
    }

    public func withSession(keyspace: String?, logger: Logger? = nil, handler: (CassandraSession) throws -> Void) rethrows {
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

    public func withSession<T>(keyspace: String?, logger: Logger? = nil, handler: (CassandraSession) -> EventLoopFuture<T>) -> EventLoopFuture<T> {
        let session = self.makeSession(keyspace: keyspace, logger: logger)
        return handler(session).always { _ in
            do {
                try session.shutdown()
            } catch {
                self.logger.warning("shutdown error: \(error)")
            }
        }
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
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    func execute(statement: Statement, logger: Logger? = nil) async throws -> Rows {
        try await self.defaultSession.execute(statement: statement, logger: logger)
    }

    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    func execute(statement: Statement, pageSize: Int32, logger: Logger? = nil) async throws -> PaginatedRows {
        try await self.defaultSession.execute(statement: statement, pageSize: pageSize, logger: logger)
    }

    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    func withSession(keyspace: String?, logger: Logger? = nil, handler: (CassandraSession) async throws -> Void) async throws {
        let session = self.makeSession(keyspace: keyspace, logger: logger)
        defer {
            do {
                try session.shutdown()
            } catch {
                self.logger.warning("shutdown error: \(error)")
            }
        }
        try await handler(session)
    }

    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    func withSession<T>(keyspace: String?, logger: Logger? = nil, handler: (CassandraSession) async throws -> T) async throws -> T {
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
