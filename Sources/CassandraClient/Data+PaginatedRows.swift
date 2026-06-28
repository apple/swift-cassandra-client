//===----------------------------------------------------------------------===//
//
// This source file is part of the Swift Cassandra Client open source project
//
// Copyright (c) 2023 Apple Inc. and the Swift Cassandra Client project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of Swift Cassandra Client project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

internal import CDataStaxDriver
import Logging
import NIO
import NIOConcurrencyHelpers

extension CassandraClient {
    /// Resulting row(s) of a Cassandra query. Data are paginated.
    public final class PaginatedRows: Sendable {
        let session: Session
        /// Not `Sendable` (wraps a non-thread-safe `CassStatement*`), but safe here: the `fetchInFlight`
        /// guard serializes all access from `PaginatedRows`, and the entry points take the `Statement`
        /// as `sending` so the caller can't alias it.
        nonisolated(unsafe) let statement: Statement
        let eventLoop: EventLoop?
        let logger: Logger?

        /// Lock-protected paging state. `fetchInFlight` serializes `nextPage()` so only one fetch
        /// touches the statement at a time; `hasMorePages` is read via the getter / `AsyncSequence` loop.
        private struct PagingState {
            var hasMorePages = true
            var fetchInFlight = false
        }
        private let _state = NIOLockedValueBox(PagingState())

        /// If `true`, calling ``nextPage()-4komz`` will return a new set of ``CassandraClient/Rows``.
        /// Otherwise it will throw ``CassandraClient/Error/rowsExhausted`` error.
        public var hasMorePages: Bool { self._state.withLockedValue { $0.hasMorePages } }

        internal init(session: Session, statement: sending Statement, on eventLoop: EventLoop, logger: Logger?) {
            self.session = session
            self.statement = statement
            self.eventLoop = eventLoop
            self.logger = logger
        }

        /// Fetches next page of rows or throw ``CassandraClient/Error/rowsExhausted`` error if there are no more pages.
        public func nextPage() -> EventLoopFuture<Rows> {
            guard let eventLoop = self.eventLoop else {
                preconditionFailure("EventLoop must not be nil")
            }

            // Claim the in-flight slot atomically with the has-more-pages check (rejecting overlap).
            let rejection = self._state.withLockedValue { state -> CassandraClient.Error? in
                guard state.hasMorePages else { return .rowsExhausted }
                guard !state.fetchInFlight else { return .concurrentPaginationUnsupported }
                state.fetchInFlight = true
                return nil
            }
            if let rejection {
                return eventLoop.makeFailedFuture(rejection)
            }

            let future = self.session.execute(
                statement: self.statement,
                on: eventLoop,
                logger: self.logger
            )
            future.whenComplete { result in
                // Flag write + C paging-state mutation + release of the in-flight slot, all under the lock.
                self._state.withLockedValue { state in
                    switch result {
                    case .success(let rows):
                        state.hasMorePages = cass_result_has_more_pages(rows.rawPointer) == cass_true
                        if state.hasMorePages {
                            cass_statement_set_paging_state(self.statement.rawPointer, rows.rawPointer)
                        }
                    case .failure:
                        state.hasMorePages = false
                    }
                    state.fetchInFlight = false
                }
            }
            return future
        }

        /// Iterates through all rows in all pages and invokes the given closure on each.
        @available(*, deprecated, message: "Use Swift Concurrency and AsyncSequence APIs instead.")
        public func forEach(_ body: @escaping @Sendable (Row) throws -> Void) -> EventLoopFuture<Void> {
            precondition(
                self.hasMorePages,
                "Only one of 'forEach' or 'map' can be called once per PaginatedRows"
            )

            guard let eventLoop = self.eventLoop else {
                preconditionFailure("EventLoop must not be nil")
            }

            @Sendable func _forEach() -> EventLoopFuture<Void> {
                self.nextPage().flatMap { rows in
                    do {
                        try rows.forEach(body)
                    } catch {
                        return eventLoop.makeFailedFuture(error)
                    }

                    guard self.hasMorePages else {
                        return eventLoop.makeSucceededFuture(())
                    }
                    return _forEach()
                }
            }
            return _forEach()
        }

        /// Iterates through all rows in all pages and applies `transform` on each.
        @available(*, deprecated, message: "Use Swift Concurrency and AsyncSequence APIs instead.")
        public func map<T: Sendable>(_ transform: @escaping @Sendable (Row) throws -> T) -> EventLoopFuture<[T]> {
            precondition(
                self.hasMorePages,
                "Only one of 'forEach' or 'map' can be called once per PaginatedRows"
            )

            guard let eventLoop = self.eventLoop else {
                preconditionFailure("EventLoop must not be nil in EventLoop based APIs")
            }

            @Sendable func _map(_ accumulated: [T]) -> EventLoopFuture<[T]> {
                self.nextPage().flatMap { rows in
                    do {
                        let transformed = try rows.map(transform)
                        let newAccumulated = accumulated + transformed
                        guard self.hasMorePages else {
                            return eventLoop.makeSucceededFuture(newAccumulated)
                        }
                        return _map(newAccumulated)
                    } catch {
                        return eventLoop.makeFailedFuture(error)
                    }
                }
            }
            return _map([])
        }

        @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
        internal init(session: Session, statement: sending Statement, logger: Logger?) {
            self.session = session
            self.statement = statement
            self.eventLoop = nil
            self.logger = logger
        }

        /// Fetches next page of rows or throw ``CassandraClient/Error/rowsExhausted`` error if there are no more pages.
        @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
        public func nextPage() async throws -> Rows {
            // Claim the in-flight slot atomically with the has-more-pages check (see EL variant).
            try self._state.withLockedValue { state in
                guard state.hasMorePages else { throw CassandraClient.Error.rowsExhausted }
                guard !state.fetchInFlight else { throw CassandraClient.Error.concurrentPaginationUnsupported }
                state.fetchInFlight = true
            }

            do {
                // The await runs outside the lock; only the state writes are locked.
                let rows = try await session.execute(statement: self.statement, logger: self.logger)
                self._state.withLockedValue { state in
                    state.hasMorePages = cass_result_has_more_pages(rows.rawPointer) == cass_true
                    if state.hasMorePages {
                        cass_statement_set_paging_state(self.statement.rawPointer, rows.rawPointer)
                    }
                    state.fetchInFlight = false
                }
                return rows
            } catch {
                self._state.withLockedValue { state in
                    state.hasMorePages = false
                    state.fetchInFlight = false
                }
                throw error
            }
        }

        /// Iterates through all rows in all pages and invokes the given closure on each.
        @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
        @available(*, deprecated, message: "Use AsyncSequence APIs instead.")
        public func forEach(_ body: @escaping (Row) throws -> Void) async throws {
            precondition(
                self.hasMorePages,
                "Only one of 'forEach' or 'map' can be called once per PaginatedRows"
            )

            func _forEach() async throws {
                let rows = try await nextPage()
                try rows.forEach(body)

                guard self.hasMorePages else {
                    return
                }
                try await _forEach()
            }
            try await _forEach()
        }

        /// Iterates through all rows in all pages and applies `transform` on each.
        @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
        @available(*, deprecated, message: "Use AsyncSequence APIs instead.")
        public func map<T>(_ transform: @escaping (Row) throws -> T) async throws -> [T] {
            precondition(
                self.hasMorePages,
                "Only one of 'forEach' or 'map' can be called once per PaginatedRows"
            )

            func _map(_ accumulated: [T]) async throws -> [T] {
                let rows = try await nextPage()
                let transformed = try rows.map(transform)
                let newAccumulated = accumulated + transformed

                guard self.hasMorePages else {
                    return newAccumulated
                }
                return try await _map(newAccumulated)
            }
            return try await _map([])
        }
    }
}

@available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
extension CassandraClient.PaginatedRows: AsyncSequence {
    public typealias Element = CassandraClient.Row

    /// Make async iterator for the ``PaginatedRows``.
    ///
    /// - Warning:
    ///   This can be called only once for each ``PaginatedRows``,
    ///   Otherwise it will throw ``CassandraClient/Error/rowsExhausted`` error.
    public func makeAsyncIterator() -> AsyncIterator {
        AsyncIterator(self)
    }

    public struct AsyncIterator: AsyncIteratorProtocol {
        private var pageStream: AsyncThrowingStream<CassandraClient.Rows, Error>.Iterator

        private var currentPage: CassandraClient.Rows.Iterator?

        init(_ paginatedRows: CassandraClient.PaginatedRows) {
            // FIXME: it would be better if this were a stream of `Row` instead of `Rows`,
            // but that implementation generates in incorrect results--a later row seems
            // to overwrite previous rows so that the iterated rows differ from those
            // yielded to continuation.
            self.pageStream = AsyncThrowingStream { continuation in
                Task {
                    do {
                        repeat {
                            // This shouldn't throw `rowExhausted` error, but if it does
                            // it means the `PaginatedRows` has already been iterated
                            // through, which indicates usage error so it's ok to throw.
                            let rows = try await paginatedRows.nextPage()
                            continuation.yield(rows)
                        } while paginatedRows.hasMorePages

                        continuation.finish()
                    } catch {
                        continuation.finish(throwing: error)
                    }
                }
            }.makeAsyncIterator()
        }

        public mutating func next() async throws -> CassandraClient.Row? {
            // Initialize current page
            if self.currentPage == nil {
                guard let nextPage = try await self.pageStream.next()?.makeIterator() else {
                    return nil
                }
                self.currentPage = nextPage
            }

            guard let nextRow = self.currentPage?.next() else {
                // Current page has no more element
                self.currentPage = nil
                return try await self.next()
            }

            return nextRow
        }
    }
}
