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

@_implementationOnly import CDataStaxDriver
import Logging
import NIO

extension CassandraClient {
  /// Resulting row(s) of a Cassandra query. Data are paginated.
  public final class PaginatedRows {
    let session: Session
    let statement: Statement
    let eventLoop: EventLoop?
    let logger: Logger?

    /// If `true`, calling ``nextPage()-4komz`` will return a new set of ``CassandraClient/Rows``.
    /// Otherwise it will throw ``CassandraClient/Error/rowsExhausted`` error.
    public private(set) var hasMorePages: Bool = true

    internal init(session: Session, statement: Statement, on eventLoop: EventLoop, logger: Logger?)
    {
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
      guard self.hasMorePages else {
        return eventLoop.makeFailedFuture(CassandraClient.Error.rowsExhausted)
      }

      let future = self.session.execute(
        statement: self.statement, on: eventLoop, logger: self.logger)
      future.whenComplete { result in
        switch result {
        case .success(let rows):
          self.hasMorePages = cass_result_has_more_pages(rows.rawPointer) == cass_true
          if self.hasMorePages {
            cass_statement_set_paging_state(self.statement.rawPointer, rows.rawPointer)
          }
        case .failure:
          self.hasMorePages = false
        }
      }
      return future
    }

    /// Iterates through all rows in all pages and invokes the given closure on each.
    @available(*, deprecated, message: "Use Swift Concurrency and AsyncSequence APIs instead.")
    public func forEach(_ body: @escaping (Row) throws -> Void) -> EventLoopFuture<Void> {
      precondition(
        self.hasMorePages, "Only one of 'forEach' or 'map' can be called once per PaginatedRows")

      guard let eventLoop = self.eventLoop else {
        preconditionFailure("EventLoop must not be nil")
      }

      func _forEach() -> EventLoopFuture<Void> {
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
    public func map<T>(_ transform: @escaping (Row) throws -> T) -> EventLoopFuture<[T]> {
      precondition(
        self.hasMorePages, "Only one of 'forEach' or 'map' can be called once per PaginatedRows")

      guard let eventLoop = self.eventLoop else {
        preconditionFailure("EventLoop must not be nil in EventLoop based APIs")
      }

      func _map(_ accumulated: [T]) -> EventLoopFuture<[T]> {
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

    #if compiler(>=5.5) && canImport(_Concurrency)
      @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
      internal init(session: Session, statement: Statement, logger: Logger?) {
        self.session = session
        self.statement = statement
        self.eventLoop = nil
        self.logger = logger
      }

      /// Fetches next page of rows or throw ``CassandraClient/Error/rowsExhausted`` error if there are no more pages.
      @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
      public func nextPage() async throws -> Rows {
        guard self.hasMorePages else {
          throw CassandraClient.Error.rowsExhausted
        }

        do {
          let rows = try await session.execute(statement: self.statement, logger: self.logger)
          self.hasMorePages = cass_result_has_more_pages(rows.rawPointer) == cass_true
          if self.hasMorePages {
            cass_statement_set_paging_state(self.statement.rawPointer, rows.rawPointer)
          }
          return rows
        } catch {
          self.hasMorePages = false
          throw error
        }
      }

      /// Iterates through all rows in all pages and invokes the given closure on each.
      @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
      @available(*, deprecated, message: "Use AsyncSequence APIs instead.")
      public func forEach(_ body: @escaping (Row) throws -> Void) async throws {
        precondition(
          self.hasMorePages, "Only one of 'forEach' or 'map' can be called once per PaginatedRows")

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
          self.hasMorePages, "Only one of 'forEach' or 'map' can be called once per PaginatedRows")

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
    #endif
  }
}

#if compiler(>=5.5) && canImport(_Concurrency)
  @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
  extension CassandraClient.PaginatedRows: AsyncSequence {
    public typealias Element = CassandraClient.Row

    /// Make async iterator for the ``PaginatedRows``.
    ///
    /// - Warning:
    ///   This can be called only once for each ``PaginatedRows``,
    ///   otherwise it will throw ``CassandraClient.Error.rowsExhausted``.
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
#endif
