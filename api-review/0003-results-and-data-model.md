# Results and data model

## Overview

- **Area:** Results and data model — 400 of the module's public symbols.
- **Note:** This PR exists to collect API review feedback ahead of 1.0. It will be closed rather than merged; anything it settles lands as its own change.
- **Generated from:** `Sources/CassandraClient` at `3780cda`, with Apple Swift version 6.3.1 (swiftlang-6.3.1.1.2 clang-2100.0.123.102). Derived, not maintained by hand — see `api-review/README.md` to regenerate.

Inherited protocol members are listed without their standard-library documentation, grouped under the protocol that declared each. Members the symbol graph does not emit are marked `[reconstructed]`.

## Public API

````swift
/// Time-based UUID (version 1).
public struct TimeBasedUUID : CustomStringConvertible, Decodable, Encodable, Hashable, Sendable {
    public init()

    public var description: String { get }
    public var uuidString: String { get }

    // [inherited from Equatable] 1 member
    static func != (lhs: Self, rhs: Self) -> Bool

    // [compiler-synthesized] 1 member
    init(from decoder: any Decoder) throws

    // [reconstructed] 2 members guaranteed by a conformance above that the symbol graph does not emit
    public static func == (lhs: Self, rhs: Self) -> Bool
    public func encode(to encoder: Encoder) throws
}

extension CassandraClient {
    /// A column in a resulting ``Row`` of a Cassandra query.
    ///
    /// Note that the value is only good as long as the iterator it came from hasn't been advanced.
    public struct Column {
        /// Get column value as `Bool`.
        public var bool: Bool? { get }
        /// Get column value as `[UInt8]`.
        public var bytes: [UInt8]? { get }
        /// Get column date value as `UInt32`.
        public var date: UInt32? { get }
        /// Get column value as `Double`.
        public var double: Double? { get }
        /// Get column value as `[Double]`.
        public var doubleArray: [Double]? { get }
        /// Get column value as `Float32`.
        public var float32: Float32? { get }
        /// Get column value as `[Float32]`.
        public var float32Array: [Float32]? { get }
        /// Get column value as `Int16`.
        public var int16: Int16? { get }
        /// Get column value as `[Int16]`.
        public var int16Array: [Int16]? { get }
        public var int16BoolMap: [Int16 : Bool]? { get }
        public var int16DoubleMap: [Int16 : Double]? { get }
        public var int16Float32Map: [Int16 : Float32]? { get }
        public var int16Int16Map: [Int16 : Int16]? { get }
        public var int16Int32Map: [Int16 : Int32]? { get }
        public var int16Int64Map: [Int16 : Int64]? { get }
        public var int16Int8Map: [Int16 : Int8]? { get }
        public var int16StringMap: [Int16 : String]? { get }
        public var int16UUIDMap: [Int16 : Foundation.UUID]? { get }
        /// Get column value as `Int32`.
        public var int32: Int32? { get }
        /// Get column value as `[Int32]`.
        public var int32Array: [Int32]? { get }
        public var int32BoolMap: [Int32 : Bool]? { get }
        public var int32DoubleMap: [Int32 : Double]? { get }
        public var int32Float32Map: [Int32 : Float32]? { get }
        public var int32Int16Map: [Int32 : Int16]? { get }
        public var int32Int32Map: [Int32 : Int32]? { get }
        public var int32Int64Map: [Int32 : Int64]? { get }
        public var int32Int8Map: [Int32 : Int8]? { get }
        public var int32StringMap: [Int32 : String]? { get }
        public var int32UUIDMap: [Int32 : Foundation.UUID]? { get }
        /// Get column value as `Int64`.
        public var int64: Int64? { get }
        /// Get column value as `[Int64]`.
        public var int64Array: [Int64]? { get }
        public var int64BoolMap: [Int64 : Bool]? { get }
        public var int64DoubleMap: [Int64 : Double]? { get }
        public var int64Float32Map: [Int64 : Float32]? { get }
        public var int64Int16Map: [Int64 : Int16]? { get }
        public var int64Int32Map: [Int64 : Int32]? { get }
        public var int64Int64Map: [Int64 : Int64]? { get }
        public var int64Int8Map: [Int64 : Int8]? { get }
        public var int64StringMap: [Int64 : String]? { get }
        public var int64UUIDMap: [Int64 : Foundation.UUID]? { get }
        /// Get column value as `Int8`.
        public var int8: Int8? { get }
        /// Get column value as `[Int8]`.
        public var int8Array: [Int8]? { get }
        public var int8BoolMap: [Int8 : Bool]? { get }
        public var int8DoubleMap: [Int8 : Double]? { get }
        public var int8Float32Map: [Int8 : Float32]? { get }
        public var int8Int16Map: [Int8 : Int16]? { get }
        public var int8Int32Map: [Int8 : Int32]? { get }
        public var int8Int64Map: [Int8 : Int64]? { get }
        public var int8Int8Map: [Int8 : Int8]? { get }
        public var int8StringMap: [Int8 : String]? { get }
        public var int8UUIDMap: [Int8 : Foundation.UUID]? { get }
        /// Get column value as `String`.
        public var string: String? { get }
        /// Get column value as `[String]`.
        public var stringArray: [String]? { get }
        public var stringBoolMap: [String : Bool]? { get }
        public var stringDoubleMap: [String : Double]? { get }
        public var stringFloat32Map: [String : Float32]? { get }
        public var stringInt16Map: [String : Int16]? { get }
        public var stringInt32Map: [String : Int32]? { get }
        public var stringInt64Map: [String : Int64]? { get }
        public var stringInt8Map: [String : Int8]? { get }
        public var stringStringMap: [String : String]? { get }
        public var stringUUIDMap: [String : Foundation.UUID]? { get }
        /// Get column timestamp value as `Int64`.
        public var timestamp: Int64? { get }
        /// Get column value as ``TimeBasedUUID``.
        public var timeuuid: TimeBasedUUID? { get }
        public var timeuuidBoolMap: [TimeBasedUUID : Bool]? { get }
        public var timeuuidDoubleMap: [TimeBasedUUID : Double]? { get }
        public var timeuuidFloat32Map: [TimeBasedUUID : Float32]? { get }
        public var timeuuidInt16Map: [TimeBasedUUID : Int16]? { get }
        public var timeuuidInt32Map: [TimeBasedUUID : Int32]? { get }
        public var timeuuidInt64Map: [TimeBasedUUID : Int64]? { get }
        public var timeuuidInt8Map: [TimeBasedUUID : Int8]? { get }
        public var timeuuidStringMap: [TimeBasedUUID : String]? { get }
        public var timeuuidUUIDMap: [TimeBasedUUID : Foundation.UUID]? { get }
        /// Get column value as `UInt32`.
        public var uint32: UInt32? { get }
        /// Get column value as `UUID`.
        public var uuid: Foundation.UUID? { get }
        /// Get column value as `[UUID]`.
        public var uuidArray: [Foundation.UUID]? { get }
        public var uuidBoolMap: [Foundation.UUID : Bool]? { get }
        public var uuidDoubleMap: [Foundation.UUID : Double]? { get }
        public var uuidFloat32Map: [Foundation.UUID : Float32]? { get }
        public var uuidInt16Map: [Foundation.UUID : Int16]? { get }
        public var uuidInt32Map: [Foundation.UUID : Int32]? { get }
        public var uuidInt64Map: [Foundation.UUID : Int64]? { get }
        public var uuidInt8Map: [Foundation.UUID : Int8]? { get }
        public var uuidStringMap: [Foundation.UUID : String]? { get }
        public var uuidUUIDMap: [Foundation.UUID : Foundation.UUID]? { get }

        /// Decrypt column and return as `[UInt8]`.
        public func decryptedBytes(encryptor: CassandraClient.Encryptor, context: CassandraClient.EncryptionContext) throws -> [UInt8]?
        /// Decrypt column and return as `Date`.
        public func decryptedDate(encryptor: CassandraClient.Encryptor, context: CassandraClient.EncryptionContext) throws -> Foundation.Date?
        /// Decrypt column and return as `Double`.
        public func decryptedDouble(encryptor: CassandraClient.Encryptor, context: CassandraClient.EncryptionContext) throws -> Double?
        /// Decrypt column and return as `Int32`.
        public func decryptedInt32(encryptor: CassandraClient.Encryptor, context: CassandraClient.EncryptionContext) throws -> Int32?
        /// Decrypt column and return as `Int64`.
        public func decryptedInt64(encryptor: CassandraClient.Encryptor, context: CassandraClient.EncryptionContext) throws -> Int64?
        /// Decrypt column and return as `String`.
        public func decryptedString(encryptor: CassandraClient.Encryptor, context: CassandraClient.EncryptionContext) throws -> String?
        /// Decrypt column and return as `UUID`.
        public func decryptedUUID(encryptor: CassandraClient.Encryptor, context: CassandraClient.EncryptionContext) throws -> Foundation.UUID?
        public func isNull() -> Bool
        /// Get column value as buffer pointer and pass it to the given closure.
        public func withUnsafeBuffer<R>(closure: (UnsafeBufferPointer<UInt8>?) throws -> R) rethrows -> R
    }

    /// A reusable page token that can be used by `Statement` to resume querying
    /// at a specific position. Two tokens can be compared for equality.
    ///
    /// - Important: This type has no public initializer and provides no API for reading its
    ///   bytes, so a token can only be vended by ``CassandraClient/Rows/opaquePagingStateToken()``
    ///   and cannot be serialized through this library. Adding a public initializer or a public
    ///   bytes accessor would let a caller move a paging state across a trust boundary.
    public struct OpaquePagingStateToken : Hashable, Sendable {

        // [inherited from Equatable] 1 member
        static func != (lhs: Self, rhs: Self) -> Bool

        // [reconstructed] 1 member guaranteed by a conformance above that the symbol graph does not emit
        public static func == (lhs: Self, rhs: Self) -> Bool
    }

    /// Resulting row(s) of a Cassandra query. Data are paginated.
    public final class PaginatedRows : AsyncSequence, Sendable {
        /// If `true`, calling ``nextPage()`` will return a new set of ``CassandraClient/Rows``.
        /// Otherwise it will throw ``CassandraClient/Error/rowsExhausted`` error.
        public var hasMorePages: Bool { get }

        public typealias Element = CassandraClient.Row
        /// An `AsyncSequence` that iterates over the adjacent pairs of the original
        /// original `AsyncSequence`.
        ///
        /// ```
        /// for await (first, second) in (1...5).async.adjacentPairs() {
        ///    print("First: \(first), Second: \(second)")
        /// }
        ///
        /// // First: 1, Second: 2
        /// // First: 2, Second: 3
        /// // First: 3, Second: 4
        /// // First: 4, Second: 5
        /// ```
        ///
        /// - Returns: An `AsyncSequence` where the element is a tuple of two adjacent elements
        ///   or the original `AsyncSequence`.
        @available(macOS 10.15, watchOS 6.0, iOS 13.0, tvOS 13.0, *)
        public func adjacentPairs() -> AsyncAdjacentPairsSequence<Self>
        /// Creates an asynchronous sequence that buffers elements.
        ///
        /// The buffering behaviour is dictated by the policy:
        /// - bounded: will buffer elements until the limit is reached. Then it will suspend the upstream async sequence.
        /// - unbounded: will buffer elements without limit.
        /// - bufferingNewest: will buffer elements until the limit is reached. Then it will discard the oldest elements.
        /// - bufferingOldest: will buffer elements until the limit is reached. Then it will discard the newest elements.
        ///
        /// - Parameter policy: A policy that drives the behaviour of the ``AsyncBufferSequence``
        /// - Returns: An asynchronous sequence that buffers elements up to a given limit.
        @available(macOS 10.15, watchOS 6.0, iOS 13.0, tvOS 13.0, *)
        public func buffer(policy: AsyncBufferSequencePolicy) -> AsyncBufferSequence<Self>
        /// Creates an asynchronous sequence that creates chunks by testing if elements belong in the same group.
        @available(macOS 10.15, watchOS 6.0, iOS 13.0, tvOS 13.0, *)
        public func chunked(by belongInSameGroup: @escaping @Sendable (Self.Element, Self.Element) -> Bool) -> AsyncChunkedByGroupSequence<Self, [Self.Element]>
        /// Creates an asynchronous sequence that creates chunks of a given `RangeReplaceableCollection` type when an `AsyncTimerSequence` fires.
        @available(macOS 13.0, watchOS 9.0, iOS 16.0, tvOS 16.0, *)
        public func chunked<C, Collected>(by timer: AsyncTimerSequence<C>, into: Collected.Type) -> AsyncChunksOfCountOrSignalSequence<Self, Collected, AsyncTimerSequence<C>> where Self : Sendable, C : Clock, Collected : RangeReplaceableCollection, Self.Element : Sendable, Self.Element == Collected.Element
        /// Creates an asynchronous sequence that creates chunks of a given `RangeReplaceableCollection` type when a signal `AsyncSequence` produces an element.
        @available(macOS 10.15, watchOS 6.0, iOS 13.0, tvOS 13.0, *)
        public func chunked<Signal, Collected>(by signal: Signal, into: Collected.Type) -> AsyncChunksOfCountOrSignalSequence<Self, Collected, Signal> where Self : Sendable, Signal : Sendable, Signal : AsyncSequence, Collected : RangeReplaceableCollection, Self.Element : Sendable, Self.Element == Collected.Element, Signal.Element : Sendable
        /// Creates an asynchronous sequence that creates chunks of a given `RangeReplaceableCollection`
        /// type by testing if elements belong in the same group.
        @available(macOS 10.15, watchOS 6.0, iOS 13.0, tvOS 13.0, *)
        public func chunked<Collected>(into: Collected.Type, by belongInSameGroup: @escaping @Sendable (Self.Element, Self.Element) -> Bool) -> AsyncChunkedByGroupSequence<Self, Collected> where Collected : RangeReplaceableCollection, Self.Element == Collected.Element
        /// Creates an asynchronous sequence that creates chunks of a given `RangeReplaceableCollection` type on the uniqueness of a given subject.
        @available(macOS 10.15, watchOS 6.0, iOS 13.0, tvOS 13.0, *)
        public func chunked<Subject, Collected>(into: Collected.Type, on projection: @escaping @Sendable (Self.Element) -> Subject) -> AsyncChunkedOnProjectionSequence<Self, Subject, Collected> where Subject : Equatable, Collected : RangeReplaceableCollection, Self.Element == Collected.Element
        /// Creates an asynchronous sequence that creates chunks on the uniqueness of a given subject.
        @available(macOS 10.15, watchOS 6.0, iOS 13.0, tvOS 13.0, *)
        public func chunked<Subject>(on projection: @escaping @Sendable (Self.Element) -> Subject) -> AsyncChunkedOnProjectionSequence<Self, Subject, [Self.Element]> where Subject : Equatable
        /// Creates an asynchronous sequence that creates chunks of a given count.
        @available(macOS 10.15, watchOS 6.0, iOS 13.0, tvOS 13.0, *)
        public func chunks(ofCount count: Int) -> AsyncChunksOfCountSequence<Self, [Self.Element]>
        /// Creates an asynchronous sequence that creates chunks of a given `RangeReplaceableCollection` of a given count.
        @available(macOS 10.15, watchOS 6.0, iOS 13.0, tvOS 13.0, *)
        public func chunks<Collected>(ofCount count: Int, into: Collected.Type) -> AsyncChunksOfCountSequence<Self, Collected> where Collected : RangeReplaceableCollection, Self.Element == Collected.Element
        /// Creates an asynchronous sequence that creates chunks of a given count or when an `AsyncTimerSequence` fires.
        @available(macOS 13.0, watchOS 9.0, iOS 16.0, tvOS 16.0, *)
        public func chunks<C>(ofCount count: Int, or timer: AsyncTimerSequence<C>) -> AsyncChunksOfCountOrSignalSequence<Self, [Self.Element], AsyncTimerSequence<C>> where Self : Sendable, C : Clock, Self.Element : Sendable
        /// Creates an asynchronous sequence that creates chunks of a given count or when a signal `AsyncSequence` produces an element.
        @available(macOS 10.15, watchOS 6.0, iOS 13.0, tvOS 13.0, *)
        public func chunks<Signal>(ofCount count: Int, or signal: Signal) -> AsyncChunksOfCountOrSignalSequence<Self, [Self.Element], Signal> where Self : Sendable, Signal : Sendable, Signal : AsyncSequence, Self.Element : Sendable, Signal.Element : Sendable
        /// Creates an asynchronous sequence that creates chunks of a given `RangeReplaceableCollection` type of a given count or when an `AsyncTimerSequence` fires.
        @available(macOS 13.0, watchOS 9.0, iOS 16.0, tvOS 16.0, *)
        public func chunks<C, Collected>(ofCount count: Int, or timer: AsyncTimerSequence<C>, into: Collected.Type) -> AsyncChunksOfCountOrSignalSequence<Self, Collected, AsyncTimerSequence<C>> where Self : Sendable, C : Clock, Collected : RangeReplaceableCollection, Self.Element : Sendable, Self.Element == Collected.Element
        /// Creates an asynchronous sequence that creates chunks of a given `RangeReplaceableCollection` type of a given count or when a signal `AsyncSequence` produces an element.
        @available(macOS 10.15, watchOS 6.0, iOS 13.0, tvOS 13.0, *)
        public func chunks<Signal, Collected>(ofCount count: Int, or signal: Signal, into: Collected.Type) -> AsyncChunksOfCountOrSignalSequence<Self, Collected, Signal> where Self : Sendable, Signal : Sendable, Signal : AsyncSequence, Collected : RangeReplaceableCollection, Self.Element : Sendable, Self.Element == Collected.Element, Signal.Element : Sendable
        /// Returns a new `AsyncSequence` that iterates over every non-nil element from the
        /// original `AsyncSequence`.
        ///
        /// Produces the same result as `c.compactMap { $0 }`.
        ///
        /// - Returns: An `AsyncSequence` where the element is the unwrapped original
        ///   element and iterates over every non-nil element from the original
        ///   `AsyncSequence`.
        @available(macOS 10.15, watchOS 6.0, iOS 13.0, tvOS 13.0, *)
        public func compacted<Unwrapped>() -> AsyncCompactedSequence<Self, Unwrapped> where Self.Element == Unwrapped?
        /// Creates an asynchronous sequence that emits the latest element after a given quiescence period
        /// has elapsed.
        @available(macOS 13.0, watchOS 9.0, iOS 16.0, tvOS 16.0, *)
        public func debounce(for interval: Duration, tolerance: Duration? = nil) -> AsyncDebounceSequence<Self, ContinuousClock> where Self : Sendable, Self.Element : Sendable
        /// Creates an asynchronous sequence that emits the latest element after a given quiescence period
        /// has elapsed by using a specified Clock.
        @available(macOS 13.0, watchOS 9.0, iOS 16.0, tvOS 16.0, *)
        public func debounce<C>(for interval: C.Instant.Duration, tolerance: C.Instant.Duration? = nil, clock: C) -> AsyncDebounceSequence<Self, C> where Self : Sendable, C : Clock, Self.Element : Sendable
        /// Transforms elements into new asynchronous sequences, emitting elements
        /// from the most recent inner sequence.
        ///
        /// When a new element is emitted by this sequence, the `transform`
        /// is called to produce a new inner sequence. Iteration on the
        /// previous inner sequence is cancelled, and iteration begins
        /// on the new one.
        @available(macOS 15.0, watchOS 11.0, iOS 18.0, visionOS 2.0, tvOS 18.0, *)
        public func flatMapLatest<T>(_ transform: @escaping @Sendable (Self.Element) -> T) -> some Sendable & AsyncSequence<T.Element, Self.Failure> where T : Sendable, T : AsyncSequence, Self.Element : Sendable, Self.Failure == T.Failure, T.Element : Sendable

        @available(macOS 15.0, watchOS 11.0, iOS 18.0, visionOS 2.0, tvOS 18.0, *)
        public func flatMapLatest<T>(_ transform: @escaping @Sendable (Self.Element) -> T) -> some Sendable & AsyncSequence<T.Element, Self.Failure> where T : Sendable, T : AsyncSequence, Self.Element : Sendable, T.Element : Sendable, T.Failure == Never

        @available(macOS 15.0, watchOS 11.0, iOS 18.0, visionOS 2.0, tvOS 18.0, *)
        public func flatMapLatest<T>(_ transform: @escaping @Sendable (Self.Element) -> T) -> some Sendable & AsyncSequence<T.Element, T.Failure> where T : Sendable, T : AsyncSequence, Self.Element : Sendable, Self.Failure == Never, T.Element : Sendable

        /// Iterates through all rows in all pages and invokes the given closure on each.
        @available(*, deprecated, message: "Use AsyncSequence APIs instead.")
        public func forEach(_ body: @escaping (Row) throws -> Void) async throws
        /// Returns a new asynchronous sequence containing the elements of this asynchronous sequence, inserting
        /// the given separator between each element.
        ///
        /// Any value of this asynchronous sequence's element type can be used as the separator.
        ///
        /// The following example shows how an async sequences of `String`s can be interspersed using `-` as the separator:
        ///
        /// ```
        /// let input = ["A", "B", "C"].async
        /// let interspersed = input.interspersed(with: { "-" })
        /// for await element in interspersed {
        ///   print(element)
        /// }
        /// // Prints "A" "-" "B" "-" "C"
        /// ```
        ///
        /// - Parameters:
        ///   - every: Dictates after how many elements a separator should be inserted.
        ///   - separator: A closure that produces the value to insert in between each of this async sequence’s elements.
        /// - Returns: The interspersed asynchronous sequence of elements.
        @available(macOS 10.15, watchOS 6.0, iOS 13.0, tvOS 13.0, *)
        public func interspersed(every: Int = 1, with separator: @escaping @Sendable () -> Self.Element) -> AsyncInterspersedSequence<Self>
        /// Returns a new asynchronous sequence containing the elements of this asynchronous sequence, inserting
        /// the given separator between each element.
        ///
        /// Any value of this asynchronous sequence's element type can be used as the separator.
        ///
        /// The following example shows how an async sequences of `String`s can be interspersed using `-` as the separator:
        ///
        /// ```
        /// let input = ["A", "B", "C"].async
        /// let interspersed = input.interspersed(with: { "-" })
        /// for await element in interspersed {
        ///   print(element)
        /// }
        /// // Prints "A" "-" "B" "-" "C"
        /// ```
        ///
        /// - Parameters:
        ///   - every: Dictates after how many elements a separator should be inserted.
        ///   - separator: A closure that produces the value to insert in between each of this async sequence’s elements.
        /// - Returns: The interspersed asynchronous sequence of elements.
        @available(macOS 10.15, watchOS 6.0, iOS 13.0, tvOS 13.0, *)
        public func interspersed(every: Int = 1, with separator: @escaping @Sendable () async -> Self.Element) -> AsyncInterspersedSequence<Self>
        /// Returns a new asynchronous sequence containing the elements of this asynchronous sequence, inserting
        /// the given separator between each element.
        ///
        /// Any value of this asynchronous sequence's element type can be used as the separator.
        ///
        /// The following example shows how an async sequences of `String`s can be interspersed using `-` as the separator:
        ///
        /// ```
        /// let input = ["A", "B", "C"].async
        /// let interspersed = input.interspersed(with: { "-" })
        /// for await element in interspersed {
        ///   print(element)
        /// }
        /// // Prints "A" "-" "B" "-" "C"
        /// ```
        ///
        /// - Parameters:
        ///   - every: Dictates after how many elements a separator should be inserted.
        ///   - separator: A closure that produces the value to insert in between each of this async sequence’s elements.
        /// - Returns: The interspersed asynchronous sequence of elements.
        @available(macOS 10.15, watchOS 6.0, iOS 13.0, tvOS 13.0, *)
        public func interspersed(every: Int = 1, with separator: @escaping @Sendable () async throws -> Self.Element) -> AsyncThrowingInterspersedSequence<Self>
        /// Returns a new asynchronous sequence containing the elements of this asynchronous sequence, inserting
        /// the given separator between each element.
        ///
        /// Any value of this asynchronous sequence's element type can be used as the separator.
        ///
        /// The following example shows how an async sequences of `String`s can be interspersed using `-` as the separator:
        ///
        /// ```
        /// let input = ["A", "B", "C"].async
        /// let interspersed = input.interspersed(with: { "-" })
        /// for await element in interspersed {
        ///   print(element)
        /// }
        /// // Prints "A" "-" "B" "-" "C"
        /// ```
        ///
        /// - Parameters:
        ///   - every: Dictates after how many elements a separator should be inserted.
        ///   - separator: A closure that produces the value to insert in between each of this async sequence’s elements.
        /// - Returns: The interspersed asynchronous sequence of elements.
        @available(macOS 10.15, watchOS 6.0, iOS 13.0, tvOS 13.0, *)
        public func interspersed(every: Int = 1, with separator: @escaping @Sendable () throws -> Self.Element) -> AsyncThrowingInterspersedSequence<Self>
        /// Returns a new asynchronous sequence containing the elements of this asynchronous sequence, inserting
        /// the given separator between each element.
        ///
        /// Any value of this asynchronous sequence's element type can be used as the separator.
        ///
        /// The following example shows how an async sequences of `String`s can be interspersed using `-` as the separator:
        ///
        /// ```
        /// let input = ["A", "B", "C"].async
        /// let interspersed = input.interspersed(with: "-")
        /// for await element in interspersed {
        ///   print(element)
        /// }
        /// // Prints "A" "-" "B" "-" "C"
        /// ```
        ///
        /// - Parameters:
        ///   - every: Dictates after how many elements a separator should be inserted.
        ///   - separator: The value to insert in between each of this async sequence’s elements.
        /// - Returns: The interspersed asynchronous sequence of elements.
        @available(macOS 10.15, watchOS 6.0, iOS 13.0, tvOS 13.0, *)
        public func interspersed(every: Int = 1, with separator: Self.Element) -> AsyncInterspersedSequence<Self>
        /// Make async iterator for the ``PaginatedRows``.
        ///
        /// - Warning:
        ///   This can be called only once for each ``PaginatedRows``,
        ///   Otherwise it will throw ``CassandraClient/Error/rowsExhausted`` error.
        public func makeAsyncIterator() -> AsyncIterator
        /// Iterates through all rows in all pages and applies `transform` on each.
        @available(*, deprecated, message: "Use AsyncSequence APIs instead.")
        public func map<T>(_ transform: @escaping (Row) throws -> T) async throws -> [T]
        /// Converts any failure into a new error.
        ///
        /// - Parameter transform: A closure that takes the failure as a parameter and returns a new error.
        /// - Returns: An asynchronous sequence that maps the error thrown into the one produced by the transform closure.
        ///
        /// Use the ``mapError(_:)`` operator when you need to replace one error type with another.
        @available(macOS 15.0, watchOS 11.0, iOS 18.0, visionOS 2.0, tvOS 18.0, *)
        public func mapError<MappedError>(_ transform: @escaping @Sendable (Self.Failure) -> MappedError) -> some AsyncSequence<Self.Element, MappedError> where MappedError : Error

        /// Converts any failure into a new error.
        ///
        /// - Parameter transform: A closure that takes the failure as a parameter and returns a new error.
        /// - Returns: An asynchronous sequence that maps the error thrown into the one produced by the transform closure.
        ///
        /// Use the ``mapError(_:)`` operator when you need to replace one error type with another.
        @available(macOS 15.0, watchOS 11.0, iOS 18.0, visionOS 2.0, tvOS 18.0, *)
        public func mapError<MappedError>(_ transform: @escaping @Sendable (Self.Failure) -> MappedError) -> some Sendable & AsyncSequence<Self.Element, MappedError> where Self : Sendable, MappedError : Error, Self.Element : Sendable

        /// Fetches next page of rows or throw ``CassandraClient/Error/rowsExhausted`` error if there are no more pages.
        public func nextPage() async throws -> Rows
        /// Returns an asynchronous sequence containing the accumulated results of combining the
        /// elements of the asynchronous sequence using the given closure.
        ///
        /// This can be seen as applying the reduce function to each element and
        /// producing an asynchronous sequence consisting of the initial value followed
        /// by these results.
        ///
        /// ```
        /// let runningTotal = [1, 2, 3, 4].async.reductions(+)
        /// print(await Array(runningTotal))
        ///
        /// // prints [1, 3, 6, 10]
        /// ```
        ///
        /// - Parameter transform: A closure that combines the previously reduced
        ///   result and the next element in the receiving sequence, and returns
        ///   the result.
        /// - Returns: An asynchronous sequence of the reduced elements.
        @available(macOS 10.15, watchOS 6.0, iOS 13.0, tvOS 13.0, *)
        public func reductions(_ transform: @escaping @Sendable (Self.Element, Self.Element) async -> Self.Element) -> AsyncInclusiveReductionsSequence<Self>
        /// Returns an asynchronous sequence containing the accumulated results of combining the
        /// elements of the asynchronous sequence using the given closure.
        ///
        /// Starting from the initial value, this combines each element of the base
        /// asynchronous sequence with the running result and emits each intermediate
        /// result. The initial value itself is not emitted, so the first element of
        /// the returned sequence is the result of combining the initial value with
        /// the first element of the base sequence. If the base sequence is empty, the
        /// returned sequence is also empty.
        ///
        /// - Parameters:
        ///   - initial: The value to use as the initial value.
        ///   - transform: A closure that combines the previously-reduced result and
        ///     the next element in the receiving asynchronous sequence, which it returns.
        /// - Returns: An asynchronous sequence of the reduced elements.
        @available(macOS 10.15, watchOS 6.0, iOS 13.0, tvOS 13.0, *)
        public func reductions<Result>(_ initial: Result, _ transform: @escaping @Sendable (Result, Self.Element) async -> Result) -> AsyncExclusiveReductionsSequence<Self, Result>
        /// Returns an asynchronous sequence containing the accumulated results of combining the
        /// elements of the asynchronous sequence using the given error-throwing closure.
        ///
        /// Starting from the initial value, this combines each element of the base
        /// asynchronous sequence with the running result and emits each intermediate
        /// result. The initial value itself is not emitted, so the first element of
        /// the returned sequence is the result of combining the initial value with
        /// the first element of the base sequence. If the base sequence is empty, the
        /// returned sequence is also empty.
        ///
        /// - Parameters:
        ///   - initial: The value to use as the initial value.
        ///   - transform: A closure that combines the previously reduced result and
        ///     the next element in the receiving asynchronous sequence and returns
        ///     the result. If the closure throws an error, the sequence throws.
        /// - Returns: An asynchronous sequence of the reduced elements.
        @available(macOS 10.15, watchOS 6.0, iOS 13.0, tvOS 13.0, *)
        public func reductions<Result>(_ initial: Result, _ transform: @escaping @Sendable (Result, Self.Element) async throws -> Result) -> AsyncThrowingExclusiveReductionsSequence<Self, Result>
        /// Returns an asynchronous sequence containing the accumulated results of combining the
        /// elements of the asynchronous sequence using the given closure.
        ///
        /// Starting from the initial value, this combines each element of the base
        /// asynchronous sequence with the running result and emits each intermediate
        /// result. The initial value itself is not emitted, so the first element of
        /// the returned sequence is the result of combining the initial value with
        /// the first element of the base sequence. If the base sequence is empty, the
        /// returned sequence is also empty.
        ///
        /// - Parameters:
        ///   - initial: The value to use as the initial value.
        ///   - transform: A closure that combines the previously-reduced result and
        ///     the next element in the receiving asynchronous sequence, mutating the
        ///     previous result instead of returning a value.
        /// - Returns: An asynchronous sequence of the reduced elements.
        @available(macOS 10.15, watchOS 6.0, iOS 13.0, tvOS 13.0, *)
        public func reductions<Result>(into initial: Result, _ transform: @escaping @Sendable (inout Result, Self.Element) async -> Void) -> AsyncExclusiveReductionsSequence<Self, Result>
        /// Returns an asynchronous sequence containing the accumulated results of combining the
        /// elements of the asynchronous sequence using the given error-throwing closure.
        ///
        /// Starting from the initial value, this combines each element of the base
        /// asynchronous sequence with the running result and emits each intermediate
        /// result. The initial value itself is not emitted, so the first element of
        /// the returned sequence is the result of combining the initial value with
        /// the first element of the base sequence. If the base sequence is empty, the
        /// returned sequence is also empty.
        ///
        /// - Parameters:
        ///   - initial: The value to use as the initial value.
        ///   - transform: A closure that combines the previously reduced result and
        ///     the next element in the receiving asynchronous sequence, mutating the
        ///     previous result instead of returning a value. If the closure throws an
        ///     error, the sequence throws.
        /// - Returns: An asynchronous sequence of the reduced elements.
        @available(macOS 10.15, watchOS 6.0, iOS 13.0, tvOS 13.0, *)
        public func reductions<Result>(into initial: Result, _ transform: @escaping @Sendable (inout Result, Self.Element) async throws -> Void) -> AsyncThrowingExclusiveReductionsSequence<Self, Result>
        /// Creates an asynchronous sequence that omits repeated elements by testing them with a predicate.
        @available(macOS 10.15, watchOS 6.0, iOS 13.0, tvOS 13.0, *)
        public func removeDuplicates(by predicate: @escaping @Sendable (Self.Element, Self.Element) async -> Bool) -> AsyncRemoveDuplicatesSequence<Self>

        public struct AsyncIterator : AsyncIteratorProtocol {
            public mutating func next() async throws -> CassandraClient.Row?

            // [inherited from AsyncIteratorProtocol] 2 members
            @available(macOS 15.0, watchOS 11.0, iOS 18.0, visionOS 2.0, tvOS 18.0, *)
            mutating func next(isolation actor: isolated (any Actor)?) async throws(Self.Failure) -> Self.Element?
            @available(macOS 15.0, watchOS 11.0, iOS 18.0, visionOS 2.0, tvOS 18.0, *)
            mutating func next() async throws(Self.Failure) -> Self.Element?
        }

        // [inherited from AsyncSequence] 20 members
        @available(macOS 10.15, watchOS 6.0, iOS 13.0, tvOS 13.0, *)
        @preconcurrency func compactMap<ElementOfResult>(_ transform: @escaping @Sendable (Self.Element) async -> ElementOfResult?) -> AsyncCompactMapSequence<Self, ElementOfResult>
        @available(macOS 10.15, watchOS 6.0, iOS 13.0, tvOS 13.0, *)
        func dropFirst(_ count: Int = 1) -> AsyncDropFirstSequence<Self>
        @available(macOS 10.15, watchOS 6.0, iOS 13.0, tvOS 13.0, *)
        @preconcurrency func drop(while predicate: @escaping @Sendable (Self.Element) async -> Bool) -> AsyncDropWhileSequence<Self>
        @available(macOS 10.15, watchOS 6.0, iOS 13.0, tvOS 13.0, *)
        @preconcurrency func filter(_ isIncluded: @escaping @Sendable (Self.Element) async -> Bool) -> AsyncFilterSequence<Self>
        @available(macOS 10.15, watchOS 6.0, iOS 13.0, tvOS 13.0, *)
        @preconcurrency func flatMap<SegmentOfResult>(_ transform: @escaping @Sendable (Self.Element) async -> SegmentOfResult) -> AsyncFlatMapSequence<Self, SegmentOfResult> where SegmentOfResult : AsyncSequence, Self.Failure == SegmentOfResult.Failure
        @available(macOS 10.15, watchOS 6.0, iOS 13.0, tvOS 13.0, *)
        @preconcurrency func flatMap<SegmentOfResult>(_ transform: @escaping @Sendable (Self.Element) async -> SegmentOfResult) -> AsyncFlatMapSequence<Self, SegmentOfResult> where SegmentOfResult : AsyncSequence, SegmentOfResult.Failure == Never
        @available(macOS 10.15, watchOS 6.0, iOS 13.0, tvOS 13.0, *)
        @preconcurrency func flatMap<SegmentOfResult>(_ transform: @escaping @Sendable (Self.Element) async -> SegmentOfResult) -> AsyncFlatMapSequence<Self, SegmentOfResult> where SegmentOfResult : AsyncSequence, Self.Failure == Never, SegmentOfResult.Failure == Never
        @available(macOS 10.15, watchOS 6.0, iOS 13.0, tvOS 13.0, *)
        @preconcurrency func map<Transformed>(_ transform: @escaping @Sendable (Self.Element) async -> Transformed) -> AsyncMapSequence<Self, Transformed>
        @available(macOS 10.15, watchOS 6.0, iOS 13.0, tvOS 13.0, *)
        func prefix(_ count: Int) -> AsyncPrefixSequence<Self>
        @available(macOS 10.15, watchOS 6.0, iOS 13.0, tvOS 13.0, *)
        @preconcurrency func prefix(while predicate: @escaping @Sendable (Self.Element) async -> Bool) rethrows -> AsyncPrefixWhileSequence<Self>
        @available(macOS 10.15, watchOS 6.0, iOS 13.0, tvOS 13.0, *)
        func reduce<Result>(_ initialResult: Result, _ nextPartialResult: (Result, Self.Element) async throws -> Result) async rethrows -> Result
        @available(macOS 10.15, watchOS 6.0, iOS 13.0, tvOS 13.0, *)
        func reduce<Result>(into initialResult: Result, _ updateAccumulatingResult: (inout Result, Self.Element) async throws -> Void) async rethrows -> Result
        @available(macOS 10.15, watchOS 6.0, iOS 13.0, tvOS 13.0, *)
        func contains(where predicate: (Self.Element) async throws -> Bool) async rethrows -> Bool
        @available(macOS 10.15, watchOS 6.0, iOS 13.0, tvOS 13.0, *)
        func allSatisfy(_ predicate: (Self.Element) async throws -> Bool) async rethrows -> Bool
        @available(macOS 10.15, watchOS 6.0, iOS 13.0, tvOS 13.0, *)
        func first(where predicate: (Self.Element) async throws -> Bool) async rethrows -> Self.Element?
        @available(macOS 10.15, watchOS 6.0, iOS 13.0, tvOS 13.0, *)
        @warn_unqualified_access func min(by areInIncreasingOrder: (Self.Element, Self.Element) async throws -> Bool) async rethrows -> Self.Element?
        @available(macOS 10.15, watchOS 6.0, iOS 13.0, tvOS 13.0, *)
        @warn_unqualified_access func max(by areInIncreasingOrder: (Self.Element, Self.Element) async throws -> Bool) async rethrows -> Self.Element?
        @available(macOS 10.15, watchOS 6.0, iOS 13.0, tvOS 13.0, *)
        @preconcurrency func compactMap<ElementOfResult>(_ transform: @escaping @Sendable (Self.Element) async throws -> ElementOfResult?) -> AsyncThrowingCompactMapSequence<Self, ElementOfResult>
        @available(macOS 10.15, watchOS 6.0, iOS 13.0, tvOS 13.0, *)
        @preconcurrency func flatMap<SegmentOfResult>(_ transform: @escaping @Sendable (Self.Element) async throws -> SegmentOfResult) -> AsyncThrowingFlatMapSequence<Self, SegmentOfResult> where SegmentOfResult : AsyncSequence
        @available(macOS 10.15, watchOS 6.0, iOS 13.0, tvOS 13.0, *)
        @preconcurrency func map<Transformed>(_ transform: @escaping @Sendable (Self.Element) async throws -> Transformed) -> AsyncThrowingMapSequence<Self, Transformed>
    }

    /// A resulting row of a Cassandra query.
    public struct Row {
        /// Get column value as `Bool`.
        public func column(_ index: Int) -> Bool?
        /// Access column with the given `index`.
        public func column(_ index: Int) -> Column?
        /// Get column value as `Double`.
        public func column(_ index: Int) -> Double?
        /// Get column value as `Float32`.
        public func column(_ index: Int) -> Float32?
        /// Get column value as `UUID`.
        public func column(_ index: Int) -> Foundation.UUID?
        /// Get column value as `Int16`.
        public func column(_ index: Int) -> Int16?
        /// Get column value as `Int32`.
        public func column(_ index: Int) -> Int32?
        /// Get column value as `Int64`.
        public func column(_ index: Int) -> Int64?
        /// Get column value as `Int8`.
        public func column(_ index: Int) -> Int8?
        /// Get column value as `String`.
        public func column(_ index: Int) -> String?
        /// Get column value as ``TimeBasedUUID``.
        public func column(_ index: Int) -> TimeBasedUUID?
        /// Get column value as `UInt32`.
        public func column(_ index: Int) -> UInt32?
        /// Get column value as `[Double]`.
        public func column(_ index: Int) -> [Double]?
        /// Get column value as `[Float32]`.
        public func column(_ index: Int) -> [Float32]?
        public func column(_ index: Int) -> [Foundation.UUID : Bool]?
        public func column(_ index: Int) -> [Foundation.UUID : Double]?
        public func column(_ index: Int) -> [Foundation.UUID : Float32]?
        public func column(_ index: Int) -> [Foundation.UUID : Foundation.UUID]?
        public func column(_ index: Int) -> [Foundation.UUID : Int16]?
        public func column(_ index: Int) -> [Foundation.UUID : Int32]?
        public func column(_ index: Int) -> [Foundation.UUID : Int64]?
        public func column(_ index: Int) -> [Foundation.UUID : Int8]?
        public func column(_ index: Int) -> [Foundation.UUID : String]?
        /// Get column value as `[UUID]`.
        public func column(_ index: Int) -> [Foundation.UUID]?
        public func column(_ index: Int) -> [Int16 : Bool]?
        public func column(_ index: Int) -> [Int16 : Double]?
        public func column(_ index: Int) -> [Int16 : Float32]?
        public func column(_ index: Int) -> [Int16 : Foundation.UUID]?
        public func column(_ index: Int) -> [Int16 : Int16]?
        public func column(_ index: Int) -> [Int16 : Int32]?
        public func column(_ index: Int) -> [Int16 : Int64]?
        public func column(_ index: Int) -> [Int16 : Int8]?
        public func column(_ index: Int) -> [Int16 : String]?
        /// Get column value as `[Int16]`.
        public func column(_ index: Int) -> [Int16]?
        public func column(_ index: Int) -> [Int32 : Bool]?
        public func column(_ index: Int) -> [Int32 : Double]?
        public func column(_ index: Int) -> [Int32 : Float32]?
        public func column(_ index: Int) -> [Int32 : Foundation.UUID]?
        public func column(_ index: Int) -> [Int32 : Int16]?
        public func column(_ index: Int) -> [Int32 : Int32]?
        public func column(_ index: Int) -> [Int32 : Int64]?
        public func column(_ index: Int) -> [Int32 : Int8]?
        public func column(_ index: Int) -> [Int32 : String]?
        /// Get column value as `[Int32]`.
        public func column(_ index: Int) -> [Int32]?
        public func column(_ index: Int) -> [Int64 : Bool]?
        public func column(_ index: Int) -> [Int64 : Double]?
        public func column(_ index: Int) -> [Int64 : Float32]?
        public func column(_ index: Int) -> [Int64 : Foundation.UUID]?
        public func column(_ index: Int) -> [Int64 : Int16]?
        public func column(_ index: Int) -> [Int64 : Int32]?
        public func column(_ index: Int) -> [Int64 : Int64]?
        public func column(_ index: Int) -> [Int64 : Int8]?
        public func column(_ index: Int) -> [Int64 : String]?
        /// Get column value as `[Int64]`.
        public func column(_ index: Int) -> [Int64]?
        public func column(_ index: Int) -> [Int8 : Bool]?
        public func column(_ index: Int) -> [Int8 : Double]?
        public func column(_ index: Int) -> [Int8 : Float32]?
        public func column(_ index: Int) -> [Int8 : Foundation.UUID]?
        public func column(_ index: Int) -> [Int8 : Int16]?
        public func column(_ index: Int) -> [Int8 : Int32]?
        public func column(_ index: Int) -> [Int8 : Int64]?
        public func column(_ index: Int) -> [Int8 : Int8]?
        public func column(_ index: Int) -> [Int8 : String]?
        /// Get column value as `[Int8]`.
        public func column(_ index: Int) -> [Int8]?
        public func column(_ index: Int) -> [String : Bool]?
        public func column(_ index: Int) -> [String : Double]?
        public func column(_ index: Int) -> [String : Float32]?
        public func column(_ index: Int) -> [String : Foundation.UUID]?
        public func column(_ index: Int) -> [String : Int16]?
        public func column(_ index: Int) -> [String : Int32]?
        public func column(_ index: Int) -> [String : Int64]?
        public func column(_ index: Int) -> [String : Int8]?
        public func column(_ index: Int) -> [String : String]?
        /// Get column value as `[String]`.
        public func column(_ index: Int) -> [String]?
        public func column(_ index: Int) -> [TimeBasedUUID : Bool]?
        public func column(_ index: Int) -> [TimeBasedUUID : Double]?
        public func column(_ index: Int) -> [TimeBasedUUID : Float32]?
        public func column(_ index: Int) -> [TimeBasedUUID : Foundation.UUID]?
        public func column(_ index: Int) -> [TimeBasedUUID : Int16]?
        public func column(_ index: Int) -> [TimeBasedUUID : Int32]?
        public func column(_ index: Int) -> [TimeBasedUUID : Int64]?
        public func column(_ index: Int) -> [TimeBasedUUID : Int8]?
        public func column(_ index: Int) -> [TimeBasedUUID : String]?
        /// Get column value as `[UInt8]`.
        public func column(_ index: Int) -> [UInt8]?
        /// Get column value as `Bool`.
        public func column(_ name: String) -> Bool?
        /// Access column with the given `name`.
        public func column(_ name: String) -> Column?
        /// Get column value as `Double`.
        public func column(_ name: String) -> Double?
        /// Get column value as `Float32`.
        public func column(_ name: String) -> Float32?
        /// Get column value as `UUID`.
        public func column(_ name: String) -> Foundation.UUID?
        /// Get column value as `Int16`.
        public func column(_ name: String) -> Int16?
        /// Get column value as `Int32`.
        public func column(_ name: String) -> Int32?
        /// Get column value as `Int64`.
        public func column(_ name: String) -> Int64?
        /// Get column value as `Int8`.
        public func column(_ name: String) -> Int8?
        /// Get column value as `String`.
        public func column(_ name: String) -> String?
        /// Get column value as ``TimeBasedUUID``.
        public func column(_ name: String) -> TimeBasedUUID?
        /// Get column value as `UInt32`.
        public func column(_ name: String) -> UInt32?
        /// Get column value as `[Double]`.
        public func column(_ name: String) -> [Double]?
        /// Get column value as `[Float32]`.
        public func column(_ name: String) -> [Float32]?
        public func column(_ name: String) -> [Foundation.UUID : Bool]?
        public func column(_ name: String) -> [Foundation.UUID : Double]?
        public func column(_ name: String) -> [Foundation.UUID : Float32]?
        public func column(_ name: String) -> [Foundation.UUID : Foundation.UUID]?
        public func column(_ name: String) -> [Foundation.UUID : Int16]?
        public func column(_ name: String) -> [Foundation.UUID : Int32]?
        public func column(_ name: String) -> [Foundation.UUID : Int64]?
        public func column(_ name: String) -> [Foundation.UUID : Int8]?
        public func column(_ name: String) -> [Foundation.UUID : String]?
        /// Get column value as `[UUID]`.
        public func column(_ name: String) -> [Foundation.UUID]?
        public func column(_ name: String) -> [Int16 : Bool]?
        public func column(_ name: String) -> [Int16 : Double]?
        public func column(_ name: String) -> [Int16 : Float32]?
        public func column(_ name: String) -> [Int16 : Foundation.UUID]?
        public func column(_ name: String) -> [Int16 : Int16]?
        public func column(_ name: String) -> [Int16 : Int32]?
        public func column(_ name: String) -> [Int16 : Int64]?
        public func column(_ name: String) -> [Int16 : Int8]?
        public func column(_ name: String) -> [Int16 : String]?
        /// Get column value as `[Int16]`.
        public func column(_ name: String) -> [Int16]?
        public func column(_ name: String) -> [Int32 : Bool]?
        public func column(_ name: String) -> [Int32 : Double]?
        public func column(_ name: String) -> [Int32 : Float32]?
        public func column(_ name: String) -> [Int32 : Foundation.UUID]?
        public func column(_ name: String) -> [Int32 : Int16]?
        public func column(_ name: String) -> [Int32 : Int32]?
        public func column(_ name: String) -> [Int32 : Int64]?
        public func column(_ name: String) -> [Int32 : Int8]?
        public func column(_ name: String) -> [Int32 : String]?
        /// Get column value as `[Int32]`.
        public func column(_ name: String) -> [Int32]?
        public func column(_ name: String) -> [Int64 : Bool]?
        public func column(_ name: String) -> [Int64 : Double]?
        public func column(_ name: String) -> [Int64 : Float32]?
        public func column(_ name: String) -> [Int64 : Foundation.UUID]?
        public func column(_ name: String) -> [Int64 : Int16]?
        public func column(_ name: String) -> [Int64 : Int32]?
        public func column(_ name: String) -> [Int64 : Int64]?
        public func column(_ name: String) -> [Int64 : Int8]?
        public func column(_ name: String) -> [Int64 : String]?
        /// Get column value as `[Int64]`.
        public func column(_ name: String) -> [Int64]?
        public func column(_ name: String) -> [Int8 : Bool]?
        public func column(_ name: String) -> [Int8 : Double]?
        public func column(_ name: String) -> [Int8 : Float32]?
        public func column(_ name: String) -> [Int8 : Foundation.UUID]?
        public func column(_ name: String) -> [Int8 : Int16]?
        public func column(_ name: String) -> [Int8 : Int32]?
        public func column(_ name: String) -> [Int8 : Int64]?
        public func column(_ name: String) -> [Int8 : Int8]?
        public func column(_ name: String) -> [Int8 : String]?
        /// Get column value as `[Int8]`.
        public func column(_ name: String) -> [Int8]?
        public func column(_ name: String) -> [String : Bool]?
        public func column(_ name: String) -> [String : Double]?
        public func column(_ name: String) -> [String : Float32]?
        public func column(_ name: String) -> [String : Foundation.UUID]?
        public func column(_ name: String) -> [String : Int16]?
        public func column(_ name: String) -> [String : Int32]?
        public func column(_ name: String) -> [String : Int64]?
        public func column(_ name: String) -> [String : Int8]?
        public func column(_ name: String) -> [String : String]?
        /// Get column value as `[String]`.
        public func column(_ name: String) -> [String]?
        public func column(_ name: String) -> [TimeBasedUUID : Bool]?
        public func column(_ name: String) -> [TimeBasedUUID : Double]?
        public func column(_ name: String) -> [TimeBasedUUID : Float32]?
        public func column(_ name: String) -> [TimeBasedUUID : Foundation.UUID]?
        public func column(_ name: String) -> [TimeBasedUUID : Int16]?
        public func column(_ name: String) -> [TimeBasedUUID : Int32]?
        public func column(_ name: String) -> [TimeBasedUUID : Int64]?
        public func column(_ name: String) -> [TimeBasedUUID : Int8]?
        public func column(_ name: String) -> [TimeBasedUUID : String]?
        /// Get column value as `[UInt8]`.
        public func column(_ name: String) -> [UInt8]?
        /// Decrypt column by index and return as `[UInt8]`.
        public func decryptedBytes(_ index: Int, encryptor: CassandraClient.Encryptor, context: CassandraClient.EncryptionContext) throws -> [UInt8]?
        /// Decrypt column by name and return as `[UInt8]`.
        public func decryptedBytes(_ name: String, encryptor: CassandraClient.Encryptor, context: CassandraClient.EncryptionContext) throws -> [UInt8]?
        /// Decrypt column by index and return as `Date`.
        public func decryptedDate(_ index: Int, encryptor: CassandraClient.Encryptor, context: CassandraClient.EncryptionContext) throws -> Foundation.Date?
        /// Decrypt column by name and return as `Date`.
        public func decryptedDate(_ name: String, encryptor: CassandraClient.Encryptor, context: CassandraClient.EncryptionContext) throws -> Foundation.Date?
        /// Decrypt column by index and return as `Double`.
        public func decryptedDouble(_ index: Int, encryptor: CassandraClient.Encryptor, context: CassandraClient.EncryptionContext) throws -> Double?
        /// Decrypt column by name and return as `Double`.
        public func decryptedDouble(_ name: String, encryptor: CassandraClient.Encryptor, context: CassandraClient.EncryptionContext) throws -> Double?
        /// Decrypt column by index and return as `Int32`.
        public func decryptedInt32(_ index: Int, encryptor: CassandraClient.Encryptor, context: CassandraClient.EncryptionContext) throws -> Int32?
        /// Decrypt column by name and return as `Int32`.
        public func decryptedInt32(_ name: String, encryptor: CassandraClient.Encryptor, context: CassandraClient.EncryptionContext) throws -> Int32?
        /// Decrypt column by index and return as `Int64`.
        public func decryptedInt64(_ index: Int, encryptor: CassandraClient.Encryptor, context: CassandraClient.EncryptionContext) throws -> Int64?
        /// Decrypt column by name and return as `Int64`.
        public func decryptedInt64(_ name: String, encryptor: CassandraClient.Encryptor, context: CassandraClient.EncryptionContext) throws -> Int64?
        /// Decrypt column by index and return as `String`.
        public func decryptedString(_ index: Int, encryptor: CassandraClient.Encryptor, context: CassandraClient.EncryptionContext) throws -> String?
        /// Decrypt column by name and return as `String`.
        public func decryptedString(_ name: String, encryptor: CassandraClient.Encryptor, context: CassandraClient.EncryptionContext) throws -> String?
        /// Decrypt column by index and return as `UUID`.
        public func decryptedUUID(_ index: Int, encryptor: CassandraClient.Encryptor, context: CassandraClient.EncryptionContext) throws -> Foundation.UUID?
        /// Decrypt column by name and return as `UUID`.
        public func decryptedUUID(_ name: String, encryptor: CassandraClient.Encryptor, context: CassandraClient.EncryptionContext) throws -> Foundation.UUID?
    }

    /// Resulting row(s) of a Cassandra query. Data are returned all at once.
    public final class Rows : Sendable, Sequence {
        /// An asynchronous sequence containing the same elements as this sequence,
        /// but on which operations, such as `map` and `filter`, are
        /// implemented asynchronously.
        @available(macOS 10.15, watchOS 6.0, iOS 13.0, tvOS 13.0, *)
        public var async: AsyncSyncSequence<Self> { get }
        public var columnsCount: Int { get }
        public var count: Int { get }
        public var first: Row? { get }

        /// Get column name by index
        /// - Parameter index: The column index (0-based)
        /// - Returns: The column name
        /// - Throws: CassandraClient.Error if index is out of bounds or column name cannot be retrieved
        public func columnName(at index: Int) throws -> String
        /// Get all column names
        /// - Returns: Array of column names
        /// - Throws: CassandraClient.Error if any column name cannot be retrieved
        public func columnNames() throws -> [String]
        public func makeIterator() -> Iterator
        /// Returns a reusable paging token.
        ///
        /// The token is usable only within the process that produced it: the library provides
        /// no API for reading its bytes.
        public func opaquePagingStateToken() throws -> OpaquePagingStateToken

        public final class Iterator : IteratorProtocol {
            public typealias Element = Row
            public func next() -> Row?
        }

        // [inherited from Sequence] 38 members
        func shuffled<T>(using generator: inout T) -> [Self.Element] where T : RandomNumberGenerator
        func shuffled() -> [Self.Element]
        var lazy: LazySequence<Self> { get }
        @available(Swift, deprecated: 4.1, renamed: "compactMap(_:)")
        func flatMap<ElementOfResult>(_ transform: (Self.Element) throws -> ElementOfResult?) rethrows -> [ElementOfResult]
        func map<T, E>(_ transform: (Self.Element) throws(E) -> T) throws(E) -> [T] where E : Error
        func filter(_ isIncluded: (Self.Element) throws -> Bool) rethrows -> [Self.Element]
        var underestimatedCount: Int { get }
        func forEach(_ body: (Self.Element) throws -> Void) rethrows
        func first(where predicate: (Self.Element) throws -> Bool) rethrows -> Self.Element?
        func split(maxSplits: Int = Int.max, omittingEmptySubsequences: Bool = true, whereSeparator isSeparator: (Self.Element) throws -> Bool) rethrows -> [ArraySlice<Self.Element>]
        func suffix(_ maxLength: Int) -> [Self.Element]
        func dropFirst(_ k: Int = 1) -> DropFirstSequence<Self>
        func dropLast(_ k: Int = 1) -> [Self.Element]
        func drop(while predicate: (Self.Element) throws -> Bool) rethrows -> DropWhileSequence<Self>
        func prefix(_ maxLength: Int) -> PrefixSequence<Self>
        func prefix(while predicate: (Self.Element) throws -> Bool) rethrows -> [Self.Element]
        func withContiguousStorageIfAvailable<R>(_ body: (UnsafeBufferPointer<Self.Element>) throws -> R) rethrows -> R?
        func enumerated() -> EnumeratedSequence<Self>
        @warn_unqualified_access func min(by areInIncreasingOrder: (Self.Element, Self.Element) throws -> Bool) rethrows -> Self.Element?
        @warn_unqualified_access func max(by areInIncreasingOrder: (Self.Element, Self.Element) throws -> Bool) rethrows -> Self.Element?
        func starts<PossiblePrefix>(with possiblePrefix: PossiblePrefix, by areEquivalent: (Self.Element, PossiblePrefix.Element) throws -> Bool) rethrows -> Bool where PossiblePrefix : Sequence
        func elementsEqual<OtherSequence>(_ other: OtherSequence, by areEquivalent: (Self.Element, OtherSequence.Element) throws -> Bool) rethrows -> Bool where OtherSequence : Sequence
        func lexicographicallyPrecedes<OtherSequence>(_ other: OtherSequence, by areInIncreasingOrder: (Self.Element, Self.Element) throws -> Bool) rethrows -> Bool where OtherSequence : Sequence, Self.Element == OtherSequence.Element
        func contains(where predicate: (Self.Element) throws -> Bool) rethrows -> Bool
        func allSatisfy(_ predicate: (Self.Element) throws -> Bool) rethrows -> Bool
        func count<E>(where predicate: (Self.Element) throws(E) -> Bool) throws(E) -> Int where E : Error
        func reduce<Result>(_ initialResult: Result, _ nextPartialResult: (Result, Self.Element) throws -> Result) rethrows -> Result
        func reduce<Result>(into initialResult: Result, _ updateAccumulatingResult: (inout Result, Self.Element) throws -> ()) rethrows -> Result
        func reversed() -> [Self.Element]
        func flatMap<SegmentOfResult>(_ transform: (Self.Element) throws -> SegmentOfResult) rethrows -> [SegmentOfResult.Element] where SegmentOfResult : Sequence
        func compactMap<ElementOfResult>(_ transform: (Self.Element) throws -> ElementOfResult?) rethrows -> [ElementOfResult]
        func sorted(by areInIncreasingOrder: (Self.Element, Self.Element) throws -> Bool) rethrows -> [Self.Element]
        @available(macOS 10.15, watchOS 6.0, iOS 13.0, tvOS 13.0, *)
        var publisher: Publishers.Sequence<Self, Never> { get }
        @available(macOS 14, watchOS 10, iOS 17, tvOS 17, *)
        func filter(_ predicate: Predicate<Self.Element>) throws -> [Self.Element]
        @available(macOS 12.0, watchOS 8.0, iOS 15.0, tvOS 15.0, *)
        func formatted<S>(_ style: S) -> S.FormatOutput where Self == S.FormatInput, S : FormatStyle
        @available(macOS 12.0, watchOS 8.0, iOS 15.0, tvOS 15.0, *)
        func sorted<Comparator>(using comparator: Comparator) -> [Self.Element] where Comparator : SortComparator, Self.Element == Comparator.Compared
        @available(macOS 12.0, watchOS 8.0, iOS 15.0, tvOS 15.0, *)
        func sorted<S, Comparator>(using comparators: S) -> [Self.Element] where S : Sequence, Comparator : SortComparator, Comparator == S.Element, Self.Element == Comparator.Compared
        @available(macOS 12.0, watchOS 8.0, iOS 15.0, tvOS 15.0, *)
        func compare<Comparator>(_ lhs: Comparator.Compared, _ rhs: Comparator.Compared) -> ComparisonResult where Comparator : SortComparator, Comparator == Self.Element
    }
}
````
