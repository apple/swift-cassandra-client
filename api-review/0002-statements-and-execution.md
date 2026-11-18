# Statements and execution

## Overview

- **Area:** Statements and execution — 158 of the module's public symbols.
- **Note:** This PR exists to collect API review feedback ahead of 1.0. It will be closed rather than merged; anything it settles lands as its own change.
- **Generated from:** `Sources/CassandraClient` at `3780cda`, with Apple Swift version 6.3.1 (swiftlang-6.3.1.1.2 clang-2100.0.123.102). Derived, not maintained by hand — see `api-review/README.md` to regenerate.

Inherited protocol members are listed without their standard-library documentation, grouped under the protocol that declared each. Members the symbol graph does not emit are marked `[reconstructed]`.

## Public API

````swift
extension CassandraClient {
    /// A batch of statements to execute in Cassandra.
    ///
    /// Not `Sendable`: a `Batch` must not be used concurrently, and `~Copyable` enforces single
    /// ownership so it cannot be reused after execution.
    public struct Batch : ~Copyable {
        /// Add a prepared statement with parameters to this batch.
        /// Handles encryption context resolution automatically when encryption is configured.
        public mutating func add(prepared: PreparedStatement, parameters: [Statement.Value], options: Statement.Options = .init()) throws
        /// Add a raw statement to this batch. Use this for non-prepared CQL statements only.
        /// For prepared statements, use ``add(prepared:parameters:options:)`` instead.
        public mutating func add(statement: Statement) throws

        /// Batch configuration options.
        public struct Configuration : Sendable {
            public init()

            /// The batch's consistency level.
            public var consistency: CassandraClient.Consistency?
            /// Whether every statement in the batch is safe to apply more than once. When `true`, the driver
            /// may retry the batch or execute it speculatively. Default `false`.
            public var isIdempotent: Bool
            /// The keyspace for the batch.
            public var keyspace: String?
            /// The batch's request timeout, rounded up to whole milliseconds. `nil` inherits
            /// ``CassandraClient/Configuration/requestTimeout``.
            public var requestTimeout: Duration?
            /// The batch's serial consistency level for conditional updates.
            public var serialConsistency: CassandraClient.SerialConsistency?
            /// The batch's write timestamp.
            public var timestamp: Foundation.Date?
            /// Whether tracing is enabled for this batch. Default `false`.
            public var tracing: Bool
            /// The batch kind. Defaults to `.logged`.
            public var type: Kind
        }

        /// The kind of a batch operation.
        public struct Kind : Hashable, Sendable {
            /// All statements must be counter updates.
            public static var counter: `Self` { get }
            /// All statements are applied atomically with a write to the batch log.
            public static var logged: `Self` { get }
            /// Statements are applied without atomicity guarantees.
            public static var unlogged: `Self` { get }

            // [inherited from Equatable] 1 member
            static func != (lhs: Self, rhs: Self) -> Bool

            // [reconstructed] 1 member guaranteed by a conformance above that the symbol graph does not emit
            public static func == (lhs: Self, rhs: Self) -> Bool
        }
    }

    /// Consistency levels
    public struct Consistency : CustomStringConvertible, Hashable, RawRepresentable, Sendable {
        public init?(rawValue: String)

        public static var all: `Self` { get }
        public static var any: `Self` { get }
        public var description: String { get }
        public static var eachQuorum: `Self` { get }
        public static var localOne: `Self` { get }
        public static var localQuorum: `Self` { get }
        public static var localSerial: `Self` { get }
        public static var one: `Self` { get }
        public static var quorum: `Self` { get }
        public var rawValue: String { get }
        public static var serial: `Self` { get }
        public static var three: `Self` { get }
        public static var two: `Self` { get }

        // [inherited from Equatable] 1 member
        static func != (lhs: Self, rhs: Self) -> Bool

        // [inherited from RawRepresentable] 2 members
        var hashValue: Int { get }
        func hash(into hasher: inout Hasher)

        // [reconstructed] 1 member guaranteed by a conformance above that the symbol graph does not emit
        public static func == (lhs: Self, rhs: Self) -> Bool
    }

    /// A server-side prepared statement that can be efficiently executed multiple times with different parameters.
    public struct PreparedStatement : Sendable {
        /// The table name for encryption context resolution.
        public let encryptionTable: String?
        /// The number of bind parameters in this prepared statement.
        public var parameterCount: Int { get }

        /// Gets the column name for the parameter at the given index.
        ///
        /// - Parameter index: Zero-based parameter index.
        /// - Returns: The column name, or `nil` if the index is out of bounds.
        public func parameterName(at index: Int) -> String?
    }

    /// Serial consistency levels
    public struct SerialConsistency : CustomStringConvertible, Hashable, RawRepresentable, Sendable {
        public init?(rawValue: String)

        public var description: String { get }
        public static var localSerial: `Self` { get }
        public var rawValue: String { get }
        public static var serial: `Self` { get }

        // [inherited from Equatable] 1 member
        static func != (lhs: Self, rhs: Self) -> Bool

        // [inherited from RawRepresentable] 2 members
        var hashValue: Int { get }
        func hash(into hasher: inout Hasher)

        // [reconstructed] 1 member guaranteed by a conformance above that the symbol graph does not emit
        public static func == (lhs: Self, rhs: Self) -> Bool
    }

    /// A prepared statement to run in a Cassandra database.
    ///
    /// Not `Sendable`: a `Statement` must not be used concurrently, or reused/mutated until a prior
    /// `execute` has completed (the driver reads it asynchronously while the request is in flight).
    public final class Statement : CustomStringConvertible {
        /// Create a new `Statement`.
        public convenience init(query: String, parameters: [Value] = [], options: Options = .init()) throws

        public var description: String { get }

        /// Sets the maximum number of rows the server returns per page for this statement.
        ///
        /// This bounds the `execute` variants that take a `Statement` and no `pageSize`, which
        /// otherwise return every row: the result becomes a single page, and pairing this with
        /// ``setPagingStateToken(_:)`` walks the rest by hand. The `execute` variants that do take a
        /// `pageSize` set it from that argument, overwriting whatever is set here. The `query`
        /// variants build their own statement, so this never reaches them.
        ///
        /// - Parameter pagingSize: Rows per page. Must be in `1...Int32.max`; a non-positive size
        ///   disables paging in the driver rather than limiting the page, and is rejected here.
        public func setPagingSize(_ pagingSize: Int) throws
        /// Sets the starting page of the returned paginated results.
        ///
        /// The paging state token can be obtained by
        /// ``CassandraClient/Rows/opaquePagingStateToken()``.
        public func setPagingStateToken(_ pagingStateToken: OpaquePagingStateToken) throws

        public struct Options : CustomStringConvertible, Sendable {
            public init(consistency: CassandraClient.Consistency? = nil, requestTimeout: Duration? = nil, isIdempotent: Bool = false)

            /// Sets the statement's consistency level. `nil` inherits
            /// ``CassandraClient/Configuration/consistency``.
            public var consistency: CassandraClient.Consistency?
            public var description: String { get }
            /// Closure that extracts encryption context from each row during Codable decoding.
            public var encryptionContextBuilder: (@Sendable (CassandraClient.Row) throws -> CassandraClient.EncryptionContext.Base)?
            /// Table name for column-registration-based automatic decryption.
            /// Use `"table"` (combined with the session keyspace) or `"keyspace.table"` for cross-keyspace queries.
            /// When set and a matching ``EncryptionSchema`` is registered, the decoder builds
            /// ``EncryptionContext/Base`` automatically. ``encryptionContextBuilder`` takes precedence if both are set.
            ///
            /// - Important: The query must SELECT all primary key columns registered in the schema.
            ///   The decoder reads these columns from each result row to build the ``PrimaryKey`` for key derivation.
            ///   Omitting a key column will cause decryption to fail at runtime.
            ///
            /// - Important: What binding validation enforces depends on the execution path.
            ///   Prepared statement execution maps each parameter to its column via statement metadata
            ///   and checks both directions: a registered column must receive an encrypted value, and an
            ///   unregistered one must not. Other execution paths have no parameter column names, so they
            ///   check only that each encrypted value's ``EncryptionContext`` names a registered column —
            ///   a plaintext value bound to a registered column is written as-is. Neither path verifies
            ///   that a value's context names the column it is actually bound to.
            public var encryptionTable: String?
            /// Whether the statement is safe to run more than once. Default `false`.
            ///
            /// A statement not marked idempotent is treated as unsafe to replay, which holds the driver back
            /// from two recovery behaviors:
            ///
            /// - When a coordinator reports that it is overloaded, shutting down, or hit an internal
            ///   error, the driver hands the request to the next host in its query plan instead of
            ///   failing the call. Without this, a single node draining during a rolling restart
            ///   surfaces as an error even though the rest of the cluster can serve the request.
            /// - Speculative execution, configured by
            ///   ``CassandraClient/Configuration/speculativeExecutionPolicy``, has no effect at all
            ///   until a statement is marked safe to replay.
            ///
            /// - Important: Only mark a statement idempotent when running it twice leaves the same
            ///   result as running it once. Counter updates, lightweight transactions, and appends to
            ///   collections are not idempotent, and replaying one corrupts the value it touches.
            ///   Plain reads, and writes that set fixed values, are.
            ///
            /// - Note: This has no effect on a statement added to a ``CassandraClient/Batch``. The
            ///   driver decides whether to replay a batch from the batch's own setting, so use
            ///   ``CassandraClient/Batch/Configuration/isIdempotent`` for that.
            public var isIdempotent: Bool
            /// Sets the statement's request timeout, rounded up to whole milliseconds. `nil` inherits
            /// ``CassandraClient/Configuration/requestTimeout``.
            public var requestTimeout: Duration?
        }

        /// Value types
        public struct Value : CustomStringConvertible {
            public var description: String { get }
            public static var null: Value { get }

            public static func bool(_ value: Bool) -> Value
            public static func bytes(_ value: [UInt8]) -> Value
            public static func bytesUnsafe(_ value: UnsafeBufferPointer<UInt8>) -> Value
            public static func date(_ value: Foundation.Date) -> Value
            public static func double(_ value: Double) -> Value
            public static func doubleArray(_ value: [Double]) -> Value
            public static func encryptedBytes(_ value: Encrypted<[UInt8]>, context: EncryptionContext? = nil) -> Value
            public static func encryptedDate(_ value: Encrypted<Foundation.Date>, context: EncryptionContext? = nil) -> Value
            public static func encryptedDouble(_ value: Encrypted<Double>, context: EncryptionContext? = nil) -> Value
            public static func encryptedInt32(_ value: Encrypted<Int32>, context: EncryptionContext? = nil) -> Value
            public static func encryptedInt64(_ value: Encrypted<Int64>, context: EncryptionContext? = nil) -> Value
            public static func encryptedString(_ value: Encrypted<String>, context: EncryptionContext? = nil) -> Value
            public static func encryptedUUID(_ value: Encrypted<Foundation.UUID>, context: EncryptionContext? = nil) -> Value
            public static func float32(_ value: Float32) -> Value
            public static func float32Array(_ value: [Float32]) -> Value
            public static func int16(_ value: Int16) -> Value
            public static func int16Array(_ value: [Int16]) -> Value
            public static func int16BoolMap(_ value: [Int16 : Bool]) -> Value
            public static func int16DoubleMap(_ value: [Int16 : Double]) -> Value
            public static func int16Float32Map(_ value: [Int16 : Float32]) -> Value
            public static func int16Int16Map(_ value: [Int16 : Int16]) -> Value
            public static func int16Int32Map(_ value: [Int16 : Int32]) -> Value
            public static func int16Int64Map(_ value: [Int16 : Int64]) -> Value
            public static func int16Int8Map(_ value: [Int16 : Int8]) -> Value
            public static func int16StringMap(_ value: [Int16 : String]) -> Value
            public static func int16UUIDMap(_ value: [Int16 : Foundation.UUID]) -> Value
            public static func int32(_ value: Int32) -> Value
            public static func int32Array(_ value: [Int32]) -> Value
            public static func int32BoolMap(_ value: [Int32 : Bool]) -> Value
            public static func int32DoubleMap(_ value: [Int32 : Double]) -> Value
            public static func int32Float32Map(_ value: [Int32 : Float32]) -> Value
            public static func int32Int16Map(_ value: [Int32 : Int16]) -> Value
            public static func int32Int32Map(_ value: [Int32 : Int32]) -> Value
            public static func int32Int64Map(_ value: [Int32 : Int64]) -> Value
            public static func int32Int8Map(_ value: [Int32 : Int8]) -> Value
            public static func int32StringMap(_ value: [Int32 : String]) -> Value
            public static func int32UUIDMap(_ value: [Int32 : Foundation.UUID]) -> Value
            public static func int64(_ value: Int64) -> Value
            public static func int64Array(_ value: [Int64]) -> Value
            public static func int64BoolMap(_ value: [Int64 : Bool]) -> Value
            public static func int64DoubleMap(_ value: [Int64 : Double]) -> Value
            public static func int64Float32Map(_ value: [Int64 : Float32]) -> Value
            public static func int64Int16Map(_ value: [Int64 : Int16]) -> Value
            public static func int64Int32Map(_ value: [Int64 : Int32]) -> Value
            public static func int64Int64Map(_ value: [Int64 : Int64]) -> Value
            public static func int64Int8Map(_ value: [Int64 : Int8]) -> Value
            public static func int64StringMap(_ value: [Int64 : String]) -> Value
            public static func int64UUIDMap(_ value: [Int64 : Foundation.UUID]) -> Value
            public static func int8(_ value: Int8) -> Value
            public static func int8Array(_ value: [Int8]) -> Value
            public static func int8BoolMap(_ value: [Int8 : Bool]) -> Value
            public static func int8DoubleMap(_ value: [Int8 : Double]) -> Value
            public static func int8Float32Map(_ value: [Int8 : Float32]) -> Value
            public static func int8Int16Map(_ value: [Int8 : Int16]) -> Value
            public static func int8Int32Map(_ value: [Int8 : Int32]) -> Value
            public static func int8Int64Map(_ value: [Int8 : Int64]) -> Value
            public static func int8Int8Map(_ value: [Int8 : Int8]) -> Value
            public static func int8StringMap(_ value: [Int8 : String]) -> Value
            public static func int8UUIDMap(_ value: [Int8 : Foundation.UUID]) -> Value
            public static func rawDate(daysSinceEpoch: UInt32) -> Value
            public static func rawTimestamp(millisecondsSinceEpoch: Int64) -> Value
            public static func string(_ value: String) -> Value
            public static func stringArray(_ value: [String]) -> Value
            public static func stringBoolMap(_ value: [String : Bool]) -> Value
            public static func stringDoubleMap(_ value: [String : Double]) -> Value
            public static func stringFloat32Map(_ value: [String : Float32]) -> Value
            public static func stringInt16Map(_ value: [String : Int16]) -> Value
            public static func stringInt32Map(_ value: [String : Int32]) -> Value
            public static func stringInt64Map(_ value: [String : Int64]) -> Value
            public static func stringInt8Map(_ value: [String : Int8]) -> Value
            public static func stringStringMap(_ value: [String : String]) -> Value
            public static func stringUUIDMap(_ value: [String : Foundation.UUID]) -> Value
            public static func timeuuid(_ value: TimeBasedUUID) -> Value
            public static func timeuuidBoolMap(_ value: [TimeBasedUUID : Bool]) -> Value
            public static func timeuuidDoubleMap(_ value: [TimeBasedUUID : Double]) -> Value
            public static func timeuuidFloat32Map(_ value: [TimeBasedUUID : Float32]) -> Value
            public static func timeuuidInt16Map(_ value: [TimeBasedUUID : Int16]) -> Value
            public static func timeuuidInt32Map(_ value: [TimeBasedUUID : Int32]) -> Value
            public static func timeuuidInt64Map(_ value: [TimeBasedUUID : Int64]) -> Value
            public static func timeuuidInt8Map(_ value: [TimeBasedUUID : Int8]) -> Value
            public static func timeuuidStringMap(_ value: [TimeBasedUUID : String]) -> Value
            public static func timeuuidUUIDMap(_ value: [TimeBasedUUID : Foundation.UUID]) -> Value
            public static func uuid(_ value: Foundation.UUID) -> Value
            public static func uuidArray(_ value: [Foundation.UUID]) -> Value
            public static func uuidBoolMap(_ value: [Foundation.UUID : Bool]) -> Value
            public static func uuidDoubleMap(_ value: [Foundation.UUID : Double]) -> Value
            public static func uuidFloat32Map(_ value: [Foundation.UUID : Float32]) -> Value
            public static func uuidInt16Map(_ value: [Foundation.UUID : Int16]) -> Value
            public static func uuidInt32Map(_ value: [Foundation.UUID : Int32]) -> Value
            public static func uuidInt64Map(_ value: [Foundation.UUID : Int64]) -> Value
            public static func uuidInt8Map(_ value: [Foundation.UUID : Int8]) -> Value
            public static func uuidStringMap(_ value: [Foundation.UUID : String]) -> Value
            public static func uuidUUIDMap(_ value: [Foundation.UUID : Foundation.UUID]) -> Value
        }
    }
}
````
