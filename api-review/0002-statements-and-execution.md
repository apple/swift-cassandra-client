# Statements and execution

## Overview

- **Area:** Statements and execution — 152 of the module's public symbols.
- **Note:** This PR exists to collect API review feedback ahead of 1.0. It will be closed rather than merged; anything it settles lands as its own change.
- **Generated from:** `Sources/CassandraClient` at `e60f45a`, with Apple Swift version 6.3.1 (swiftlang-6.3.1.1.2 clang-2100.0.123.102). Derived, not maintained by hand — see `api-review/README.md` to regenerate.

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
        @available(macOS 15.0, iOS 18.0, visionOS 2.0, *)
        public mutating func add(prepared: PreparedStatement, parameters: [Statement.Value], options: Statement.Options = .init()) throws
        /// Add a raw statement to this batch. Use this for non-prepared CQL statements only.
        /// For prepared statements, use ``add(prepared:parameters:options:)`` instead.
        public mutating func add(statement: Statement) throws

        /// Batch configuration options.
        public struct Configuration : Sendable {
            public init()

            /// The batch's consistency level.
            public var consistency: CassandraClient.Consistency?
            /// Whether the batch is idempotent.
            public var isIdempotent: Bool?
            /// The keyspace for the batch.
            public var keyspace: String?
            /// The batch's request timeout in milliseconds.
            public var requestTimeout: UInt64?
            /// The batch's serial consistency level for conditional updates.
            public var serialConsistency: CassandraClient.SerialConsistency?
            /// The batch's write timestamp.
            public var timestamp: Foundation.Date?
            /// Whether tracing is enabled for this batch.
            public var tracing: Bool?
            /// The batch type. Defaults to `.logged`.
            public var type: CassandraClient.BatchType
        }
    }

    /// The type of a batch operation.
    public struct BatchType : Hashable, Sendable {
        /// All statements must be counter updates.
        public static let counter: CassandraClient.BatchType
        /// All statements are applied atomically with a write to the batch log.
        public static let logged: CassandraClient.BatchType
        /// Statements are applied without atomicity guarantees.
        public static let unlogged: CassandraClient.BatchType

        // [inherited from Equatable] 1 member
        static func != (lhs: Self, rhs: Self) -> Bool

        // [reconstructed] 1 member guaranteed by a conformance above that the symbol graph does not emit
        public static func == (lhs: Self, rhs: Self) -> Bool
    }

    /// Consistency levels
    public enum Consistency : Hashable, RawRepresentable, Sendable {
        case all
        case any
        case eachQuorum
        case localOne
        case localQuorum
        case localSerial
        case one
        case quorum
        case serial
        case three
        case two

        // [inherited from Equatable] 1 member
        static func != (lhs: Self, rhs: Self) -> Bool

        // [inherited from RawRepresentable] 2 members
        var hashValue: Int { get }
        func hash(into hasher: inout Hasher)

        // [compiler-synthesized] 1 member
        init?(rawValue: String)

        // [reconstructed] 1 member guaranteed by a conformance above that the symbol graph does not emit
        public static func == (lhs: Self, rhs: Self) -> Bool
    }

    /// A server-side prepared statement that can be efficiently executed multiple times with different parameters.
    public final class PreparedStatement : Sendable {
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
    public enum SerialConsistency : Hashable, RawRepresentable, Sendable {
        case localSerial
        case serial

        // [inherited from Equatable] 1 member
        static func != (lhs: Self, rhs: Self) -> Bool

        // [inherited from RawRepresentable] 2 members
        var hashValue: Int { get }
        func hash(into hasher: inout Hasher)

        // [compiler-synthesized] 1 member
        init?(rawValue: String)

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
        ///
        /// - Note: The `EventLoopFuture` `execute` takes the statement as `sending`, so a hand-rolled
        ///   loop needs a new `Statement` for each page.
        public func setPagingSize(_ pagingSize: Int) throws
        /// Sets the starting page of the returned paginated results.
        ///
        /// The paging state token can be obtained by
        /// ``CassandraClient/Rows/opaquePagingStateToken()``.
        public func setPagingStateToken(_ pagingStateToken: OpaquePagingStateToken) throws

        public struct Options : CustomStringConvertible, Sendable {
            public init(consistency: CassandraClient.Consistency? = nil, requestTimeout: UInt64? = nil)

            /// Sets the statement's consistency level. `nil` inherits
            /// ``CassandraClient/Configuration/consistency``.
            public var consistency: CassandraClient.Consistency?
            public var description: String { get }
            /// Closure that extracts encryption context from each row during Codable decoding.
            @available(macOS 15.0, iOS 18.0, visionOS 2.0, *)
            public var encryptionContextBuilder: (@Sendable (CassandraClient.Row) throws -> CassandraClient.EncryptionContext.Base)? { get set }
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
            @available(macOS 15.0, iOS 18.0, visionOS 2.0, *)
            public var encryptionTable: String? { get set }
            /// Sets the statement's request timeout in milliseconds. `nil` inherits
            /// ``CassandraClient/Configuration/requestTimeoutMillis``.
            public var requestTimeout: UInt64?
        }

        /// Value types
        public enum Value {
            case bool(Bool)
            case bytes([UInt8])
            case bytesUnsafe(UnsafeBufferPointer<UInt8>)
            case date(Foundation.Date)
            case double(Double)
            case doubleArray([Double])
            case encryptedBytes(Encrypted<[UInt8]>, context: EncryptionContext? = nil)
            case encryptedDate(Encrypted<Foundation.Date>, context: EncryptionContext? = nil)
            case encryptedDouble(Encrypted<Double>, context: EncryptionContext? = nil)
            case encryptedInt32(Encrypted<Int32>, context: EncryptionContext? = nil)
            case encryptedInt64(Encrypted<Int64>, context: EncryptionContext? = nil)
            case encryptedString(Encrypted<String>, context: EncryptionContext? = nil)
            case encryptedUUID(Encrypted<Foundation.UUID>, context: EncryptionContext? = nil)
            case float32(Float32)
            case float32Array([Float32])
            case int16(Int16)
            case int16Array([Int16])
            case int16BoolMap([Int16 : Bool])
            case int16DoubleMap([Int16 : Double])
            case int16Float32Map([Int16 : Float32])
            case int16Int16Map([Int16 : Int16])
            case int16Int32Map([Int16 : Int32])
            case int16Int64Map([Int16 : Int64])
            case int16Int8Map([Int16 : Int8])
            case int16StringMap([Int16 : String])
            case int16UUIDMap([Int16 : Foundation.UUID])
            case int32(Int32)
            case int32Array([Int32])
            case int32BoolMap([Int32 : Bool])
            case int32DoubleMap([Int32 : Double])
            case int32Float32Map([Int32 : Float32])
            case int32Int16Map([Int32 : Int16])
            case int32Int32Map([Int32 : Int32])
            case int32Int64Map([Int32 : Int64])
            case int32Int8Map([Int32 : Int8])
            case int32StringMap([Int32 : String])
            case int32UUIDMap([Int32 : Foundation.UUID])
            case int64(Int64)
            case int64Array([Int64])
            case int64BoolMap([Int64 : Bool])
            case int64DoubleMap([Int64 : Double])
            case int64Float32Map([Int64 : Float32])
            case int64Int16Map([Int64 : Int16])
            case int64Int32Map([Int64 : Int32])
            case int64Int64Map([Int64 : Int64])
            case int64Int8Map([Int64 : Int8])
            case int64StringMap([Int64 : String])
            case int64UUIDMap([Int64 : Foundation.UUID])
            case int8(Int8)
            case int8Array([Int8])
            case int8BoolMap([Int8 : Bool])
            case int8DoubleMap([Int8 : Double])
            case int8Float32Map([Int8 : Float32])
            case int8Int16Map([Int8 : Int16])
            case int8Int32Map([Int8 : Int32])
            case int8Int64Map([Int8 : Int64])
            case int8Int8Map([Int8 : Int8])
            case int8StringMap([Int8 : String])
            case int8UUIDMap([Int8 : Foundation.UUID])
            case null
            case rawDate(daysSinceEpoch: UInt32)
            case rawTimestamp(millisecondsSinceEpoch: Int64)
            case string(String)
            case stringArray([String])
            case stringBoolMap([String : Bool])
            case stringDoubleMap([String : Double])
            case stringFloat32Map([String : Float32])
            case stringInt16Map([String : Int16])
            case stringInt32Map([String : Int32])
            case stringInt64Map([String : Int64])
            case stringInt8Map([String : Int8])
            case stringStringMap([String : String])
            case stringUUIDMap([String : Foundation.UUID])
            case timeuuid(TimeBasedUUID)
            case timeuuidBoolMap([TimeBasedUUID : Bool])
            case timeuuidDoubleMap([TimeBasedUUID : Double])
            case timeuuidFloat32Map([TimeBasedUUID : Float32])
            case timeuuidInt16Map([TimeBasedUUID : Int16])
            case timeuuidInt32Map([TimeBasedUUID : Int32])
            case timeuuidInt64Map([TimeBasedUUID : Int64])
            case timeuuidInt8Map([TimeBasedUUID : Int8])
            case timeuuidStringMap([TimeBasedUUID : String])
            case timeuuidUUIDMap([TimeBasedUUID : Foundation.UUID])
            case uuid(Foundation.UUID)
            case uuidArray([Foundation.UUID])
            case uuidBoolMap([Foundation.UUID : Bool])
            case uuidDoubleMap([Foundation.UUID : Double])
            case uuidFloat32Map([Foundation.UUID : Float32])
            case uuidInt16Map([Foundation.UUID : Int16])
            case uuidInt32Map([Foundation.UUID : Int32])
            case uuidInt64Map([Foundation.UUID : Int64])
            case uuidInt8Map([Foundation.UUID : Int8])
            case uuidStringMap([Foundation.UUID : String])
            case uuidUUIDMap([Foundation.UUID : Foundation.UUID])
        }
    }
}
````
