//===----------------------------------------------------------------------===//
//
// This source file is part of the Swift Cassandra Client open source project
//
// Copyright (c) 2022-2023 Apple Inc. and the Swift Cassandra Client project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of Swift Cassandra Client project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import Foundation
import Logging
import NIO
import XCTest

@testable import CassandraClient

final class Tests: XCTestCase {
    var cassandraClient: CassandraClient!
    var configuration: CassandraClient.Configuration!

    override func setUp() {
        super.setUp()

        let env = ProcessInfo.processInfo.environment
        let keyspace = env["CASSANDRA_KEYSPACE"] ?? "test"
        self.configuration = CassandraClient.Configuration(
            contactPointsProvider: { callback in
                callback(.success([env["CASSANDRA_HOST"] ?? "127.0.0.1"]))
            },
            port: env["CASSANDRA_CQL_PORT"].flatMap(Int32.init) ?? 9042,
            protocolVersion: .v3
        )
        self.configuration.username = env["CASSANDRA_USER"]
        self.configuration.password = env["CASSANDRA_PASSWORD"]
        self.configuration.keyspace = keyspace
        self.configuration.requestTimeoutMillis = UInt32(24_000)  // Default: 12_000 ms
        self.configuration.connectTimeoutMillis = UInt32(10_000)  // Default: 5_000 ms

        var logger = Logger(label: "test")
        logger.logLevel = .debug

        // client for the tests
        self.cassandraClient = CassandraClient(configuration: self.configuration, logger: logger)
        // keyspace for the tests
        XCTAssertNoThrow(
            try self.cassandraClient.withSession(keyspace: .none) { session in
                try session
                    .run(
                        "create keyspace if not exists \(keyspace) with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"
                    )
                    .wait()
            }
        )
    }

    override func tearDown() {
        super.tearDown()

        XCTAssertNoThrow(try self.cassandraClient.shutdown())
        self.cassandraClient = nil  // FIXME: for tsan
    }

    func testSession() {
        let session = self.cassandraClient.makeSession(keyspace: self.configuration.keyspace)
        defer { XCTAssertNoThrow(try session.shutdown()) }

        let tableName = "test_\(DispatchTime.now().uptimeNanoseconds)"
        XCTAssertNoThrow(try session.run("create table \(tableName) (data bigint primary key);").wait())

        let count = Int.random(in: 10...100)
        for index in 0..<count {
            XCTAssertNoThrow(try session.run("insert into \(tableName) (data) values (\(index));").wait())
        }

        let result = try! session.query("select * from \(tableName);").wait()
        XCTAssertEqual(Array(result).count, count)
    }

    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    func testAsyncSession() throws {
        runAsyncAndWaitFor {
            let session = self.cassandraClient.makeSession(keyspace: self.configuration.keyspace)
            defer { XCTAssertNoThrow(try session.shutdown()) }

            let tableName = "test_\(DispatchTime.now().uptimeNanoseconds)"
            try await session.run("create table \(tableName) (data bigint primary key);")

            let count = Int.random(in: 10...100)
            await withThrowingTaskGroup(of: Void.self) { group in
                for index in 0..<count {
                    group.addTask {
                        try await session.run("insert into \(tableName) (data) values (\(index));")
                    }
                }
            }

            let result = try await session.query("select * from \(tableName);")
            XCTAssertEqual(Array(result).count, count)
        }
    }

    func testWithSessionBlocking() {
        var configuration = self.configuration!
        configuration.keyspace = "test_\(DispatchTime.now().uptimeNanoseconds)"
        let cassandraClient = CassandraClient(configuration: configuration)
        defer { XCTAssertNoThrow(try cassandraClient.shutdown()) }

        XCTAssertNoThrow(
            try cassandraClient.withSession(keyspace: .none) { session in
                try session.run(
                    "create keyspace \(configuration.keyspace!) with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"
                ).wait()
            }
        )
        XCTAssertNoThrow(try cassandraClient.run("create table test (data bigint primary key);").wait())
    }

    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    func testWithAsyncSession() throws {
        runAsyncAndWaitFor {
            var configuration = self.configuration!
            configuration.keyspace = "test_\(DispatchTime.now().uptimeNanoseconds)"
            let cassandraClient = CassandraClient(configuration: configuration)
            defer { XCTAssertNoThrow(try cassandraClient.shutdown()) }

            try await cassandraClient.withSession(keyspace: .none) { session in
                try await session.run(
                    "create keyspace \(configuration.keyspace!) with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"
                )
            }
            try await cassandraClient.run("create table test (data bigint primary key);")
        }
    }

    func testWithSessionChaining() {
        var configuration = self.configuration!
        configuration.keyspace = "test_\(DispatchTime.now().uptimeNanoseconds)"
        let cassandraClient = CassandraClient(configuration: configuration)
        defer { XCTAssertNoThrow(try cassandraClient.shutdown()) }
        XCTAssertNoThrow(
            try cassandraClient.withSession(keyspace: .none) { session in
                session.run(
                    "create keyspace \(configuration.keyspace!) with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"
                )
            }.flatMap { _ in
                cassandraClient.run("create table test (data bigint primary key);")
            }.wait()
        )
    }

    func testShutdownELGManaged() {
        let cassandraClient = CassandraClient(configuration: configuration)
        let tableName = "test_\(DispatchTime.now().uptimeNanoseconds)"
        XCTAssertNoThrow(
            try cassandraClient.run("create table \(tableName) (id int primary key);").wait()
        )
        XCTAssertNoThrow(try cassandraClient.shutdown())
        XCTAssertNoThrow(try cassandraClient.shutdown())
    }

    func testShutdownELGShared() {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }

        let cassandraClient = CassandraClient(
            eventLoopGroupProvider: .shared(eventLoopGroup),
            configuration: configuration
        )
        let tableName = "test_\(DispatchTime.now().uptimeNanoseconds)"
        XCTAssertNoThrow(
            try cassandraClient.run("create table \(tableName) (id int primary key);").wait()
        )
        XCTAssertNoThrow(try cassandraClient.shutdown())
        XCTAssertThrowsError(try cassandraClient.query("select * from \(tableName);").wait()) { error in
            XCTAssertEqual(error as? CassandraClient.Error, CassandraClient.Error.disconnected)
        }
        XCTAssertNoThrow(try cassandraClient.shutdown())
    }

    func testKeyspace() {
        let keyspace1 = "test_\(DispatchTime.now().uptimeNanoseconds)"
        XCTAssertNoThrow(
            try self.cassandraClient.withSession(keyspace: .none) { session in
                try session
                    .run(
                        "create keyspace \(keyspace1) with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"
                    )
                    .wait()
            }
        )
        XCTAssertNoThrow(
            try self.cassandraClient.withSession(keyspace: keyspace1) { session in
                try session
                    .run("create table test (id int primary key);")
                    .wait()
            }
        )

        let keyspace2 = "test_\(DispatchTime.now().uptimeNanoseconds)"
        var configuration = self.configuration!
        configuration.keyspace = keyspace2
        let cassandraClient = CassandraClient(configuration: configuration)
        defer { XCTAssertNoThrow(try cassandraClient.shutdown()) }
        XCTAssertNoThrow(
            try cassandraClient.withSession(keyspace: .none) { session in
                try session
                    .run(
                        "create keyspace \(keyspace2) with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"
                    )
                    .wait()
            }
        )
        XCTAssertNoThrow(
            try cassandraClient
                .run("create table testtest (id int primary key);")
                .wait()
        )
    }

    func testQueryIterator() {
        let tableName = "test_\(DispatchTime.now().uptimeNanoseconds)"
        XCTAssertNoThrow(
            try self.cassandraClient.run("create table \(tableName) (id int primary key, data text);")
                .wait()
        )

        let options = CassandraClient.Statement.Options(consistency: .localQuorum)

        let count = Int.random(in: 5000...6000)
        var futures = [EventLoopFuture<Void>]()
        for index in 0..<count {
            futures.append(
                self.cassandraClient.run(
                    "insert into \(tableName) (id, data) values (?, ?);",
                    parameters: [.int32(Int32(index)), .string(UUID().uuidString)],
                    options: options
                )
            )
        }

        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: System.coreCount)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
        XCTAssertNoThrow(try EventLoopFuture.andAllSucceed(futures, on: eventLoopGroup.next()).wait())

        let rows = try! self.cassandraClient.query("select id, data from \(tableName);").wait()
        XCTAssertEqual(rows.count, count, "result count should match")
        XCTAssertEqual(rows.columnsCount, 2, "result column count should match")
        let ids = rows.compactMap { $0.column(0)?.int32 }
        XCTAssertEqual(ids.count, count, "result count should match")
        for (index, id) in ids.sorted().enumerated() {
            XCTAssertEqual(id, Int32(index))
        }

        let paginatedIDs = try! self.cassandraClient.query(
            "select id, data from \(tableName);",
            pageSize: Int32(1000)
        )
        .flatMap { paginatedRows in
            paginatedRows.map { row in
                row.column(0)?.int32
            }
        }.wait().compactMap { $0 }
        XCTAssertEqual(paginatedIDs.count, count, "result count should match")
        for (index, id) in paginatedIDs.sorted().enumerated() {
            XCTAssertEqual(id, Int32(index))
        }
    }

    func testPagingToken() throws {
        let tableName = "test_\(DispatchTime.now().uptimeNanoseconds)"
        try self.cassandraClient.run("create table \(tableName) (id int primary key, data text);")
            .wait()

        let options = CassandraClient.Statement.Options(consistency: .localQuorum)

        let count = Int.random(in: 5000...6000)
        var futures = [EventLoopFuture<Void>]()
        for index in 0..<count {
            futures.append(
                self.cassandraClient.run(
                    "insert into \(tableName) (id, data) values (?, ?);",
                    parameters: [.int32(Int32(index)), .string(UUID().uuidString)],
                    options: options
                )
            )
        }

        let initialPages = try self.cassandraClient.query(
            "select id, data from \(tableName);",
            pageSize: Int32(5)
        ).wait()

        for _ in 0..<Int.random(in: 10...20) {
            _ = try! initialPages.nextPage().wait()
        }

        let page = try initialPages.nextPage().wait()
        let pageToken = try page.opaquePagingStateToken()
        let row = try initialPages.nextPage().wait().first!

        let statement = try CassandraClient.Statement(query: "select id, data from \(tableName);")
        try! statement.setPagingStateToken(pageToken)
        let offsetPages = try self.cassandraClient.execute(
            statement: statement,
            pageSize: Int32(5),
            on: nil
        ).wait()
        let pagedRow: CassandraClient.Row = try offsetPages.nextPage().wait().first!

        let id1: CassandraClient.Column = pagedRow.column(0)!
        let id2: CassandraClient.Column = row.column(0)!
        XCTAssertEqual(id1.int32, id2.int32)
    }

    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    func testQueryAsyncIterator() throws {
        runAsyncAndWaitFor(
            {
                let tableName = "test_\(DispatchTime.now().uptimeNanoseconds)"
                try await self.cassandraClient.run(
                    "create table \(tableName) (id int primary key, data text);"
                )

                let count = Int.random(in: 1000...2000)
                await withThrowingTaskGroup(of: Void.self) { group in
                    let options = CassandraClient.Statement.Options(consistency: .localQuorum)
                    for index in 0..<count {
                        group.addTask {
                            try await self.cassandraClient.run(
                                "insert into \(tableName) (id, data) values (?, ?);",
                                parameters: [.int32(Int32(index)), .string(UUID().uuidString)],
                                options: options
                            )
                        }
                    }
                }

                let rows = try await self.cassandraClient.query("select id, data from \(tableName);")
                XCTAssertEqual(rows.count, count, "result count should match")
                XCTAssertEqual(rows.columnsCount, 2, "result column count should match")
                let ids = rows.compactMap { $0.column(0)?.int32 }
                XCTAssertEqual(ids.count, count, "result count should match")
                for (index, id) in ids.sorted().enumerated() {
                    XCTAssertEqual(id, Int32(index))
                }

                let paginatedRows = try await self.cassandraClient.query(
                    "select id, data from \(tableName);",
                    pageSize: Int32(300)
                )

                var paginatedIDs: [Int32] = []
                for try await paginatedID in paginatedRows.map({ row in row.column(0)?.int32 })
                    .compactMap({ $0 })
                {
                    paginatedIDs.append(paginatedID)
                }

                XCTAssertEqual(paginatedIDs.count, count, "result count should match")
                for (index, id) in paginatedIDs.sorted().enumerated() {
                    XCTAssertEqual(id, Int32(index))
                }
            },
            5.0
        )
    }

    func testQueryBuffered() {
        let tableName = "test_\(DispatchTime.now().uptimeNanoseconds)"
        XCTAssertNoThrow(
            try self.cassandraClient.run("create table \(tableName) (id int primary key, data text);")
                .wait()
        )

        let count = Int.random(in: 5000...6000)
        var futures = [EventLoopFuture<Void>]()
        for index in 0..<count {
            futures.append(
                self.cassandraClient.run(
                    "insert into \(tableName) (id, data) values (?, ?);",
                    parameters: [.int32(Int32(index)), .string(UUID().uuidString)]
                )
            )
        }

        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: System.coreCount)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
        XCTAssertNoThrow(try EventLoopFuture.andAllSucceed(futures, on: eventLoopGroup.next()).wait())

        let rows = try! self.cassandraClient.query("select id, data from \(tableName);") {
            $0.column(0)?.int32
        }.wait()
        XCTAssertEqual(rows.count, count, "result count should match")
        if rows.count == count {
            for (index, value) in rows.sorted().enumerated() {
                XCTAssertEqual(value, Int32(index))
            }
        }
    }

    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    func testQueryAsyncBuffered() throws {
        runAsyncAndWaitFor(
            {
                let tableName = "test_\(DispatchTime.now().uptimeNanoseconds)"
                try await self.cassandraClient.run(
                    "create table \(tableName) (id int primary key, data text);"
                )

                let count = Int.random(in: 1000...2000)
                await withThrowingTaskGroup(of: Void.self) { group in
                    for index in 0..<count {
                        group.addTask {
                            try await self.cassandraClient.run(
                                "insert into \(tableName) (id, data) values (?, ?);",
                                parameters: [.int32(Int32(index)), .string(UUID().uuidString)]
                            )
                        }
                    }
                }

                let rows = try await self.cassandraClient.query("select id, data from \(tableName);") {
                    $0.column(0)?.int32
                }
                XCTAssertEqual(rows.count, count, "result count should match")
                if rows.count == count {
                    for (index, value) in rows.sorted().enumerated() {
                        XCTAssertEqual(value, Int32(index))
                    }
                }
            },
            5.0
        )
    }

    func testSelectIn() throws {
        let tableName = "test_\(DispatchTime.now().uptimeNanoseconds)"
        XCTAssertNoThrow(
            try self.cassandraClient.run("create table \(tableName) (id int primary key, data text);")
                .wait()
        )

        let count = Int.random(in: 5...100)
        var futures = [EventLoopFuture<Void>]()
        for index in 0..<count {
            futures.append(
                self.cassandraClient.run(
                    "insert into \(tableName) (id, data) values (?, ?);",
                    parameters: [.int32(Int32(index)), .string(UUID().uuidString)]
                )
            )
        }

        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: System.coreCount)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
        XCTAssertNoThrow(try EventLoopFuture.andAllSucceed(futures, on: eventLoopGroup.next()).wait())

        let selectIDs: [Int32] = (0...Int.random(in: 1...5)).map { _ in
            Int32.random(in: 0..<Int32(count))
        }
        let rows = try self.cassandraClient.query(
            "select id, data from \(tableName) where id in ?;",
            parameters: [.int32Array(selectIDs)]
        ).wait()
        XCTAssertEqual(
            Set(rows.compactMap { row in row.column(0)?.int32 }),
            Set(selectIDs),
            "result should match"
        )
    }

    func testDecoding() {
        struct Model: Codable, Equatable {
            let col1: Int8
            let col2: Int16
            let col3: Int32
            let col4: Int64
            let col5: Float32
            let col6: Double
            let col7: String
            let col8: Date
            let col9: UUID
            let col10: TimeBasedUUID
            let col11: [UInt8]
            let col12: Bool
            let col13: String?
            let col14: [Int8]
            let col15: [Int16]
            let col16: [Int32]
            let col17: [Int64]
            let col18: [Float32]
            let col19: [Double]
            let col20: [String]
            let doesNotExist: Bool?
        }

        let data: [Model] = (Int8(0)..<Int8(10)).map { index in
            Model(
                col1: index,
                col2: Int16.random(in: Int16.min...Int16.max),
                col3: Int32.random(in: Int32.min...Int32.max),
                col4: Int64.random(in: Int64.min...Int64.max),
                col5: Float32.random(in: Float(Int32.min)...Float(Int32.max)),
                col6: Double.random(in: Double(Int64.min)...Double(Int64.max)),
                col7: UUID().uuidString,
                col8: Date(),
                col9: UUID(),
                col10: TimeBasedUUID(),
                col11: randomBytes(size: Int.random(in: 10...1024 * 1024)),
                col12: Bool.random(),
                col13: nil,
                col14: (0...Int.random(in: 1...3)).map { _ in Int8.random(in: Int8.min...Int8.max) },
                col15: (0...Int.random(in: 1...3)).map { _ in Int16.random(in: Int16.min...Int16.max) },
                col16: (0...Int.random(in: 1...3)).map { _ in Int32.random(in: Int32.min...Int32.max) },
                col17: (0...Int.random(in: 1...3)).map { _ in Int64.random(in: Int64.min...Int64.max) },
                col18: (0...Int.random(in: 1...3)).map { _ in
                    Float32.random(in: Float(Int32.min)...Float(Int32.max))
                },
                col19: (0...Int.random(in: 1...3)).map { _ in
                    Double.random(in: Double(Int64.min)...Double(Int64.max))
                },
                col20: (0...Int.random(in: 1...3)).map { _ in UUID().uuidString },
                doesNotExist: nil
            )
        }

        let tableName = "test_\(DispatchTime.now().uptimeNanoseconds)"
        print(tableName)
        XCTAssertNoThrow(
            try self.cassandraClient.run(
                """
                create table \(tableName)
                (
                col1 tinyint primary key,
                col2 smallint,
                col3 int,
                col4 bigint,
                col5 float,
                col6 double,
                col7 text,
                col8 timestamp,
                col9 uuid,
                col10 timeuuid,
                col11 blob,
                col12 boolean,
                col13 text,
                col14 list<tinyint>,
                col15 list<smallint>,
                col16 list<int>,
                col17 list<bigint>,
                col18 list<float>,
                col19 list<double>,
                col20 list<text>
                );
                """
            ).wait()
        )

        var futures = [EventLoopFuture<Void>]()
        for model in data {
            let parameters: [CassandraClient.Statement.Value] = [
                .int8(model.col1),
                .int16(model.col2),
                .int32(model.col3),
                .int64(model.col4),
                .float32(model.col5),
                .double(model.col6),
                .string(model.col7),
                .date(model.col8),
                .uuid(model.col9),
                .timeuuid(model.col10),
                .bytes(model.col11),
                .bool(model.col12),
                .null,
                .int8Array(model.col14),
                .int16Array(model.col15),
                .int32Array(model.col16),
                .int64Array(model.col17),
                .float32Array(model.col18),
                .doubleArray(model.col19),
                .stringArray(model.col20),
            ]
            futures.append(
                self.cassandraClient.run(
                    """
                    insert into \(tableName)
                    (col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12, col13, col14, col15, col16, col17, col18, col19, col20)
                    values
                    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    parameters: parameters
                )
            )
        }

        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: System.coreCount)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
        XCTAssertNoThrow(try EventLoopFuture.andAllSucceed(futures, on: eventLoopGroup.next()).wait())

        let result: [Model] = try! self.cassandraClient.query("select * from \(tableName);").wait()
            .sorted { a, b in a.col1 < b.col1 }
        XCTAssertEqual(result.count, data.count, "result count should match")
        for (index, item) in result.enumerated() {
            XCTAssertEqual(item.col1, data[index].col1, "results should match")
            XCTAssertEqual(item.col2, data[index].col2, "results should match")
            XCTAssertEqual(item.col3, data[index].col3, "results should match")
            XCTAssertEqual(item.col4, data[index].col4, "results should match")
            XCTAssertEqual(item.col5, data[index].col5, "results should match")
            XCTAssertEqual(item.col6, data[index].col6, "results should match")
            XCTAssertEqual(item.col7, data[index].col7, "results should match")
            XCTAssertEqual(
                item.col8.timeIntervalSince1970,
                data[index].col8.timeIntervalSince1970,
                accuracy: 1,
                "results should match"
            )
            XCTAssertEqual(item.col9, data[index].col9, "results should match")
            XCTAssertEqual(item.col10, data[index].col10, "results should match")
            XCTAssertEqual(item.col11, data[index].col11, "results should match")
            XCTAssertEqual(item.col12, data[index].col12, "results should match")
            XCTAssertEqual(item.col13, data[index].col13, "results should match")
            XCTAssertEqual(item.col14, data[index].col14, "results should match")
            XCTAssertEqual(item.col15, data[index].col15, "results should match")
            XCTAssertEqual(item.col16, data[index].col16, "results should match")
            XCTAssertEqual(item.col17, data[index].col17, "results should match")
            XCTAssertEqual(item.col18, data[index].col18, "results should match")
            XCTAssertEqual(item.col19, data[index].col19, "results should match")
            XCTAssertEqual(item.col20, data[index].col20, "results should match")
        }
    }

    // tinyint: 8-bit signed integer.
    // smallint: 16-bit signed integer.
    // int: 32-bit signed integer.
    // bigint: 64-bit signed integer.
    // varint: Arbitrary-precision integer.

    // decimal: Variable-precision decimal, supports integers and floats.
    // float: 32-bit IEEE-754 floating point.
    // double: 64-bit IEEE-754 floating point.

    // ascii: US-ASCII characters.`
    // text: UTF-8 encoded string.
    // varchar: UTF-8 encoded string.

    // date: 32-bit unsigned integer representing the number of days since epoch
    // timestamp: 64-bit signed integer representing the date and time since epoch (January 1 1970 at 00:00:00 GMT) in milliseconds.
    //           INSERT or UPDATE string format is ISO-8601; the string must contain the date and optionally can include the time and time

    // uuid: 128 bit universally unique identifier (UUID). Generate with the UUID function.
    // timeuuid: Version 1 UUID; unique identifier that includes a“conflict-free” timestamp. Generate with the NOW function

    // blob: Arbitrary bytes (no validation), expressed as hexadecimal. See Blob conversion functions.
    // boolean: True or false. Stored internally as true or false; when using the COPY TO in cqlsh to import or export data,
    //          change the format using the BOOLSTYLE option, for example when importing survey results that have yes/no style answer column.

    // IP: address string in IPv4 or IPv6 format
    func testDataTypes() {
        let tableName = "test_\(DispatchTime.now().uptimeNanoseconds)"
        XCTAssertNoThrow(
            try self.cassandraClient.run(
                """
                create table \(tableName) (
                col1 tinyint primary key,
                col2 smallint,
                col3 int,
                col4 bigint,
                col5 varint,
                col6 decimal,
                col7 float,
                col8 double,
                col9 text,
                col10 date,
                col11 timestamp,
                col12 uuid,
                col13 timeuuid,
                col14 blob,
                col15 boolean,
                col16 text,
                col17 list<tinyint>,
                col18 list<smallint>,
                col19 list<int>,
                col20 list<bigint>,
                col21 list<float>,
                col22 list<double>,
                col23 list<text>,
                )
                """
            ).wait()
        )

        for index in Int8(0)..<Int8(10) {
            let int16 = Int16.random(in: Int16.min...Int16.max)
            let int32 = Int32.random(in: Int32.min...Int32.max)
            let int64 = Int64.random(in: Int64.min...Int64.max)
            // let varint = // FIXME: implement varint
            // let decimal = // FIXME: implement decimal
            let float32 = Float32.random(in: Float(Int32.min)...Float(Int32.max))
            let double = Double.random(in: Double(Int64.min)...Double(Int64.max))
            let text = UUID().uuidString
            let now = Date().timeIntervalSince1970  // seconds
            let date = UInt32(now / 86400)  // days
            let timestamp = Int64(now * 1000)  // millisconds
            let uuid = UUID()
            let timeuuid = TimeBasedUUID()
            let blob = self.randomBytes(size: Int.random(in: 10...1024 * 1024))
            let bool = Bool.random()
            let null: String? = nil
            let int8List = (0...Int.random(in: 1...3)).map { _ in Int8.random(in: Int8.min...Int8.max) }
            let int16List = (0...Int.random(in: 1...3)).map { _ in Int16.random(in: Int16.min...Int16.max)
            }
            let int32List = (0...Int.random(in: 1...3)).map { _ in Int32.random(in: Int32.min...Int32.max)
            }
            let int64List = (0...Int.random(in: 1...3)).map { _ in Int64.random(in: Int64.min...Int64.max)
            }
            let float32List = (0...Int.random(in: 1...3)).map { _ in
                Float32.random(in: Float(Int32.min)...Float(Int32.max))
            }
            let doubleList = (0...Int.random(in: 1...3)).map { _ in
                Double.random(in: Double(Int64.min)...Double(Int64.max))
            }
            let textList = (0...Int.random(in: 1...3)).map { _ in UUID().uuidString }

            let parameters: [CassandraClient.Statement.Value] = [
                .int8(index),  // tinyint
                .int16(int16),  // smallint
                .int32(int32),  // int
                .int64(int64),  // bigint
                .null,  // varint
                .null,  // decimal
                .float32(float32),  // float
                .double(double),  // double
                .string(text),  // text
                .rawDate(daysSinceEpoch: date),  // date
                .rawTimestamp(millisecondsSinceEpoch: timestamp),  // timestamp
                .uuid(uuid),  // uuid
                .timeuuid(timeuuid),  // timeuuid
                .bytes(blob),  // bytes
                .bool(bool),
                .null,
                .int8Array(int8List),  // list<tinyint>
                .int16Array(int16List),  // list<smallint>
                .int32Array(int32List),  // list<int>
                .int64Array(int64List),  // list<bigint>
                .float32Array(float32List),  // list<float>
                .doubleArray(doubleList),  // list<double>
                .stringArray(textList),  // list<text>
            ]

            XCTAssertNoThrow(
                try self.cassandraClient.run(
                    """
                    insert into \(tableName)
                    (col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, col21, col22, col23)
                    values
                    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    parameters: parameters
                ).wait()
            )

            let result = try! self.cassandraClient.query("select * from \(tableName);").wait()
            XCTAssertEqual(Int8(result.count), index + 1, "expected exactly one result")
            let row = result.first { $0.column("col1") == index }!
            XCTAssertEqual(row.column("col1"), index, "expected value to match")
            XCTAssertEqual(row.column("col2"), int16, "expected value to match")
            XCTAssertEqual(row.column("col3"), int32, "expected value to match")
            XCTAssertEqual(row.column("col4"), int64, "expected value to match")
            // XCTAssertEqual(row.column("col5"), varint, "expected value to match")
            // XCTAssertEqual(row.column("col6"), decimal, "expected value to match")
            XCTAssertEqual(row.column("col7"), float32, "expected value to match")
            XCTAssertEqual(row.column("col8"), double, "expected value to match")
            XCTAssertEqual(row.column("col9"), text, "expected value to match")
            XCTAssertEqual(
                row.column("col10")?.date.map { Double($0 * 86400) } ?? 0.0,
                now,
                accuracy: 100_000,
                "expected value to match"
            )
            XCTAssertEqual(
                row.column("col11")?.timestamp.map { Double($0 / 1000) } ?? 0.0,
                now,
                accuracy: 1,
                "expected value to match"
            )
            XCTAssertEqual(row.column("col12"), uuid, "expected value to match")
            XCTAssertEqual(row.column("col13"), timeuuid, "expected value to match")
            XCTAssertEqual(row.column("col14"), blob, "expected value to match")
            XCTAssertEqual(row.column("col15"), bool, "expected value to match")
            XCTAssertEqual(row.column("col16"), null, "expected value to match")
            XCTAssertEqual(row.column("col17"), int8List, "expected value to match")
            XCTAssertEqual(row.column("col18"), int16List, "expected value to match")
            XCTAssertEqual(row.column("col19"), int32List, "expected value to match")
            XCTAssertEqual(row.column("col20"), int64List, "expected value to match")
            XCTAssertEqual(row.column("col21"), float32List, "expected value to match")
            XCTAssertEqual(row.column("col22"), doubleList, "expected value to match")
            XCTAssertEqual(row.column("col23"), textList, "expected value to match")
        }
    }

    func testMapTypes() {
        let tableName = "test_maps_\(DispatchTime.now().uptimeNanoseconds)"
        XCTAssertNoThrow(
            try self.cassandraClient.run(
                """
                create table \(tableName) (
                col1 int primary key,
                col2 map<tinyint, tinyint>,
                col3 map<tinyint, smallint>,
                col4 map<tinyint, int>,
                col5 map<tinyint, bigint>,
                col6 map<tinyint, float>,
                col7 map<tinyint, double>,
                col8 map<tinyint, boolean>,
                col9 map<tinyint, text>,
                col10 map<tinyint, uuid>,
                col11 map<smallint, tinyint>,
                col12 map<smallint, smallint>,
                col13 map<smallint, int>,
                col14 map<smallint, bigint>,
                col15 map<smallint, float>,
                col16 map<smallint, double>,
                col17 map<smallint, boolean>,
                col18 map<smallint, text>,
                col19 map<smallint, uuid>,
                col20 map<int, tinyint>,
                col21 map<int, smallint>,
                col22 map<int, int>,
                col23 map<int, bigint>,
                col24 map<int, float>,
                col25 map<int, double>,
                col26 map<int, boolean>,
                col27 map<int, text>,
                col28 map<int, uuid>,
                col29 map<bigint, tinyint>,
                col30 map<bigint, smallint>,
                col31 map<bigint, int>,
                col32 map<bigint, bigint>,
                col33 map<bigint, float>,
                col34 map<bigint, double>,
                col35 map<bigint, boolean>,
                col36 map<bigint, text>,
                col37 map<bigint, uuid>,
                col38 map<text, tinyint>,
                col39 map<text, smallint>,
                col40 map<text, int>,
                col41 map<text, bigint>,
                col42 map<text, float>,
                col43 map<text, double>,
                col44 map<text, boolean>,
                col45 map<text, text>,
                col46 map<text, uuid>,
                col47 map<uuid, tinyint>,
                col48 map<uuid, smallint>,
                col49 map<uuid, int>,
                col50 map<uuid, bigint>,
                col51 map<uuid, float>,
                col52 map<uuid, double>,
                col53 map<uuid, boolean>,
                col54 map<uuid, text>,
                col55 map<uuid, uuid>,
                col56 map<timeuuid, tinyint>,
                col57 map<timeuuid, smallint>,
                col58 map<timeuuid, int>,
                col59 map<timeuuid, bigint>,
                col60 map<timeuuid, float>,
                col61 map<timeuuid, double>,
                col62 map<timeuuid, boolean>,
                col63 map<timeuuid, text>,
                col64 map<timeuuid, uuid>
                )
                """
            ).wait()
        )

        for index in Int32(0)..<Int32(3) {
            let int8Int8Map = [Int8(1): Int8(10), Int8(2): Int8(20)]
            let int8Int16Map = [Int8(1): Int16(100), Int8(2): Int16(200)]
            let int8Int32Map = [Int8(1): Int32(1000), Int8(2): Int32(2000)]
            let int8Int64Map = [Int8(1): Int64(10000), Int8(2): Int64(20000)]
            let int8Float32Map = [Int8(1): Float32(1.5), Int8(2): Float32(2.5)]
            let int8DoubleMap = [Int8(1): Double(10.5), Int8(2): Double(20.5)]
            let int8BoolMap = [Int8(1): true, Int8(2): false]
            let int8StringMap = [Int8(1): "value1", Int8(2): "value2"]
            let int8UUIDMap = [Int8(1): UUID(), Int8(2): UUID()]

            let int16Int8Map = [Int16(10): Int8(1), Int16(20): Int8(2)]
            let int16Int16Map = [Int16(10): Int16(100), Int16(20): Int16(200)]
            let int16Int32Map = [Int16(10): Int32(1000), Int16(20): Int32(2000)]
            let int16Int64Map = [Int16(10): Int64(10000), Int16(20): Int64(20000)]
            let int16Float32Map = [Int16(10): Float32(1.5), Int16(20): Float32(2.5)]
            let int16DoubleMap = [Int16(10): Double(10.5), Int16(20): Double(20.5)]
            let int16BoolMap = [Int16(10): true, Int16(20): false]
            let int16StringMap = [Int16(10): "value1", Int16(20): "value2"]
            let int16UUIDMap = [Int16(10): UUID(), Int16(20): UUID()]

            let int32Int8Map = [Int32(100): Int8(1), Int32(200): Int8(2)]
            let int32Int16Map = [Int32(100): Int16(10), Int32(200): Int16(20)]
            let int32Int32Map = [Int32(100): Int32(1000), Int32(200): Int32(2000)]
            let int32Int64Map = [Int32(100): Int64(10000), Int32(200): Int64(20000)]
            let int32Float32Map = [Int32(100): Float32(1.5), Int32(200): Float32(2.5)]
            let int32DoubleMap = [Int32(100): Double(10.5), Int32(200): Double(20.5)]
            let int32BoolMap = [Int32(100): true, Int32(200): false]
            let int32StringMap = [Int32(100): "value1", Int32(200): "value2"]
            let int32UUIDMap = [Int32(100): UUID(), Int32(200): UUID()]

            let int64Int8Map = [Int64(1000): Int8(1), Int64(2000): Int8(2)]
            let int64Int16Map = [Int64(1000): Int16(10), Int64(2000): Int16(20)]
            let int64Int32Map = [Int64(1000): Int32(100), Int64(2000): Int32(200)]
            let int64Int64Map = [Int64(1000): Int64(10000), Int64(2000): Int64(20000)]
            let int64Float32Map = [Int64(1000): Float32(1.5), Int64(2000): Float32(2.5)]
            let int64DoubleMap = [Int64(1000): Double(10.5), Int64(2000): Double(20.5)]
            let int64BoolMap = [Int64(1000): true, Int64(2000): false]
            let int64StringMap = [Int64(1000): "value1", Int64(2000): "value2"]
            let int64UUIDMap = [Int64(1000): UUID(), Int64(2000): UUID()]

            let stringInt8Map = ["key1": Int8(1), "key2": Int8(2)]
            let stringInt16Map = ["key1": Int16(10), "key2": Int16(20)]
            let stringInt32Map = ["key1": Int32(100), "key2": Int32(200)]
            let stringInt64Map = ["key1": Int64(1000), "key2": Int64(2000)]
            let stringFloat32Map = ["key1": Float32(1.5), "key2": Float32(2.5)]
            let stringDoubleMap = ["key1": Double(10.5), "key2": Double(20.5)]
            let stringBoolMap = ["key1": true, "key2": false]
            let stringStringMap = ["key1": "value1", "key2": "value2"]
            let stringUUIDMap = ["key1": UUID(), "key2": UUID()]

            let uuid1 = UUID()
            let uuid2 = UUID()
            let uuidInt8Map = [uuid1: Int8(1), uuid2: Int8(2)]
            let uuidInt16Map = [uuid1: Int16(10), uuid2: Int16(20)]
            let uuidInt32Map = [uuid1: Int32(100), uuid2: Int32(200)]
            let uuidInt64Map = [uuid1: Int64(1000), uuid2: Int64(2000)]
            let uuidFloat32Map = [uuid1: Float32(1.5), uuid2: Float32(2.5)]
            let uuidDoubleMap = [uuid1: Double(10.5), uuid2: Double(20.5)]
            let uuidBoolMap = [uuid1: true, uuid2: false]
            let uuidStringMap = [uuid1: "value1", uuid2: "value2"]
            let uuidUUIDMap = [uuid1: UUID(), uuid2: UUID()]

            let timeuuid1 = TimeBasedUUID()
            let timeuuid2 = TimeBasedUUID()
            let timeuuidInt8Map = [timeuuid1: Int8(1), timeuuid2: Int8(2)]
            let timeuuidInt16Map = [timeuuid1: Int16(10), timeuuid2: Int16(20)]
            let timeuuidInt32Map = [timeuuid1: Int32(100), timeuuid2: Int32(200)]
            let timeuuidInt64Map = [timeuuid1: Int64(1000), timeuuid2: Int64(2000)]
            let timeuuidFloat32Map = [timeuuid1: Float32(1.5), timeuuid2: Float32(2.5)]
            let timeuuidDoubleMap = [timeuuid1: Double(10.5), timeuuid2: Double(20.5)]
            let timeuuidBoolMap = [timeuuid1: true, timeuuid2: false]
            let timeuuidStringMap = [timeuuid1: "value1", timeuuid2: "value2"]
            let timeuuidUUIDMap = [timeuuid1: UUID(), timeuuid2: UUID()]

            let parameters: [CassandraClient.Statement.Value] = [
                .int32(index),
                .int8Int8Map(int8Int8Map),
                .int8Int16Map(int8Int16Map),
                .int8Int32Map(int8Int32Map),
                .int8Int64Map(int8Int64Map),
                .int8Float32Map(int8Float32Map),
                .int8DoubleMap(int8DoubleMap),
                .int8BoolMap(int8BoolMap),
                .int8StringMap(int8StringMap),
                .int8UUIDMap(int8UUIDMap),
                .int16Int8Map(int16Int8Map),
                .int16Int16Map(int16Int16Map),
                .int16Int32Map(int16Int32Map),
                .int16Int64Map(int16Int64Map),
                .int16Float32Map(int16Float32Map),
                .int16DoubleMap(int16DoubleMap),
                .int16BoolMap(int16BoolMap),
                .int16StringMap(int16StringMap),
                .int16UUIDMap(int16UUIDMap),
                .int32Int8Map(int32Int8Map),
                .int32Int16Map(int32Int16Map),
                .int32Int32Map(int32Int32Map),
                .int32Int64Map(int32Int64Map),
                .int32Float32Map(int32Float32Map),
                .int32DoubleMap(int32DoubleMap),
                .int32BoolMap(int32BoolMap),
                .int32StringMap(int32StringMap),
                .int32UUIDMap(int32UUIDMap),
                .int64Int8Map(int64Int8Map),
                .int64Int16Map(int64Int16Map),
                .int64Int32Map(int64Int32Map),
                .int64Int64Map(int64Int64Map),
                .int64Float32Map(int64Float32Map),
                .int64DoubleMap(int64DoubleMap),
                .int64BoolMap(int64BoolMap),
                .int64StringMap(int64StringMap),
                .int64UUIDMap(int64UUIDMap),
                .stringInt8Map(stringInt8Map),
                .stringInt16Map(stringInt16Map),
                .stringInt32Map(stringInt32Map),
                .stringInt64Map(stringInt64Map),
                .stringFloat32Map(stringFloat32Map),
                .stringDoubleMap(stringDoubleMap),
                .stringBoolMap(stringBoolMap),
                .stringStringMap(stringStringMap),
                .stringUUIDMap(stringUUIDMap),
                .uuidInt8Map(uuidInt8Map),
                .uuidInt16Map(uuidInt16Map),
                .uuidInt32Map(uuidInt32Map),
                .uuidInt64Map(uuidInt64Map),
                .uuidFloat32Map(uuidFloat32Map),
                .uuidDoubleMap(uuidDoubleMap),
                .uuidBoolMap(uuidBoolMap),
                .uuidStringMap(uuidStringMap),
                .uuidUUIDMap(uuidUUIDMap),
                .timeuuidInt8Map(timeuuidInt8Map),
                .timeuuidInt16Map(timeuuidInt16Map),
                .timeuuidInt32Map(timeuuidInt32Map),
                .timeuuidInt64Map(timeuuidInt64Map),
                .timeuuidFloat32Map(timeuuidFloat32Map),
                .timeuuidDoubleMap(timeuuidDoubleMap),
                .timeuuidBoolMap(timeuuidBoolMap),
                .timeuuidStringMap(timeuuidStringMap),
                .timeuuidUUIDMap(timeuuidUUIDMap),
            ]

            XCTAssertNoThrow(
                try self.cassandraClient.run(
                    """
                    insert into \(tableName)
                    (col1, col2, col3, col4, col5, col6, col7, col8, col9, col10,
                     col11, col12, col13, col14, col15, col16, col17, col18, col19,
                     col20, col21, col22, col23, col24, col25, col26, col27, col28,
                     col29, col30, col31, col32, col33, col34, col35, col36, col37,
                     col38, col39, col40, col41, col42, col43, col44, col45, col46,
                     col47, col48, col49, col50, col51, col52, col53, col54, col55,
                     col56, col57, col58, col59, col60, col61, col62, col63, col64)
                    values
                    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                     ?, ?, ?, ?, ?, ?, ?, ?, ?,
                     ?, ?, ?, ?, ?, ?, ?, ?, ?,
                     ?, ?, ?, ?, ?, ?, ?, ?, ?,
                     ?, ?, ?, ?, ?, ?, ?, ?, ?,
                     ?, ?, ?, ?, ?, ?, ?, ?, ?,
                     ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    parameters: parameters
                ).wait()
            )

            let result = try! self.cassandraClient.query(
                "select * from \(tableName) where col1 = ?;",
                parameters: [.int32(index)]
            ).wait()
            XCTAssertEqual(result.count, 1, "expected exactly one result")
            let row = result.first!

            XCTAssertEqual(row.column("col1"), index, "expected value to match")
            XCTAssertEqual(row.column("col2"), int8Int8Map, "expected map to match")
            XCTAssertEqual(row.column("col3"), int8Int16Map, "expected map to match")
            XCTAssertEqual(row.column("col4"), int8Int32Map, "expected map to match")
            XCTAssertEqual(row.column("col5"), int8Int64Map, "expected map to match")
            XCTAssertEqual(row.column("col6"), int8Float32Map, "expected map to match")
            XCTAssertEqual(row.column("col7"), int8DoubleMap, "expected map to match")
            XCTAssertEqual(row.column("col8"), int8BoolMap, "expected map to match")
            XCTAssertEqual(row.column("col9"), int8StringMap, "expected map to match")
            XCTAssertEqual(row.column("col10"), int8UUIDMap, "expected map to match")
            XCTAssertEqual(row.column("col11"), int16Int8Map, "expected map to match")
            XCTAssertEqual(row.column("col12"), int16Int16Map, "expected map to match")
            XCTAssertEqual(row.column("col13"), int16Int32Map, "expected map to match")
            XCTAssertEqual(row.column("col14"), int16Int64Map, "expected map to match")
            XCTAssertEqual(row.column("col15"), int16Float32Map, "expected map to match")
            XCTAssertEqual(row.column("col16"), int16DoubleMap, "expected map to match")
            XCTAssertEqual(row.column("col17"), int16BoolMap, "expected map to match")
            XCTAssertEqual(row.column("col18"), int16StringMap, "expected map to match")
            XCTAssertEqual(row.column("col19"), int16UUIDMap, "expected map to match")
            XCTAssertEqual(row.column("col20"), int32Int8Map, "expected map to match")
            XCTAssertEqual(row.column("col21"), int32Int16Map, "expected map to match")
            XCTAssertEqual(row.column("col22"), int32Int32Map, "expected map to match")
            XCTAssertEqual(row.column("col23"), int32Int64Map, "expected map to match")
            XCTAssertEqual(row.column("col24"), int32Float32Map, "expected map to match")
            XCTAssertEqual(row.column("col25"), int32DoubleMap, "expected map to match")
            XCTAssertEqual(row.column("col26"), int32BoolMap, "expected map to match")
            XCTAssertEqual(row.column("col27"), int32StringMap, "expected map to match")
            XCTAssertEqual(row.column("col28"), int32UUIDMap, "expected map to match")
            XCTAssertEqual(row.column("col29"), int64Int8Map, "expected map to match")
            XCTAssertEqual(row.column("col30"), int64Int16Map, "expected map to match")
            XCTAssertEqual(row.column("col31"), int64Int32Map, "expected map to match")
            XCTAssertEqual(row.column("col32"), int64Int64Map, "expected map to match")
            XCTAssertEqual(row.column("col33"), int64Float32Map, "expected map to match")
            XCTAssertEqual(row.column("col34"), int64DoubleMap, "expected map to match")
            XCTAssertEqual(row.column("col35"), int64BoolMap, "expected map to match")
            XCTAssertEqual(row.column("col36"), int64StringMap, "expected map to match")
            XCTAssertEqual(row.column("col37"), int64UUIDMap, "expected map to match")
            XCTAssertEqual(row.column("col38"), stringInt8Map, "expected map to match")
            XCTAssertEqual(row.column("col39"), stringInt16Map, "expected map to match")
            XCTAssertEqual(row.column("col40"), stringInt32Map, "expected map to match")
            XCTAssertEqual(row.column("col41"), stringInt64Map, "expected map to match")
            XCTAssertEqual(row.column("col42"), stringFloat32Map, "expected map to match")
            XCTAssertEqual(row.column("col43"), stringDoubleMap, "expected map to match")
            XCTAssertEqual(row.column("col44"), stringBoolMap, "expected map to match")
            XCTAssertEqual(row.column("col45"), stringStringMap, "expected map to match")
            XCTAssertEqual(row.column("col46"), stringUUIDMap, "expected map to match")
            XCTAssertEqual(row.column("col47"), uuidInt8Map, "expected map to match")
            XCTAssertEqual(row.column("col48"), uuidInt16Map, "expected map to match")
            XCTAssertEqual(row.column("col49"), uuidInt32Map, "expected map to match")
            XCTAssertEqual(row.column("col50"), uuidInt64Map, "expected map to match")
            XCTAssertEqual(row.column("col51"), uuidFloat32Map, "expected map to match")
            XCTAssertEqual(row.column("col52"), uuidDoubleMap, "expected map to match")
            XCTAssertEqual(row.column("col53"), uuidBoolMap, "expected map to match")
            XCTAssertEqual(row.column("col54"), uuidStringMap, "expected map to match")
            XCTAssertEqual(row.column("col55"), uuidUUIDMap, "expected map to match")
            XCTAssertEqual(row.column("col56"), timeuuidInt8Map, "expected map to match")
            XCTAssertEqual(row.column("col57"), timeuuidInt16Map, "expected map to match")
            XCTAssertEqual(row.column("col58"), timeuuidInt32Map, "expected map to match")
            XCTAssertEqual(row.column("col59"), timeuuidInt64Map, "expected map to match")
            XCTAssertEqual(row.column("col60"), timeuuidFloat32Map, "expected map to match")
            XCTAssertEqual(row.column("col61"), timeuuidDoubleMap, "expected map to match")
            XCTAssertEqual(row.column("col62"), timeuuidBoolMap, "expected map to match")
            XCTAssertEqual(row.column("col63"), timeuuidStringMap, "expected map to match")
            XCTAssertEqual(row.column("col64"), timeuuidUUIDMap, "expected map to match")
        }
    }

    func testColumnName() {
        let tableName = "test_\(DispatchTime.now().uptimeNanoseconds)"
        XCTAssertNoThrow(
            try self.cassandraClient.run(
                "create table \(tableName) (id int primary key, name text, age int, email text);"
            ).wait()
        )

        XCTAssertNoThrow(
            try self.cassandraClient.run(
                "insert into \(tableName) (id, name, age, email) values (1, 'Alice', 30, 'alice@example.com');"
            ).wait()
        )

        let rows = try! self.cassandraClient.query("select id, name, age, email from \(tableName);").wait()
        XCTAssertEqual(rows.count, 1, "expected exactly one row")

        // Test valid column indices
        XCTAssertEqual(rows.columnName(at: 0), "id", "first column should be 'id'")
        XCTAssertEqual(rows.columnName(at: 1), "name", "second column should be 'name'")
        XCTAssertEqual(rows.columnName(at: 2), "age", "third column should be 'age'")
        XCTAssertEqual(rows.columnName(at: 3), "email", "fourth column should be 'email'")

        // Test invalid indices
        XCTAssertNil(rows.columnName(at: -1), "negative index should return nil")
        XCTAssertNil(rows.columnName(at: 4), "out of bounds index should return nil")
        XCTAssertNil(rows.columnName(at: 100), "large out of bounds index should return nil")

        // Test with select *
        let rowsStar = try! self.cassandraClient.query("select * from \(tableName);").wait()
        XCTAssertNotNil(rowsStar.columnName(at: 0), "select * should return valid column names")
        XCTAssertEqual(rowsStar.columnsCount, 4, "select * should return all 4 columns")
    }

    func testColumnNames() {
        let tableName = "test_\(DispatchTime.now().uptimeNanoseconds)"
        XCTAssertNoThrow(
            try self.cassandraClient.run(
                "create table \(tableName) (id int primary key, username text, score bigint, active boolean);"
            ).wait()
        )

        XCTAssertNoThrow(
            try self.cassandraClient.run(
                "insert into \(tableName) (id, username, score, active) values (1, 'Bob', 9500, true);"
            ).wait()
        )

        // Test columnNames with explicit column selection
        let rows = try! self.cassandraClient.query("select id, username, score, active from \(tableName);").wait()
        let columnNames = rows.columnNames()

        XCTAssertEqual(columnNames.count, 4, "should return 4 column names")
        XCTAssertEqual(columnNames[0], "id", "first column name should be 'id'")
        XCTAssertEqual(columnNames[1], "username", "second column name should be 'username'")
        XCTAssertEqual(columnNames[2], "score", "third column name should be 'score'")
        XCTAssertEqual(columnNames[3], "active", "fourth column name should be 'active'")

        // Test columnNames with select *
        let rowsStar = try! self.cassandraClient.query("select * from \(tableName);").wait()
        let columnNamesStar = rowsStar.columnNames()
        XCTAssertEqual(columnNamesStar.count, 4, "select * should return all 4 column names")

        // Verify column names array matches individual columnName calls
        for (index, name) in columnNames.enumerated() {
            XCTAssertEqual(
                rows.columnName(at: index),
                name,
                "columnNames array should match columnName at index \(index)"
            )
        }

        // Test columnNames with partial column selection
        let rowsPartial = try! self.cassandraClient.query("select username, active from \(tableName);").wait()
        let columnNamesPartial = rowsPartial.columnNames()
        XCTAssertEqual(columnNamesPartial.count, 2, "partial select should return 2 column names")
        XCTAssertEqual(columnNamesPartial[0], "username", "first column should be 'username'")
        XCTAssertEqual(columnNamesPartial[1], "active", "second column should be 'active'")

        // Test columnNames with single column
        let rowsSingle = try! self.cassandraClient.query("select id from \(tableName);").wait()
        let columnNamesSingle = rowsSingle.columnNames()
        XCTAssertEqual(columnNamesSingle.count, 1, "single column select should return 1 column name")
        XCTAssertEqual(columnNamesSingle[0], "id", "single column should be 'id'")
    }

    func testErrorMapping() {
        XCTAssertThrowsError(try self.cassandraClient.run("boom!").wait()) { error in
            XCTAssertEqual(
                error as? CassandraClient.Error,
                .syntaxError("line 1:0 no viable alternative at input \'boom\' ([boom]...)")
            )
        }
    }

    func testSerialConsistency() {
        let env = ProcessInfo.processInfo.environment
        let keyspace = env["CASSANDRA_KEYSPACE"] ?? "test"

        var serialConfig = CassandraClient.Configuration(
            contactPointsProvider: { callback in
                callback(.success([env["CASSANDRA_HOST"] ?? "127.0.0.1"]))
            },
            port: env["CASSANDRA_CQL_PORT"].flatMap(Int32.init) ?? 9042,
            protocolVersion: .v3
        )
        serialConfig.username = env["CASSANDRA_USER"]
        serialConfig.password = env["CASSANDRA_PASSWORD"]
        serialConfig.keyspace = keyspace
        serialConfig.requestTimeoutMillis = UInt32(24_000)
        serialConfig.connectTimeoutMillis = UInt32(10_000)
        serialConfig.serialConsistency = .serial

        var logger = Logger(label: "test")
        logger.logLevel = .debug

        let serialClient = CassandraClient(configuration: serialConfig, logger: logger)
        defer { XCTAssertNoThrow(try serialClient.shutdown()) }

        XCTAssertNoThrow(
            try serialClient.withSession(keyspace: .none) { session in
                try session.run(
                    "create keyspace if not exists \(keyspace) with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"
                ).wait()
            }
        )

        let serialSession = serialClient.makeSession(keyspace: keyspace)
        defer { XCTAssertNoThrow(try serialSession.shutdown()) }

        let tableName = "test_serial_\(DispatchTime.now().uptimeNanoseconds)"
        XCTAssertNoThrow(try serialSession.run("create table \(tableName) (id int primary key, value int);").wait())
        XCTAssertNoThrow(try serialSession.run("insert into \(tableName) (id, value) values (1, 100);").wait())

        let lwtQuery = "update \(tableName) set value = 200 where id = 1 if value = 100;"
        var serialResult: CassandraClient.Rows?
        XCTAssertNoThrow(serialResult = try serialSession.query(lwtQuery).wait())
        XCTAssertNotNil(serialResult, "Serial consistency LWT should succeed")

        let serialRows = Array(serialResult!)
        XCTAssertFalse(serialRows.isEmpty, "Serial LWT query should return at least one row")
        if let firstRow = serialRows.first {
            XCTAssertNotNil(firstRow.column("[applied]")?.bool, "Serial LWT result should contain [applied] column")
        }

        var localSerialConfig = serialConfig
        localSerialConfig.serialConsistency = .localSerial

        let localSerialClient = CassandraClient(configuration: localSerialConfig, logger: logger)
        defer { XCTAssertNoThrow(try localSerialClient.shutdown()) }

        let localSerialSession = localSerialClient.makeSession(keyspace: keyspace)
        defer { XCTAssertNoThrow(try localSerialSession.shutdown()) }

        let tableName2 = "test_local_serial_\(DispatchTime.now().uptimeNanoseconds)"
        XCTAssertNoThrow(
            try localSerialSession.run("create table \(tableName2) (id int primary key, value int);").wait()
        )
        XCTAssertNoThrow(try localSerialSession.run("insert into \(tableName2) (id, value) values (1, 300);").wait())

        let localLwtQuery = "update \(tableName2) set value = 400 where id = 1 if value = 300;"
        var localSerialResult: CassandraClient.Rows?
        XCTAssertNoThrow(localSerialResult = try localSerialSession.query(localLwtQuery).wait())
        XCTAssertNotNil(localSerialResult, "Local serial consistency LWT should succeed")

        let localSerialRows = Array(localSerialResult!)
        XCTAssertFalse(localSerialRows.isEmpty, "Local serial LWT query should return at least one row")
        if let firstRow = localSerialRows.first {
            XCTAssertNotNil(
                firstRow.column("[applied]")?.bool,
                "Local serial LWT result should contain [applied] column"
            )
        }

        var nilSerialConfig = serialConfig
        nilSerialConfig.serialConsistency = nil

        let nilSerialClient = CassandraClient(configuration: nilSerialConfig, logger: logger)
        defer { XCTAssertNoThrow(try nilSerialClient.shutdown()) }

        let nilSerialSession = nilSerialClient.makeSession(keyspace: keyspace)
        defer { XCTAssertNoThrow(try nilSerialSession.shutdown()) }

        let tableName3 = "test_nil_serial_\(DispatchTime.now().uptimeNanoseconds)"
        XCTAssertNoThrow(try nilSerialSession.run("create table \(tableName3) (id int primary key, value int);").wait())
        XCTAssertNoThrow(try nilSerialSession.run("insert into \(tableName3) (id, value) values (1, 500);").wait())

        let nilLwtQuery = "update \(tableName3) set value = 600 where id = 1 if value = 500;"
        var nilSerialResult: CassandraClient.Rows?
        XCTAssertNoThrow(nilSerialResult = try nilSerialSession.query(nilLwtQuery).wait())
        XCTAssertNotNil(nilSerialResult, "Default serial consistency LWT should succeed")
    }

    // meh, but nothing cross platform available
    func randomBytes(size: Int) -> [UInt8] {
        var buffer = [UInt8]()
        var generator = SystemRandomNumberGenerator()
        for index in stride(from: 0, to: size, by: 8) {
            let int64 = Int64.random(in: Int64.min...Int64.max, using: &generator)
            let bytes = withUnsafeBytes(of: int64.bigEndian) { Array($0) }
            if index + bytes.count > size {
                buffer += bytes.dropLast(index + bytes.count - size)
            } else {
                buffer += bytes
            }
        }
        return buffer
    }
}

extension XCTestCase {
    // TODO: remove once XCTest supports async functions
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    func runAsyncAndWaitFor(
        _ closure: @escaping @Sendable () async throws -> Void,
        _ timeout: TimeInterval = 3.0
    ) {
        let finished = expectation(description: "finished")
        Task.detached {
            try await closure()
            finished.fulfill()
        }
        wait(for: [finished], timeout: timeout)
    }
}
