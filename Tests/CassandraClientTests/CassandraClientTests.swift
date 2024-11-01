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

    func testErrorMapping() {
        XCTAssertThrowsError(try self.cassandraClient.run("boom!").wait()) { error in
            XCTAssertEqual(
                error as? CassandraClient.Error,
                .syntaxError("line 1:0 no viable alternative at input \'boom\' ([boom]...)")
            )
        }
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
