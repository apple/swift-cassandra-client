//===----------------------------------------------------------------------===//
//
// This source file is part of the Swift Cassandra Client open source project
//
// Copyright (c) 2026 Apple Inc. and the Swift Cassandra Client project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of Swift Cassandra Client project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import CassandraClient
import XCTest

/// Unit tests for ``CassandraClient/Statement/Options/isIdempotent``. Marking a statement idempotent
/// is what lets the driver replay it on another coordinator, so the option has to survive the trip
/// from `Options` into the statement the driver executes.
///
/// The driver offers no way to read the flag back: the setter always reports success, and the
/// declaration that holds the value is private to the C++ sources. So the failover this unlocks is
/// only observable against a cluster with a node actually refusing requests, which is beyond what
/// this suite can stand up. What is pinned here is the option's contract: its default, that it
/// reaches statement construction for both plain and parameterized queries, and that it is carried
/// rather than dropped.
///
/// - Note: Imported without `@testable` (unlike the rest of the suite) so that the file stops
///   compiling if the option loses its `public` access level.
final class StatementIdempotencyTests: XCTestCase {
    /// Unset by default, so the driver keeps treating statements as non-idempotent. Anything else
    /// would silently make every existing caller's writes replayable.
    func testDefaultsToUnset() {
        XCTAssertNil(CassandraClient.Statement.Options().isIdempotent)
        XCTAssertNil(CassandraClient.Statement.Options(consistency: .quorum).isIdempotent)
        XCTAssertNil(
            CassandraClient.Statement.Options(consistency: .quorum, requestTimeout: 1000).isIdempotent
        )
    }

    /// `nil` and `false` are distinct inputs that reach the driver differently (one leaves the
    /// default in place, the other sets it), so the option keeps them apart instead of collapsing
    /// to a plain `Bool`.
    func testCarriesEachSetting() {
        for setting in [true, false] {
            var options = CassandraClient.Statement.Options()
            options.isIdempotent = setting
            XCTAssertEqual(options.isIdempotent, setting)
        }
    }

    func testInitializerAcceptsIdempotency() {
        let options = CassandraClient.Statement.Options(
            consistency: .quorum,
            requestTimeout: 1000,
            isIdempotent: true
        )
        XCTAssertEqual(options.isIdempotent, true)
        XCTAssertEqual(options.consistency, .quorum)
        XCTAssertEqual(options.requestTimeout, 1000)
    }

    /// The option is applied while the statement is being built, alongside consistency and the
    /// request timeout. Building one for each setting pins that the apply step accepts them all
    /// rather than rejecting a value the driver is fine with.
    func testStatementConstructionAcceptsEachSetting() throws {
        for setting in [nil, true, false] as [Bool?] {
            var options = CassandraClient.Statement.Options()
            options.isIdempotent = setting
            XCTAssertNoThrow(
                try CassandraClient.Statement(query: "select id from test;", options: options),
                "isIdempotent \(String(describing: setting)) should be accepted"
            )
        }
    }

    /// Parameterized statements bind their values before the options are applied, so they exercise
    /// a longer path to the same apply step.
    func testParameterizedStatementConstructionAcceptsIdempotency() throws {
        let options = CassandraClient.Statement.Options(isIdempotent: true)
        XCTAssertNoThrow(
            try CassandraClient.Statement(
                query: "select id from test where id = ?;",
                parameters: [.int32(1)],
                options: options
            )
        )
    }

    /// `Options` is printed when a query is logged or traced, and a replayable statement is worth
    /// seeing there.
    func testDescriptionReportsIdempotency() {
        XCTAssertTrue(
            CassandraClient.Statement.Options(isIdempotent: true).description.contains("isIdempotent"),
            "description should report the idempotency setting"
        )
    }
}
