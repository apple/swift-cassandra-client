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
/// is what lets the driver replay it on another coordinator.
///
/// These cover the option's own contract only: its default, that each setting is carried rather than
/// dropped, and that it is reported. They deliberately stop short of asserting that the value
/// reaches the driver, because nothing here can observe that. The setter always reports success and
/// the declaration holding the value is private to the C++ sources, so a test that built a statement
/// and checked for no error would pass just as happily with the option ignored.
///
/// - Note: Imported without `@testable` (unlike the rest of the suite) so that the file stops
///   compiling if the option loses its `public` access level.
final class StatementIdempotencyTests: XCTestCase {
    /// `false` by default, so the driver keeps treating statements as non-idempotent. Anything else
    /// would silently make every existing caller's writes replayable.
    func testDefaultsToFalse() {
        XCTAssertFalse(CassandraClient.Statement.Options().isIdempotent)
        XCTAssertFalse(CassandraClient.Statement.Options(consistency: .quorum).isIdempotent)
        XCTAssertFalse(
            CassandraClient.Statement.Options(consistency: .quorum, requestTimeout: .seconds(1)).isIdempotent
        )
    }

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
            requestTimeout: .seconds(1),
            isIdempotent: true
        )
        XCTAssertTrue(options.isIdempotent)
        XCTAssertEqual(options.consistency, .quorum)
        XCTAssertEqual(options.requestTimeout, .seconds(1))
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
