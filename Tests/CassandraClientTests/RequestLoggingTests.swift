//===----------------------------------------------------------------------===//
//
// This source file is part of the Swift Cassandra Client open source project
//
// Copyright (c) 2022-2025 Apple Inc. and the Swift Cassandra Client project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of Swift Cassandra Client project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import Dispatch
import Foundation
import Logging
import XCTest

@testable import CassandraClient

/// Unit tests for request logging: a pure-categoriser test (no cluster) and direct-helper emission tests
/// using the shared capturing `LogHandler` (`TestLogCapture`) — no cluster, no double-tautology.
final class RequestLoggingTests: XCTestCase {
    /// Categorisation is a pure function — no cluster. Covers at least one case per bucket (incl. a
    /// `server`-default catch-all case) and pins the metric-reused raw values.
    func testCategorization() {
        // transient
        XCTAssertEqual(CassandraClient.Error.readTimeout("").category, .transient)
        XCTAssertEqual(CassandraClient.Error.serverOverloaded("").category, .transient)
        // unavailable (incl. connect-class lib errors)
        XCTAssertEqual(CassandraClient.Error.noHostsAvailable("").category, .unavailable)
        XCTAssertEqual(CassandraClient.Error.unableToConnect("").category, .unavailable)
        // callerFault (incl. bind + encryption-config)
        XCTAssertEqual(CassandraClient.Error.syntaxError("").category, .callerFault)
        XCTAssertEqual(CassandraClient.Error.parameterUnset("").category, .callerFault)
        XCTAssertEqual(CassandraClient.Error.encryptionConfigError("").category, .callerFault)
        // server (default bucket) — incl. errors that only land here via the catch-all, and unprepared
        XCTAssertEqual(CassandraClient.Error.serverError("").category, .server)
        XCTAssertEqual(CassandraClient.Error.notImplemented("").category, .server)
        XCTAssertEqual(CassandraClient.Error.unprepared("").category, .server)
        // internal (wrapper invariant)
        XCTAssertEqual(CassandraClient.Error.internalError("").category, .wrapperInternal)

        // The category raw values are what a future metric tag reuses verbatim — pin them.
        XCTAssertEqual(CassandraClient.ErrorCategory.transient.rawValue, "transient")
        XCTAssertEqual(CassandraClient.ErrorCategory.unavailable.rawValue, "unavailable")
        XCTAssertEqual(CassandraClient.ErrorCategory.callerFault.rawValue, "callerFault")
        XCTAssertEqual(CassandraClient.ErrorCategory.server.rawValue, "server")
        XCTAssertEqual(CassandraClient.ErrorCategory.wrapperInternal.rawValue, "internal")
    }

    /// Emission shape via a direct helper call — no live server, no tautology.
    func testFailureEmissionShape() {
        let (logger, capture) = makeCapturingLogger()
        let error = CassandraClient.Error.syntaxError("bad cql near 'SELET'")

        CassandraClient.RequestLog.logFailure(
            error,
            query: "SELECT * FROM t WHERE id = ?",
            consistency: .localOne,
            startedAt: nil,
            logger: logger
        )

        let entries = capture.all
        XCTAssertEqual(entries.count, 1)
        let entry = entries[0]
        XCTAssertEqual(entry.level, .debug)  // library convention — never warning/error
        XCTAssertEqual(entry.message, "request failed")  // constant message, not the interpolated error
        XCTAssertEqual(entry.metadata[CassandraClient.LogKey.errorCategory], "callerFault")
        XCTAssertEqual(entry.metadata[CassandraClient.LogKey.query], "SELECT * FROM t WHERE id = ?")
        // The error rides swift-log's `error:` param, carrying only the code label — the server-provided
        // message must NOT leak into the message or the error.
        XCTAssertEqual(entry.error, error.shortDescription)
        XCTAssertFalse(entry.message.contains("SELET"))
        XCTAssertFalse((entry.error ?? "").contains("SELET"))
    }

    /// Threshold 0 logs a slow record for a (>0 ms) success.
    func testSlowQueryThresholdZeroLogs() {
        let (logger, capture) = makeCapturingLogger()
        let past = DispatchTime(uptimeNanoseconds: DispatchTime.now().uptimeNanoseconds - 5_000_000)  // ~5 ms ago
        CassandraClient.RequestLog.checkSlowSuccess(
            startedAt: past,
            query: "SELECT 1",
            thresholdMillis: 0,
            logger: logger
        )
        let entries = capture.all
        XCTAssertEqual(entries.count, 1)
        XCTAssertEqual(entries[0].level, .debug)
        XCTAssertEqual(entries[0].metadata[CassandraClient.LogKey.query], "SELECT 1")
        XCTAssertNotNil(entries[0].metadata[CassandraClient.LogKey.latencyMs])
    }

    /// Threshold nil does no timing and logs nothing.
    func testSlowQueryThresholdNilSkips() {
        let (logger, capture) = makeCapturingLogger()
        let past = DispatchTime(uptimeNanoseconds: DispatchTime.now().uptimeNanoseconds - 5_000_000)
        CassandraClient.RequestLog.checkSlowSuccess(
            startedAt: past,
            query: "SELECT 1",
            thresholdMillis: nil,
            logger: logger
        )
        XCTAssertTrue(capture.all.isEmpty)
    }

    /// A high threshold does not log a fast query.
    func testSlowQueryHighThresholdNoLog() {
        let (logger, capture) = makeCapturingLogger()
        let past = DispatchTime(uptimeNanoseconds: DispatchTime.now().uptimeNanoseconds - 5_000_000)
        CassandraClient.RequestLog.checkSlowSuccess(
            startedAt: past,
            query: "SELECT 1",
            thresholdMillis: 600_000,
            logger: logger
        )
        XCTAssertTrue(capture.all.isEmpty)
    }

    /// Query text longer than the cap is truncated in the record.
    func testQueryTruncation() {
        let (logger, capture) = makeCapturingLogger()
        let longQuery = String(repeating: "a", count: 600)
        CassandraClient.RequestLog.logFailure(
            CassandraClient.Error.serverError("x"),
            query: longQuery,
            consistency: nil,
            startedAt: nil,
            logger: logger
        )
        let entries = capture.all
        XCTAssertEqual(entries.count, 1)
        let expected = String(longQuery.prefix(CassandraClient.Configuration.maxLoggedQueryLength)) + "…"
        XCTAssertEqual(entries[0].metadata[CassandraClient.LogKey.query], "\(expected)")
    }

    /// Bound values are absent unless provided (logBoundValues), and are capped when present.
    func testBoundValuesGate() {
        // off (nil) -> no bound-values key
        let (loggerOff, captureOff) = makeCapturingLogger()
        CassandraClient.RequestLog.logFailure(
            CassandraClient.Error.serverError("x"),
            query: "q",
            boundValues: nil,
            logger: loggerOff
        )
        XCTAssertNil(captureOff.all[0].metadata[CassandraClient.LogKey.boundValues])

        // on -> present
        let (loggerOn, captureOn) = makeCapturingLogger()
        CassandraClient.RequestLog.logFailure(
            CassandraClient.Error.serverError("x"),
            query: "q",
            boundValues: "v0=42",
            logger: loggerOn
        )
        XCTAssertEqual(captureOn.all[0].metadata[CassandraClient.LogKey.boundValues], "v0=42")
    }

    /// Each bound value is capped to maxLoggedValueLength.
    func testBoundValueTruncation() {
        let long = String(repeating: "b", count: 100)
        let formatted = CassandraClient.RequestLog.formatValues([.string(long)])
        XCTAssertTrue(formatted.hasSuffix("…"))
        XCTAssertLessThanOrEqual(formatted.count, CassandraClient.Configuration.maxLoggedValueLength + 1)
    }
}
