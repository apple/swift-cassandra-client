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

import Foundation
import Logging
import NIOConcurrencyHelpers

/// Test-only capturing `LogHandler` — records emitted entries into a lock-protected buffer so tests can
/// assert on level / message / metadata. Shared by the request-logging unit and integration tests.
final class TestLogCapture: Sendable {
    struct Entry {
        let level: Logger.Level
        let message: String
        let metadata: Logger.Metadata
        /// Stringified `LogEvent.error` (the swift-log `error:` param), if any.
        let error: String?
    }

    private let entries = NIOLockedValueBox<[Entry]>([])

    func append(_ entry: Entry) {
        self.entries.withLockedValue { $0.append(entry) }
    }

    var all: [Entry] {
        self.entries.withLockedValue { $0 }
    }

    func clear() {
        self.entries.withLockedValue { $0.removeAll() }
    }
}

struct TestCapturingLogHandler: LogHandler {
    let capture: TestLogCapture
    var logLevel: Logger.Level = .trace
    var metadata: Logger.Metadata = [:]

    subscript(metadataKey key: String) -> Logger.Metadata.Value? {
        get { self.metadata[key] }
        set { self.metadata[key] = newValue }
    }

    // Implements the event-based method so the `error:` param (`event.error`) is captured too.
    func log(event: LogEvent) {
        var merged = self.metadata
        if let metadata = event.metadata {
            merged.merge(metadata) { _, new in new }
        }
        self.capture.append(
            .init(
                level: event.level,
                message: "\(event.message)",
                metadata: merged,
                error: event.error.map { "\($0)" }
            )
        )
    }
}

/// A `Logger` that records into a returned `TestLogCapture`.
func makeCapturingLogger() -> (Logger, TestLogCapture) {
    let capture = TestLogCapture()
    var logger = Logger(label: "capture") { _ in TestCapturingLogHandler(capture: capture) }
    logger.logLevel = .trace
    return (logger, capture)
}
