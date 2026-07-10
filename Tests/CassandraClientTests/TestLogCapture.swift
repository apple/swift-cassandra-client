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

/// Test-only capturing `LogHandler` — records emitted entries into a lock-protected buffer so tests can
/// assert on level / message / metadata. Shared by the request-logging unit and integration tests.
final class TestLogCapture: @unchecked Sendable {
    struct Entry {
        let level: Logger.Level
        let message: String
        let metadata: Logger.Metadata
    }

    private let lock = NSLock()
    private var entries: [Entry] = []

    func append(_ entry: Entry) {
        self.lock.lock()
        defer { self.lock.unlock() }
        self.entries.append(entry)
    }

    var all: [Entry] {
        self.lock.lock()
        defer { self.lock.unlock() }
        return self.entries
    }

    func clear() {
        self.lock.lock()
        defer { self.lock.unlock() }
        self.entries.removeAll()
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

    func log(
        level: Logger.Level,
        message: Logger.Message,
        metadata: Logger.Metadata?,
        source: String,
        file: String,
        function: String,
        line: UInt
    ) {
        var merged = self.metadata
        if let metadata {
            merged.merge(metadata) { _, new in new }
        }
        self.capture.append(.init(level: level, message: "\(message)", metadata: merged))
    }
}

/// A `Logger` that records into a returned `TestLogCapture`.
func makeCapturingLogger() -> (Logger, TestLogCapture) {
    let capture = TestLogCapture()
    var logger = Logger(label: "capture") { _ in TestCapturingLogHandler(capture: capture) }
    logger.logLevel = .trace
    return (logger, capture)
}
