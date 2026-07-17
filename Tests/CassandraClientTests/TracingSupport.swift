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
import Tracing

// In-memory test tracer: swift-distributed-tracing ships no test tracer, so these tests provide one. It
// captures each span's name / kind / attributes / status / recorded errors and its parent span id, so tests
// can assert real parent-child linkage — not merely that a span exists.

/// Parent-linkage key: the current span's id, carried in `ServiceContext` so a child span can find its parent.
private enum TestSpanIDKey: ServiceContextKey {
    typealias Value = Int
}

extension ServiceContext {
    fileprivate var testSpanID: Int? {
        get { self[TestSpanIDKey.self] }
        set { self[TestSpanIDKey.self] = newValue }
    }
}

/// A captured span — the immutable record a test asserts against.
struct CapturedSpan {
    let operationName: String
    let kind: SpanKind
    let spanID: Int
    let parentSpanID: Int?
    let attributes: [String: String]
    let statusCode: SpanStatus.Code?
    let statusMessage: String?
    /// Stringified errors handed to `recordError` (should never contain the raw server message — see V2b).
    let recordedErrors: [String]
}

/// Test span: accumulates state, and on `end()` flushes an immutable `CapturedSpan` into the tracer.
final class TestSpan: Span, @unchecked Sendable {
    var operationName: String
    private let kind: SpanKind
    private let spanID: Int
    private let parentSpanID: Int?
    private unowned let sink: TestTracer

    private let lock = NSLock()
    private var _attributes = SpanAttributes()
    private var _status: SpanStatus?
    private var _errors: [String] = []

    var context: ServiceContext

    init(operationName: String, kind: SpanKind, parent: ServiceContext, sink: TestTracer) {
        self.operationName = operationName
        self.kind = kind
        self.spanID = sink.nextID()
        self.parentSpanID = parent.testSpanID
        self.sink = sink
        var ctx = parent
        ctx.testSpanID = self.spanID
        self.context = ctx
    }

    var isRecording: Bool { true }

    var attributes: SpanAttributes {
        get { self.lock.withLock { self._attributes } }
        set { self.lock.withLock { self._attributes = newValue } }
    }

    func setStatus(_ status: SpanStatus) {
        self.lock.withLock { self._status = status }
    }

    func recordError(_ error: Error, attributes: SpanAttributes, at instant: @autoclosure () -> some TracerInstant) {
        self.lock.withLock { self._errors.append("\(error)") }
    }

    func addEvent(_ event: SpanEvent) {}
    func addLink(_ link: SpanLink) {}

    func end(at instant: @autoclosure () -> some TracerInstant) {
        let captured = self.lock.withLock {
            CapturedSpan(
                operationName: self.operationName,
                kind: self.kind,
                spanID: self.spanID,
                parentSpanID: self.parentSpanID,
                attributes: self._attributes.prettyStrings,
                statusCode: self._status?.code,
                statusMessage: self._status?.message,
                recordedErrors: self._errors
            )
        }
        self.sink.record(captured)
    }
}

/// In-memory tracer: hands out `TestSpan`s and collects `CapturedSpan`s. Bootstrap once per process (guarded),
/// `reset()` per test.
final class TestTracer: Tracer, @unchecked Sendable {
    private let lock = NSLock()
    private var spans: [CapturedSpan] = []
    private var counter = 0

    func nextID() -> Int {
        self.lock.withLock {
            self.counter += 1
            return self.counter
        }
    }

    func record(_ span: CapturedSpan) {
        self.lock.withLock { self.spans.append(span) }
    }

    /// All spans that have ended, in end order.
    var recorded: [CapturedSpan] {
        self.lock.withLock { self.spans }
    }

    func reset() {
        self.lock.withLock { self.spans.removeAll() }
    }

    func startSpan(
        _ operationName: String,
        context: @autoclosure () -> ServiceContext,
        ofKind kind: SpanKind,
        at instant: @autoclosure () -> some TracerInstant,
        function: String,
        file fileID: String,
        line: UInt
    ) -> TestSpan {
        TestSpan(operationName: operationName, kind: kind, parent: context(), sink: self)
    }

    func forceFlush() {}

    // Instrument conformance — no cross-process propagation needed for these tests.
    func inject<Carrier, Inject>(_ context: ServiceContext, into carrier: inout Carrier, using injector: Inject)
    where Inject: Injector, Carrier == Inject.Carrier {}

    func extract<Carrier, Extract>(_ carrier: Carrier, into context: inout ServiceContext, using extractor: Extract)
    where Extract: Extractor, Carrier == Extract.Carrier {}
}

extension SpanAttributes {
    /// Flatten to `[key: stringValue]` for easy assertion (only string-valued attributes are read here).
    fileprivate var prettyStrings: [String: String] {
        var out: [String: String] = [:]
        // `SpanAttributes` isn't a `Sequence`; this is its own `forEach`, so a for-in loop won't compile.
        // swift-format-ignore: ReplaceForEachWithForLoop
        self.forEach { name, attribute in
            if case .string(let value) = attribute {
                out[name] = value
            }
        }
        return out
    }
}

/// Bootstrap the shared `TestTracer` exactly once (process-global `InstrumentationSystem.bootstrap` is
/// one-shot); every test uses this instance and calls `reset()` in `setUp`.
enum SharedTestTracer {
    static let instance = TestTracer()
    private static let bootstrapOnce: Void = {
        InstrumentationSystem.bootstrap(SharedTestTracer.instance)
    }()

    static func bootstrap() {
        _ = Self.bootstrapOnce
    }
}
