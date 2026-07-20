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
import InMemoryTracing
import Tracing

// Tests use swift-distributed-tracing's shipped `InMemoryTracer`. `CapturedSpan` is a thin read-model over its
// finished spans so assertions read name / kind / attributes / status / parent linkage without unpacking
// `SpanAttributes` at each call site.

/// A finished in-memory span, flattened for assertions.
struct CapturedSpan {
    let operationName: String
    let kind: SpanKind
    let spanID: String
    let parentSpanID: String?
    let attributes: [String: String]
    let statusCode: SpanStatus.Code?
    let statusMessage: String?
    /// Errors recorded on the span (should be empty — the tracing helper records status, not the raw error).
    let recordedErrors: [String]

    init(_ span: FinishedInMemorySpan) {
        self.operationName = span.operationName
        self.kind = span.kind
        self.spanID = span.spanID
        self.parentSpanID = span.parentSpanID
        self.attributes = span.attributes.stringValues
        self.statusCode = span.status?.code
        self.statusMessage = span.status?.message
        self.recordedErrors = span.errors.map { "\($0.error)" }
    }
}

extension SpanAttributes {
    /// Flatten string-valued attributes to `[key: value]` for assertions.
    fileprivate var stringValues: [String: String] {
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

/// Bootstraps the shipped `InMemoryTracer` exactly once (process-global `InstrumentationSystem.bootstrap` is
/// one-shot); every test reads finished spans through `instance` and calls `reset()` in `setUp`.
enum SharedTestTracer {
    static let instance = Recorder()
    private static let bootstrapOnce: Void = {
        InstrumentationSystem.bootstrap(instance.tracer)
    }()

    static func bootstrap() {
        _ = Self.bootstrapOnce
    }

    /// Thin accessor around the shipped `InMemoryTracer`, exposing finished spans as `CapturedSpan`s.
    final class Recorder {
        let tracer = InMemoryTracer()
        var recorded: [CapturedSpan] { self.tracer.finishedSpans.map(CapturedSpan.init) }
        func reset() { self.tracer.clearAll(includingActive: true) }
    }
}
