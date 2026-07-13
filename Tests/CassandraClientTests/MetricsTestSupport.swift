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

import CoreMetrics
import MetricsTestKit

/// Shared, one-shot metrics bootstrap for the test suite.
///
/// `MetricsSystem.bootstrap` is process-global and may be called only once, so every test that
/// inspects metrics shares this single `TestMetrics` and bootstraps through `bootstrap()`.
enum MetricsTestSupport {
    static let testMetrics = TestMetrics()

    private static let bootstrapOnce: Void = {
        MetricsSystem.bootstrap(MetricsTestSupport.testMetrics)
    }()

    /// Bootstrap once; safe to call from every `setUp`.
    static func bootstrap() {
        _ = Self.bootstrapOnce
    }
}
