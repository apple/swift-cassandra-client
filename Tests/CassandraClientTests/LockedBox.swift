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

import Synchronization

/// Shares a value between closures and tasks in tests. A `Mutex` can't be copied or captured by an escaping
/// closure, so this holds one in a class.
final class LockedBox<Value: Sendable>: Sendable {
    private let mutex: Mutex<Value>

    init(_ value: Value) {
        self.mutex = Mutex(value)
    }

    func withLock<T: Sendable>(_ body: (inout Value) -> T) -> T {
        self.mutex.withLock { body(&$0) }
    }
}
