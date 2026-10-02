//===----------------------------------------------------------------------===//
//
// This source file is part of the Swift Cassandra Client open source project
//
// Copyright (c) 2022 Apple Inc. and the Swift Cassandra Client project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of Swift Cassandra Client project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import Testing

@testable import CassandraClient

struct OneShotSignalTests {
    private struct SignalError: Error, Equatable {}

    @Test
    func waitReturnsOnceCompleted() async throws {
        let signal = OneShotSignal()
        signal.complete(.success(()))
        try await signal.wait()
    }

    @Test
    func everyWaitingTaskResumesWhenCompleted() async throws {
        let signal = OneShotSignal()
        try await withThrowingTaskGroup(of: Void.self) { group in
            for _ in 0..<3 {
                group.addTask { try await signal.wait() }
            }
            signal.complete(.success(()))
            try await group.waitForAll()
        }
    }

    @Test
    func failureIsThrownToTheWaiter() async {
        let signal = OneShotSignal()
        signal.complete(.failure(SignalError()))
        await #expect(throws: SignalError.self) { try await signal.wait() }
    }

    @Test
    func laterCompletionsAreIgnored() async throws {
        let signal = OneShotSignal()
        signal.complete(.success(()))
        signal.complete(.failure(SignalError()))
        try await signal.wait()
    }

    @Test
    func cancellingATaskBeforeItWaitsEndsTheWait() async {
        let signal = OneShotSignal()
        let waiting = Task {
            while !Task.isCancelled { await Task.yield() }
            try await signal.wait()
        }
        waiting.cancel()
        await #expect(throws: CancellationError.self) { try await waiting.value }
    }

    @Test
    func cancellingAWaitingTaskEndsItsWaitAndLeavesTheOthers() async throws {
        let signal = OneShotSignal()
        let cancelled = Task { try await signal.wait() }
        let other = Task { try await signal.wait() }
        try await Task.sleep(for: .milliseconds(50))
        cancelled.cancel()
        await #expect(throws: CancellationError.self) { try await cancelled.value }
        signal.complete(.success(()))
        try await other.value
    }
}
