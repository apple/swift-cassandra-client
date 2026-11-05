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
import Foundation
import NIOConcurrencyHelpers

/// A result that is set once and that any number of tasks can wait for.
final class OneShotSignal: Sendable {
    private enum State {
        case pending([UUID: CheckedContinuation<Void, any Error>])
        case completed(Result<Void, any Error>)
    }

    private let state = NIOLockedValueBox(State.pending([:]))

    /// Sets the result and resumes every waiting task. Later calls do nothing.
    func complete(_ result: Result<Void, any Error>) {
        let waiters: [CheckedContinuation<Void, any Error>] = self.state.withLockedValue { state in
            guard case .pending(let waiters) = state else { return [] }
            state = .completed(result)
            return Array(waiters.values)
        }
        for waiter in waiters {
            waiter.resume(with: result)
        }
    }

    /// Returns once the result is set, or throws it if it is a failure. Throws `CancellationError` if the calling
    /// task is cancelled first.
    func wait() async throws {
        let id = UUID()
        try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, any Error>) in
                let outcome: Result<Void, any Error>? = self.state.withLockedValue { state in
                    switch state {
                    case .completed(let result):
                        return result
                    case .pending(var waiters):
                        // The cancellation handler can run before this point, so check for cancellation here too.
                        if Task.isCancelled {
                            return .failure(CancellationError())
                        }
                        waiters[id] = continuation
                        state = .pending(waiters)
                        return nil
                    }
                }
                if let outcome {
                    continuation.resume(with: outcome)
                }
            }
        } onCancel: {
            let waiter: CheckedContinuation<Void, any Error>? = self.state.withLockedValue { state in
                guard case .pending(var waiters) = state else { return nil }
                let waiter = waiters.removeValue(forKey: id)
                state = .pending(waiters)
                return waiter
            }
            waiter?.resume(throwing: CancellationError())
        }
    }
}
