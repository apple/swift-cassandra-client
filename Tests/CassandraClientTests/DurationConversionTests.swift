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

import Testing

@testable import CassandraClient

struct DurationConversionTests {
    @Test
    func millisecondsRoundUp() throws {
        #expect(try Duration.milliseconds(1500).driverMilliseconds(UInt32.self, name: "t") == 1500)
        #expect(try Duration.microseconds(1500).driverMilliseconds(UInt32.self, name: "t") == 2)
        #expect(try Duration.nanoseconds(1).driverMilliseconds(UInt32.self, name: "t") == 1)
        #expect(try Duration.zero.driverMilliseconds(UInt32.self, name: "t") == 0)
    }

    @Test
    func secondsRoundUp() throws {
        #expect(try Duration.seconds(30).driverSeconds(name: "t") == 30)
        #expect(try Duration.milliseconds(1500).driverSeconds(name: "t") == 2)
        #expect(try Duration.zero.driverSeconds(name: "t") == 0)
    }

    @Test
    func negativeThrows() {
        #expect(throws: CassandraClient.Error.self) {
            try Duration.milliseconds(-1).driverMilliseconds(UInt32.self, name: "t")
        }
        #expect(throws: CassandraClient.Error.self) {
            try Duration.seconds(-1).driverSeconds(name: "t")
        }
    }

    @Test
    func outOfRangeThrows() {
        #expect(throws: CassandraClient.Error.self) {
            try (Duration.milliseconds(Int64(UInt32.max)) + .milliseconds(1)).driverMilliseconds(UInt32.self, name: "t")
        }
        #expect(throws: CassandraClient.Error.self) {
            try Duration.seconds(Int64.max).driverMilliseconds(UInt64.self, name: "t")
        }
    }
}
