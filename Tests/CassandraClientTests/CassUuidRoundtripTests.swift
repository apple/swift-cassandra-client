//===----------------------------------------------------------------------===//
//
// This source file is part of the Swift Cassandra Client open source project
//
// Copyright (c) 2022-2023 Apple Inc. and the Swift Cassandra Client project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of Swift Cassandra Client project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import CDataStaxDriver
import Foundation
import XCTest

@testable import CassandraClient

final class CassUuidRoundtripTests: XCTestCase {
    func testCassUuidRoundtrip() {
        let input = UUID()
        let cassUuid = CassUuid(input.uuid)
        let roundtripped = cassUuid.uuid()

        print("input:        \(input.uuidString)")
        print("roundtripped: \(roundtripped.uuidString)")

        XCTAssertEqual(
            input,
            roundtripped,
            "UUID roundtrip corrupted: \(input.uuidString) → \(roundtripped.uuidString)"
        )
    }
}
