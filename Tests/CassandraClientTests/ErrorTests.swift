//===----------------------------------------------------------------------===//
//
// This source file is part of the Swift Cassandra Client open source project
//
// Copyright (c) 2022-2026 Apple Inc. and the Swift Cassandra Client project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of Swift Cassandra Client project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import XCTest

@testable import CassandraClient

final class ErrorTests: XCTestCase {
    func testWasRequestUnexecuted() {
        XCTAssertTrue(
            CassandraClient.Error.serverOverloaded(
                "Queried host was overloaded: Server is shutting down"
            ).wasRequestUnexecuted
        )
        XCTAssertTrue(CassandraClient.Error.serverBootstrapping("").wasRequestUnexecuted)

        XCTAssertFalse(CassandraClient.Error.serverOverloaded("Too many requests").wasRequestUnexecuted)
        XCTAssertFalse(CassandraClient.Error.noHostsAvailable("").wasRequestUnexecuted)
        XCTAssertFalse(CassandraClient.Error.requestTimedOut("").wasRequestUnexecuted)
        XCTAssertFalse(CassandraClient.Error.syntaxError("").wasRequestUnexecuted)
        XCTAssertFalse(CassandraClient.Error.unauthorized("").wasRequestUnexecuted)
    }
}
