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

import CDataStaxDriver
import Testing

@testable import CassandraClient

struct ConsistencyTests {
    @Test
    func everyLevelMapsToItsDriverConstant() {
        let levels: [(CassandraClient.Consistency, CassConsistency)] = [
            (.any, CASS_CONSISTENCY_ANY),
            (.one, CASS_CONSISTENCY_ONE),
            (.two, CASS_CONSISTENCY_TWO),
            (.three, CASS_CONSISTENCY_THREE),
            (.quorum, CASS_CONSISTENCY_QUORUM),
            (.all, CASS_CONSISTENCY_ALL),
            (.localQuorum, CASS_CONSISTENCY_LOCAL_QUORUM),
            (.eachQuorum, CASS_CONSISTENCY_EACH_QUORUM),
            (.serial, CASS_CONSISTENCY_SERIAL),
            (.localSerial, CASS_CONSISTENCY_LOCAL_SERIAL),
            (.localOne, CASS_CONSISTENCY_LOCAL_ONE),
        ]
        for (level, driver) in levels {
            #expect(level.cassConsistency == driver)
        }
    }

    @Test
    func everySerialLevelMapsToItsDriverConstant() {
        #expect(CassandraClient.SerialConsistency.serial.cassConsistency == CASS_CONSISTENCY_SERIAL)
        #expect(CassandraClient.SerialConsistency.localSerial.cassConsistency == CASS_CONSISTENCY_LOCAL_SERIAL)
    }
}
