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

@_implementationOnly import CDataStaxDriver

extension CassandraClient {
    /// Consistency levels
    public enum Consistency: Hashable {
        case any
        case one
        case two
        case three
        case quorum
        case all
        case localQuorum
        case eachQuorum
        case serial
        case localSerial
        case localOne

        var cassConsistency: CassConsistency {
            switch self {
            case .any:
                return CASS_CONSISTENCY_ANY
            case .one:
                return CASS_CONSISTENCY_ONE
            case .two:
                return CASS_CONSISTENCY_TWO
            case .three:
                return CASS_CONSISTENCY_THREE
            case .quorum:
                return CASS_CONSISTENCY_QUORUM
            case .all:
                return CASS_CONSISTENCY_ALL
            case .localQuorum:
                return CASS_CONSISTENCY_LOCAL_QUORUM
            case .eachQuorum:
                return CASS_CONSISTENCY_EACH_QUORUM
            case .serial:
                return CASS_CONSISTENCY_SERIAL
            case .localSerial:
                return CASS_CONSISTENCY_LOCAL_SERIAL
            case .localOne:
                return CASS_CONSISTENCY_LOCAL_ONE
            }
        }
    }
}
