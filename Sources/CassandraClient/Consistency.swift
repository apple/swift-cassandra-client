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

internal import CDataStaxDriver

extension CassandraClient {
    /// Consistency levels
    public struct Consistency: RawRepresentable, CustomStringConvertible, Sendable, Hashable {
        private enum Backing: String, Hashable {
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
        }

        private let backing: Backing

        public static var any: Self { Self(backing: .any) }
        public static var one: Self { Self(backing: .one) }
        public static var two: Self { Self(backing: .two) }
        public static var three: Self { Self(backing: .three) }
        public static var quorum: Self { Self(backing: .quorum) }
        public static var all: Self { Self(backing: .all) }
        public static var localQuorum: Self { Self(backing: .localQuorum) }
        public static var eachQuorum: Self { Self(backing: .eachQuorum) }
        public static var serial: Self { Self(backing: .serial) }
        public static var localSerial: Self { Self(backing: .localSerial) }
        public static var localOne: Self { Self(backing: .localOne) }

        public var rawValue: String { self.backing.rawValue }

        public var description: String { self.rawValue }

        public init?(rawValue: String) {
            guard let backing = Backing(rawValue: rawValue) else { return nil }
            self.backing = backing
        }

        private init(backing: Backing) {
            self.backing = backing
        }

        var cassConsistency: CassConsistency {
            switch self.backing {
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

        /// The OpenTelemetry `cassandra.consistency.level` token — exhaustive, so a new level can't silently
        /// emit a wrong string.
        var otelToken: String {
            switch self.backing {
            case .any: return "any"
            case .one: return "one"
            case .two: return "two"
            case .three: return "three"
            case .quorum: return "quorum"
            case .all: return "all"
            case .localQuorum: return "local_quorum"
            case .eachQuorum: return "each_quorum"
            case .serial: return "serial"
            case .localSerial: return "local_serial"
            case .localOne: return "local_one"
            }
        }
    }

    /// Serial consistency levels
    public struct SerialConsistency: RawRepresentable, CustomStringConvertible, Sendable, Hashable {
        private enum Backing: String, Hashable {
            case serial
            case localSerial
        }

        private let backing: Backing

        public static var serial: Self { Self(backing: .serial) }
        public static var localSerial: Self { Self(backing: .localSerial) }

        public var rawValue: String { self.backing.rawValue }

        public var description: String { self.rawValue }

        public init?(rawValue: String) {
            guard let backing = Backing(rawValue: rawValue) else { return nil }
            self.backing = backing
        }

        private init(backing: Backing) {
            self.backing = backing
        }

        var cassConsistency: CassConsistency {
            switch self.backing {
            case .serial:
                return CASS_CONSISTENCY_SERIAL
            case .localSerial:
                return CASS_CONSISTENCY_LOCAL_SERIAL
            }
        }
    }
}
