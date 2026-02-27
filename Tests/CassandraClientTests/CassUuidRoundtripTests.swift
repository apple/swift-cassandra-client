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

        XCTAssertEqual(input, roundtripped, "UUID roundtrip corrupted: \(input.uuidString) → \(roundtripped.uuidString)")
    }
}
