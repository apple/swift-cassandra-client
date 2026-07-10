//===----------------------------------------------------------------------===//
//
// This source file is part of the Swift Cassandra Client open source project
//
// Copyright (c) 2025 Apple Inc. and the Swift Cassandra Client project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of Swift Cassandra Client project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import Foundation

extension CassandraClient {
    /// Convert a `Foundation.Decimal` to Cassandra's DECIMAL wire form: a big-endian two's-complement
    /// unscaled integer (`varint`) and an `Int32` `scale`, where value = unscaled * 10^(-scale).
    internal static func decimalToVarint(_ value: Foundation.Decimal) -> (varint: [UInt8], scale: Int32) {
        if value.isZero {
            return ([0x00], 0)
        }

        let negative = value < 0
        var magnitude = negative ? -value : value

        // Scale up until the magnitude is integer-valued; this avoids relying on the decimal's
        // internal exponent and never yields a negative scale.
        var scale: Int32 = 0
        while !isInteger(magnitude) {
            magnitude *= 10
            scale += 1
        }

        var bytes = magnitudeBytes(magnitude)
        if negative {
            bytes = twosComplement(bytes)
        }
        return (bytes, scale)
    }

    /// Convert Cassandra's DECIMAL wire form back to a `Foundation.Decimal`.
    /// Returns `nil` if the value exceeds `Foundation.Decimal`'s representable range.
    internal static func varintToDecimal(varint: [UInt8], scale: Int32) -> Foundation.Decimal? {
        guard let first = varint.first else {
            return nil
        }
        // Foundation.Decimal holds a 128-bit (16-byte) mantissa; anything wider cannot be
        // represented, so return nil rather than risk an overflowed, wrong value.
        guard varint.count <= 16 else {
            return nil
        }

        let negative = (first & 0x80) != 0

        var unsigned = Foundation.Decimal(0)
        for byte in varint {
            unsigned = unsigned * 256 + Foundation.Decimal(UInt(byte))
            if unsigned.isNaN { return nil }
        }

        var value: Foundation.Decimal
        if negative {
            let modulus = pow(Foundation.Decimal(256), varint.count)
            if modulus.isNaN { return nil }
            value = unsigned - modulus
        } else {
            value = unsigned
        }

        if scale != 0 {
            let power = pow(Foundation.Decimal(10), abs(Int(scale)))
            if power.isNaN { return nil }
            value = scale > 0 ? value / power : value * power
        }

        return value.isNaN ? nil : value
    }

    /// Whether a non-negative decimal has no fractional part.
    private static func isInteger(_ value: Foundation.Decimal) -> Bool {
        var input = value
        var rounded = Foundation.Decimal()
        NSDecimalRound(&rounded, &input, 0, .down)
        return rounded == input
    }

    /// Truncating (round-toward-zero) integer division behavior, reused across conversions.
    private static let roundDown = NSDecimalNumberHandler(
        roundingMode: .down,
        scale: 0,
        raiseOnExactness: false,
        raiseOnOverflow: false,
        raiseOnUnderflow: false,
        raiseOnDivideByZero: false
    )

    /// Big-endian magnitude bytes of a non-negative, integer-valued decimal, with a leading `0x00`
    /// inserted when the high bit is set so the value is not misread as negative two's complement.
    private static func magnitudeBytes(_ magnitude: Foundation.Decimal) -> [UInt8] {
        let divisor = NSDecimalNumber(value: 256)

        var value = NSDecimalNumber(decimal: magnitude)
        var bytes: [UInt8] = []
        while value.compare(NSDecimalNumber.zero) == .orderedDescending {
            let quotient = value.dividing(by: divisor, withBehavior: self.roundDown)
            let remainder = value.subtracting(quotient.multiplying(by: divisor))
            bytes.append(UInt8(remainder.intValue))
            value = quotient
        }

        if bytes.isEmpty {
            bytes = [0x00]
        }
        bytes.reverse()
        if bytes[0] & 0x80 != 0 {
            bytes.insert(0x00, at: 0)
        }
        return bytes
    }

    /// Two's complement of big-endian magnitude bytes (invert and add one). The input is guaranteed
    /// to have a clear high bit, so the result's high bit is set, marking it negative.
    private static func twosComplement(_ bytes: [UInt8]) -> [UInt8] {
        var result = bytes.map { ~$0 }
        var carry: UInt16 = 1
        for index in stride(from: result.count - 1, through: 0, by: -1) {
            let sum = UInt16(result[index]) + carry
            result[index] = UInt8(sum & 0xFF)
            carry = sum >> 8
        }
        if result[0] & 0x80 == 0 {
            result.insert(0xFF, at: 0)
        }
        return result
    }
}
