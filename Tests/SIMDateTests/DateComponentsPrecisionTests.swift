import Testing

@testable import SIMDate

#if canImport(FoundationEssentials)
    import FoundationEssentials
#else
    import Foundation
#endif

/// Any number of fractional digits
private let anyFraction = ComponentPrecision.subsecond(precision: .arbitrary)
/// Exactly `n` fractional digits
private func digits(_ n: UInt8) -> ComponentPrecision { .subsecond(precision: .digits(n)) }
private let millis = ComponentPrecision.subsecond(precision: .millis)
private let micros = ComponentPrecision.subsecond(precision: .micros)
private let nanos = ComponentPrecision.subsecond(precision: .nanos)

/// The `range` of `ComponentsOption.datetime(separator:range:)`.
///
/// - The input's precision must lie within the range: coarser than the lower bound or finer than
///   the upper bound is rejected.
/// - Subsecond precision is the number of fractional digits; `.arbitrary` is any count.
/// - Fields up to the upper bound are zero-filled when the input omits them; fields above it are nil.
/// - An offset does not count toward precision.
@Suite("DateComponents decoding: precision range")
struct DateComponentsPrecisionTests {

    @Test("Presets", arguments: [
        DCCase("2020-02-02", components: .dateOnly, expect: DateComponents(year: 2020, month: 2, day: 2)),
        DCCase(reject: "2020-02-02T12", components: .dateOnly),
        DCCase(reject: "2020-02-02T08:00:34.5", components: .dateOnly),
        DCCase(reject: "2020-02-02", components: .datetimeOnly),
        DCCase("2020-02-02T12", components: .datetimeOnly, expect: DateComponents(year: 2020, month: 2, day: 2, hour: 12, minute: 0, second: 0, nanosecond: 0)),
        DCCase("2020-02-02T08:00:34.123456789", components: .datetimeOnly, expect: DateComponents(year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 123_456_789)),
        DCCase("2020-02-02", components: .acceptBareDate, expect: DateComponents(year: 2020, month: 2, day: 2, hour: 0, minute: 0, second: 0, nanosecond: 0)),
        DCCase("2020-02-02T12", components: .acceptBareDate, expect: DateComponents(year: 2020, month: 2, day: 2, hour: 12, minute: 0, second: 0, nanosecond: 0)),
        // no separator admits no time part: the same as `.dateOnly`, whatever the range
        DCCase("2020-02-02", components: .datetime(separator: [], range: .hour ... anyFraction), expect: DateComponents(year: 2020, month: 2, day: 2)),
        DCCase(reject: "2020-02-02T12", components: .datetime(separator: [], range: .hour ... anyFraction)),
    ])
    func presets(_ c: DCCase) { c.check() }

    @Test("Input finer than the upper bound is rejected", arguments: [
        // .hour
        DCCase("2020-02-02T12", components: .datetime(range: .date ... .hour), expect: DateComponents(year: 2020, month: 2, day: 2, hour: 12)),
        DCCase(reject: "2020-02-02T12:30", components: .datetime(range: .date ... .hour)),
        DCCase(reject: "2020-02-02T12:30:00", components: .datetime(range: .date ... .hour)),
        DCCase(reject: "2020-02-02T12:30:00.5", components: .datetime(range: .date ... .hour)),
        // .minute
        DCCase("2020-02-02T12:30", components: .datetime(range: .date ... .minute), expect: DateComponents(year: 2020, month: 2, day: 2, hour: 12, minute: 30)),
        DCCase(reject: "2020-02-02T12:30:45", components: .datetime(range: .date ... .minute)),
        // .second
        DCCase("2020-02-02T08:00:34", components: .datetime(range: .date ... .second), expect: DateComponents(year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34)),
        DCCase(reject: "2020-02-02T08:00:34.5", components: .datetime(range: .date ... .second)),
        // a digit count
        DCCase("2020-02-02T08:00:34.5", components: .datetime(range: .date ... millis), expect: DateComponents(year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 500_000_000)),
        DCCase("2020-02-02T08:00:34.123", components: .datetime(range: .date ... millis), expect: DateComponents(year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 123_000_000)),
        DCCase(reject: "2020-02-02T08:00:34.1234", components: .datetime(range: .date ... millis)),
        DCCase("2020-02-02T08:00:34.123456789", components: .datetime(range: .date ... nanos), expect: DateComponents(year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 123_456_789)),
        DCCase(reject: "2020-02-02T08:00:34.1234567891", components: .datetime(range: .date ... nanos)),
        // .arbitrary: any count, truncated to nanoseconds
        DCCase("2020-02-02T08:00:34.123456789123", components: .datetime(range: .date ... anyFraction), expect: DateComponents(year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 123_456_789)),
    ])
    func upperBound(_ c: DCCase) { c.check() }

    @Test("Input coarser than the lower bound is rejected", arguments: [
        // .hour
        DCCase(reject: "2020-02-02", components: .datetime(range: .hour ... anyFraction)),
        DCCase("2020-02-02T12", components: .datetime(range: .hour ... anyFraction), expect: DateComponents(year: 2020, month: 2, day: 2, hour: 12, minute: 0, second: 0, nanosecond: 0)),
        // .minute
        DCCase(reject: "2020-02-02", components: .datetime(range: .minute ... anyFraction)),
        DCCase(reject: "2020-02-02T12", components: .datetime(range: .minute ... anyFraction)),
        DCCase("2020-02-02T12:30", components: .datetime(range: .minute ... anyFraction), expect: DateComponents(year: 2020, month: 2, day: 2, hour: 12, minute: 30, second: 0, nanosecond: 0)),
        // .second
        DCCase(reject: "2020-02-02T12:30", components: .datetime(range: .second ... anyFraction)),
        DCCase("2020-02-02T12:30:00", components: .datetime(range: .second ... anyFraction), expect: DateComponents(year: 2020, month: 2, day: 2, hour: 12, minute: 30, second: 0, nanosecond: 0)),
        // a digit count
        DCCase(reject: "2020-02-02T08:00:34", components: .datetime(range: digits(1) ... anyFraction)),
        DCCase("2020-02-02T08:00:34.5", components: .datetime(range: digits(1) ... anyFraction), expect: DateComponents(year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 500_000_000)),
        DCCase(reject: "2020-02-02T08:00:34.12", components: .datetime(range: millis ... anyFraction)),
        DCCase("2020-02-02T08:00:34.123", components: .datetime(range: millis ... anyFraction), expect: DateComponents(year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 123_000_000)),
        DCCase("2020-02-02T08:00:34.1234", components: .datetime(range: millis ... anyFraction), expect: DateComponents(year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 123_400_000)),
        DCCase(reject: "2020-02-02T08:00:34.12345678", components: .datetime(range: nanos ... anyFraction)),
        DCCase("2020-02-02T08:00:34.123456789", components: .datetime(range: nanos ... anyFraction), expect: DateComponents(year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 123_456_789)),
        DCCase("2020-02-02T08:00:34.1234567891", components: .datetime(range: nanos ... anyFraction), expect: DateComponents(year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 123_456_789)),
    ])
    func lowerBound(_ c: DCCase) { c.check() }

    @Test("Omitted fields are zero-filled up to the upper bound, nil above it", arguments: [
        DCCase("2020-02-02", components: .datetime(range: .date ... .hour), expect: DateComponents(year: 2020, month: 2, day: 2, hour: 0)),
        DCCase("2020-02-02", components: .datetime(range: .date ... .minute), expect: DateComponents(year: 2020, month: 2, day: 2, hour: 0, minute: 0)),
        DCCase("2020-02-02T12", components: .datetime(range: .date ... .minute), expect: DateComponents(year: 2020, month: 2, day: 2, hour: 12, minute: 0)),
        DCCase("2020-02-02", components: .datetime(range: .date ... .second), expect: DateComponents(year: 2020, month: 2, day: 2, hour: 0, minute: 0, second: 0)),
        DCCase("2020-02-02T12", components: .datetime(range: .hour ... .second), expect: DateComponents(year: 2020, month: 2, day: 2, hour: 12, minute: 0, second: 0)),
        DCCase("2020-02-02T12", components: .datetime(range: .hour ... millis), expect: DateComponents(year: 2020, month: 2, day: 2, hour: 12, minute: 0, second: 0, nanosecond: 0)),
        DCCase("2020-02-02T08:00:34", components: .datetime(range: .second ... millis), expect: DateComponents(year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 0)),
    ])
    func zeroFill(_ c: DCCase) { c.check() }

    @Test("Single-precision ranges accept exactly that precision", arguments: [
        DCCase("2020-02-02T12", components: .datetime(range: .hour ... .hour), expect: DateComponents(year: 2020, month: 2, day: 2, hour: 12)),
        DCCase(reject: "2020-02-02", components: .datetime(range: .hour ... .hour)),
        DCCase(reject: "2020-02-02T12:30", components: .datetime(range: .hour ... .hour)),
        DCCase("2020-02-02T12:30", components: .datetime(range: .minute ... .minute), expect: DateComponents(year: 2020, month: 2, day: 2, hour: 12, minute: 30)),
        DCCase(reject: "2020-02-02T12", components: .datetime(range: .minute ... .minute)),
        DCCase(reject: "2020-02-02T12:30:00", components: .datetime(range: .minute ... .minute)),
        DCCase("2020-02-02T08:00:34", components: .datetime(range: .second ... .second), expect: DateComponents(year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34)),
        DCCase(reject: "2020-02-02T08:00", components: .datetime(range: .second ... .second)),
        DCCase(reject: "2020-02-02T08:00:34.5", components: .datetime(range: .second ... .second)),
        DCCase("2020-02-02T08:00:34.123", components: .datetime(range: millis ... millis), expect: DateComponents(year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 123_000_000)),
        DCCase(reject: "2020-02-02T08:00:34", components: .datetime(range: millis ... millis)),
        DCCase(reject: "2020-02-02T08:00:34.12", components: .datetime(range: millis ... millis)),
        DCCase(reject: "2020-02-02T08:00:34.1234", components: .datetime(range: millis ... millis)),
        DCCase("2020-02-02T08:00:34.5", components: .datetime(range: digits(1) ... digits(1)), expect: DateComponents(year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 500_000_000)),
        DCCase(reject: "2020-02-02T08:00:34.55", components: .datetime(range: digits(1) ... digits(1))),
    ])
    func exact(_ c: DCCase) { c.check() }

    @Test("Fractional digit windows", arguments: [
        DCCase(reject: "2020-02-02T08:00:34.12", components: .datetime(range: millis ... micros)),
        DCCase("2020-02-02T08:00:34.123", components: .datetime(range: millis ... micros), expect: DateComponents(year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 123_000_000)),
        DCCase("2020-02-02T08:00:34.12345", components: .datetime(range: millis ... micros), expect: DateComponents(year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 123_450_000)),
        DCCase("2020-02-02T08:00:34.123456", components: .datetime(range: millis ... micros), expect: DateComponents(year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 123_456_000)),
        DCCase(reject: "2020-02-02T08:00:34.1234567", components: .datetime(range: millis ... micros)),
        // a fraction is optional when the range starts at `.second`
        DCCase("2020-02-02T08:00:34", components: .datetime(range: .second ... millis), expect: DateComponents(year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 0)),
        DCCase("2020-02-02T08:00:34.12", components: .datetime(range: .second ... millis), expect: DateComponents(year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 120_000_000)),
        DCCase(reject: "2020-02-02T08:00:34.1234", components: .datetime(range: .second ... millis)),
        // trailing zeros still count as digits
        DCCase(reject: "2020-02-02T08:00:34.1230", components: .datetime(range: .second ... millis)),
        DCCase(reject: "2020-02-02T08:00:34.10", components: .datetime(range: digits(1) ... digits(1))),
    ])
    func fractionWindows(_ c: DCCase) { c.check() }

    @Test("An offset does not count toward precision", arguments: [
        DCCase("2020-02-02T12Z", components: .datetime(range: .hour ... .hour), expect: DateComponents(timeZone: gmt, year: 2020, month: 2, day: 2, hour: 12)),
        DCCase("2020-02-02T12+05:30", components: .datetime(range: .hour ... .hour), expect: DateComponents(timeZone: east(5, 30), year: 2020, month: 2, day: 2, hour: 12)),
        DCCase("2020-02-02T12-053045", components: .datetime(range: .hour ... .hour), expect: DateComponents(timeZone: west(5, 30, 45), year: 2020, month: 2, day: 2, hour: 12)),
        DCCase(reject: "2020-02-02T12:30Z", components: .datetime(range: .hour ... .hour)),
        DCCase("2020-02-02T12:30-08:00", components: .datetime(range: .date ... .minute), expect: DateComponents(timeZone: west(8), year: 2020, month: 2, day: 2, hour: 12, minute: 30)),
        DCCase(reject: "2020-02-02T12:30:45Z", components: .datetime(range: .date ... .minute)),
        DCCase("2020-02-02T08:00:34+0530", components: .datetime(range: .second ... .second), expect: DateComponents(timeZone: east(5, 30), year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34)),
        DCCase(reject: "2020-02-02T08:00:34.5Z", components: .datetime(range: .second ... .second)),
        DCCase(reject: "2020-02-02T12Z", components: .datetime(range: .minute ... anyFraction)),
    ])
    func offsets(_ c: DCCase) { c.check() }

    @Test("The range combines with the separator", arguments: [
        DCCase("2020-02-02 12:30", components: .datetime(separator: [.space], range: .minute ... .minute), expect: DateComponents(year: 2020, month: 2, day: 2, hour: 12, minute: 30)),
        DCCase(reject: "2020-02-02T12:30", components: .datetime(separator: [.space], range: .minute ... .minute)),
        DCCase(reject: "2020-02-02 12", components: .datetime(separator: [.space], range: .minute ... .minute)),
        DCCase("2020-02-02", components: .datetime(separator: [.T], range: .date ... .hour), expect: DateComponents(year: 2020, month: 2, day: 2, hour: 0)),
        DCCase(reject: "2020-02-02 12", components: .datetime(separator: [.T], range: .date ... .hour)),
    ])
    func withSeparator(_ c: DCCase) { c.check() }
}
