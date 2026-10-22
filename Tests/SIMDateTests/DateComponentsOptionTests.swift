import Testing

@testable import SIMDate

#if canImport(FoundationEssentials)
    import FoundationEssentials
#else
    import Foundation
#endif

/// Behaviour agreed for non-default options, recorded ahead of implementation.
/// Remove `.disabled` once the parameters are wired through `DCCase.decode()`.
@Suite("DateComponents decoding: options", .disabled("DateComponents options are not implemented yet"))
struct DateComponentsOptionTests {

    @Test("components: precision ranges", arguments: [
        // Fields above the range's upper bound are nil
        DCCase("2020-02-02T12", components: .hour ... .minute, expect: DateComponents(year: 2020, month: 2, day: 2, hour: 12, minute: 0)),
        DCCase("2020-02-02", components: .dateOnly, expect: DateComponents(year: 2020, month: 2, day: 2)),
        DCCase(reject: "2020-02-02T12", components: .dateOnly),
        DCCase(reject: "2020-02-02", components: .datetimeOnly),
        DCCase("2020-02-02T12", components: .datetimeOnly, expect: DateComponents(year: 2020, month: 2, day: 2, hour: 12, minute: 0, second: 0, nanosecond: 0)),
        // Subsecond bounds are exact digit counts
        DCCase(
            "2020-02-02T08:00:34.123", components: .subsecond(precision: .millis) ... .subsecond(precision: .millis),
            expect: DateComponents(year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 123_000_000)
        ),
        DCCase(reject: "2020-02-02T08:00:34.12", components: .subsecond(precision: .millis) ... .subsecond(precision: .millis)),
        DCCase(reject: "2020-02-02T08:00:34.1234", components: .subsecond(precision: .millis) ... .subsecond(precision: .millis)),
        DCCase(reject: "2020-02-02T08:00:34", components: .subsecond(precision: .millis) ... .subsecond(precision: .millis)),
        DCCase(reject: "2020-02-02T08:00:34.1234", components: .date ... .subsecond(precision: .millis)),
    ])
    func components(_ c: DCCase) { c.check() }

    @Test("decimalSeparator", arguments: [
        DCCase("2020-02-02T08:00:34,5", decimalSeparator: .comma, expect: DateComponents(year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 500_000_000)),
        DCCase(reject: "2020-02-02T08:00:34.5", decimalSeparator: .comma),
        DCCase("2020-02-02T08:00:34,5", decimalSeparator: .acceptAny, expect: DateComponents(year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 500_000_000)),
        DCCase("2020-02-02T08:00:34.5", decimalSeparator: .acceptAny, expect: DateComponents(year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 500_000_000)),
    ])
    func decimalSeparator(_ c: DCCase) { c.check() }

    @Test("timezone: .z modifies .Z", arguments: [
        DCCase("2020-02-02T12Z", timezone: [.Z], expect: DateComponents(timeZone: gmt, year: 2020, month: 2, day: 2, hour: 12, minute: 0, second: 0, nanosecond: 0)),
        DCCase(reject: "2020-02-02T12z", timezone: [.Z]),
        DCCase(reject: "2020-02-02T12Z", timezone: [.z]),
        DCCase(reject: "2020-02-02T12z", timezone: [.z]),
        DCCase(reject: "2020-02-02T12Z", timezone: []),
        DCCase("2020-02-02T12", timezone: [], expect: DateComponents(year: 2020, month: 2, day: 2, hour: 12, minute: 0, second: 0, nanosecond: 0)),
    ])
    func timezone(_ c: DCCase) { c.check() }
}
