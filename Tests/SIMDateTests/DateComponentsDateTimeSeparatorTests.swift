import Testing

@testable import SIMDate

#if canImport(FoundationEssentials)
    import FoundationEssentials
#else
    import Foundation
#endif

/// Any precision, bare dates included: the range of `.acceptBareDate`
private let anyPrecision: ClosedRange<ComponentPrecision> = .date ... .subsecond(precision: .arbitrary)
/// `2020-02-02T08:00:34`, as `DateComponents(decodingISO8601:)` reports it
private let t080034 = DateComponents(year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 0)
/// `2020-02-02`, zero-filled
private let bareDate = DateComponents(year: 2020, month: 2, day: 2, hour: 0, minute: 0, second: 0, nanosecond: 0)

/// The `separator` of `ComponentsOption.datetime(separator:range:)`. Each `DateTimeSeparator` member
/// accepts exactly one character between the date and the time. A bare date has no separator, so the
/// set never affects it; `separator: []` admits no time part and is the same as `.dateOnly`.
@Suite("DateComponents decoding: datetime separator")
struct DateComponentsDateTimeSeparatorTests {

    @Test(".T", arguments: [
        DCCase("2020-02-02T08:00:34", components: .datetime(separator: [.T], range: anyPrecision), expect: t080034),
        DCCase(reject: "2020-02-02t08:00:34", components: .datetime(separator: [.T], range: anyPrecision)),
        DCCase(reject: "2020-02-02 08:00:34", components: .datetime(separator: [.T], range: anyPrecision)),
    ])
    func upperT(_ c: DCCase) { c.check() }

    @Test(".t", arguments: [
        DCCase(reject: "2020-02-02t08:00:34", components: .datetime(separator: [.t], range: anyPrecision)),
        DCCase(reject: "2020-02-02T08:00:34", components: .datetime(separator: [.t], range: anyPrecision)),
        DCCase(reject: "2020-02-02 08:00:34", components: .datetime(separator: [.t], range: anyPrecision)),
    ])
    func lowerT(_ c: DCCase) { c.check() }

    @Test(".space", arguments: [
        DCCase("2020-02-02 08:00:34", components: .datetime(separator: [.space], range: anyPrecision), expect: t080034),
        DCCase(reject: "2020-02-02T08:00:34", components: .datetime(separator: [.space], range: anyPrecision)),
        DCCase(reject: "2020-02-02t08:00:34", components: .datetime(separator: [.space], range: anyPrecision)),
    ])
    func space(_ c: DCCase) { c.check() }

    @Test("Combined sets accept the union of their separators", arguments: [
        DCCase("2020-02-02T08:00:34", components: .datetime(separator: [.t, .T], range: anyPrecision), expect: t080034),
        DCCase("2020-02-02t08:00:34", components: .datetime(separator: [.t, .T], range: anyPrecision), expect: t080034),
        DCCase(reject: "2020-02-02 08:00:34", components: .datetime(separator: [.t, .T], range: anyPrecision)),
        DCCase("2020-02-02T08:00:34", components: .datetime(separator: [.T, .space], range: anyPrecision), expect: t080034),
        DCCase("2020-02-02 08:00:34", components: .datetime(separator: [.T, .space], range: anyPrecision), expect: t080034),
        DCCase(reject: "2020-02-02t08:00:34", components: .datetime(separator: [.T, .space], range: anyPrecision)),
    ])
    func combined(_ c: DCCase) { c.check() }

    @Test("separator: [] is the same as .dateOnly", arguments: [
        DCCase("2020-02-02", components: .datetime(separator: [], range: anyPrecision), expect: DateComponents(year: 2020, month: 2, day: 2)),
        DCCase(reject: "2020-02-02T08:00:34", components: .datetime(separator: [], range: anyPrecision)),
        DCCase(reject: "2020-02-02t08:00:34", components: .datetime(separator: [], range: anyPrecision)),
        DCCase(reject: "2020-02-02 08:00:34", components: .datetime(separator: [], range: anyPrecision)),
        DCCase(reject: "2020-02-02T12", components: .datetime(separator: [], range: anyPrecision)),
        DCCase(reject: "2020-02-02T08:00:34Z", components: .datetime(separator: [], range: anyPrecision)),
        // an offset on a bare date is still malformed
        DCCase(reject: "2020-02-02Z", components: .datetime(separator: [], range: anyPrecision)),
        // a date-only range admits no time part, whatever the separator
        DCCase("2020-02-02", components: .datetime(separator: [.T], range: .date ... .date), expect: DateComponents(year: 2020, month: 2, day: 2)),
        DCCase(reject: "2020-02-02T08:00:34", components: .datetime(separator: [.T], range: .date ... .date)),
    ])
    func empty(_ c: DCCase) { c.check() }

    @Test("Bare dates are unaffected by the set", arguments: [
        DCCase("2020-02-02", components: .datetime(separator: [.T], range: anyPrecision), expect: bareDate),
        DCCase("2020-02-02", components: .datetime(separator: [.t, .T], range: anyPrecision), expect: bareDate),
        DCCase("2020-02-02", components: .datetime(separator: [.space], range: anyPrecision), expect: bareDate),
    ])
    func bareDates(_ c: DCCase) { c.check() }

    @Test("The separator applies at every time precision", arguments: [
        DCCase("2020-02-02 12", components: .datetime(separator: [.space], range: anyPrecision), expect: DateComponents(year: 2020, month: 2, day: 2, hour: 12, minute: 0, second: 0, nanosecond: 0)),
        DCCase(reject: "2020-02-02T12", components: .datetime(separator: [.space], range: anyPrecision)),
        DCCase("2020-02-02t12:30", components: .datetime(separator: [.t, .T], range: anyPrecision), expect: DateComponents(year: 2020, month: 2, day: 2, hour: 12, minute: 30, second: 0, nanosecond: 0)),
        DCCase(reject: "2020-02-02t12:30", components: .datetime(separator: [.t], range: anyPrecision)),
        DCCase(reject: "2020-02-02T12:30", components: .datetime(separator: [.t], range: anyPrecision)),
        DCCase("2020-02-02T08:00:34.5", components: .datetime(separator: [.T], range: anyPrecision), expect: DateComponents(year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 500_000_000)),
        DCCase(reject: "2020-02-02 08:00:34.5", components: .datetime(separator: [.T], range: anyPrecision)),
        DCCase("2020-02-02 08:00:34+05:30", components: .datetime(separator: [.space], range: anyPrecision), expect: DateComponents(timeZone: east(5, 30), year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 0)),
        DCCase(reject: "2020-02-02T08:00:34+05:30", components: .datetime(separator: [.space], range: anyPrecision)),
    ])
    func precisions(_ c: DCCase) { c.check() }

    /// `.t` is not a case-insensitive `.T`, unlike `.z` for Zulu offsets: the two are independent
    @Test("Separator and Zulu case are independent", arguments: [
        DCCase("2020-02-02T08:00:34z", components: .datetime(separator: [.T], range: anyPrecision), expect: DateComponents(timeZone: gmt, year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 0)),
        DCCase("2020-02-02t08:00:34Z", components: .datetime(separator: [.t, .T], range: anyPrecision), timezone: [.Z], expect: DateComponents(timeZone: gmt, year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 0)),
        DCCase(reject: "2020-02-02t08:00:34z", components: .datetime(separator: [.t], range: anyPrecision), timezone: [.Z]),
        DCCase(reject: "2020-02-02T08:00:34Z", components: .datetime(separator: [.t], range: anyPrecision), timezone: [.Z]),
    ])
    func zuluIndependence(_ c: DCCase) { c.check() }

    /// Only `T`, `t` and a single space exist as separators; nothing else is admitted by any set
    @Test("Non-member separators are rejected under the full set", arguments: [
        DCCase(reject: "2020-02-02_08:00:34", components: .datetime(separator: [.t, .T, .space], range: anyPrecision)),
        DCCase(reject: "2020-02-02\t08:00:34", components: .datetime(separator: [.t, .T, .space], range: anyPrecision)),
        DCCase(reject: "2020-02-02\u{00A0}08:00:34", components: .datetime(separator: [.t, .T, .space], range: anyPrecision)),
        DCCase(reject: "2020-02-02  08:00:34", components: .datetime(separator: [.t, .T, .space], range: anyPrecision)),
        DCCase(reject: "2020-02-02T 08:00:34", components: .datetime(separator: [.t, .T, .space], range: anyPrecision)),
        DCCase(reject: "2020-02-0208:00:34", components: .datetime(separator: [.t, .T, .space], range: anyPrecision)),
        DCCase(reject: "2020-02-02x08:00:34", components: .datetime(separator: [.t, .T, .space], range: anyPrecision)),
        DCCase(reject: "2020-02-02\u{0}08:00:34", components: .datetime(separator: [.t, .T, .space], range: anyPrecision)),  // 0x00 | 0x20 == ' '
        DCCase(reject: "2020-02-02\u{0}08:00:34", components: .datetime(separator: [.space], range: anyPrecision)),
    ])
    func nonMembers(_ c: DCCase) { c.check() }
}
