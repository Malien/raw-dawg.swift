import Testing

@testable import SIMDate

#if canImport(FoundationEssentials)
    import FoundationEssentials
#else
    import Foundation
#endif

/// `2020-02-02T08:00:34` with the given offset, as `DateComponents(decodingISO8601:)` reports it
private func t080034(_ timeZone: TimeZone?) -> DateComponents {
    DateComponents(timeZone: timeZone, year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 0)
}

/// `DateComponents(decodingISO8601:timezone:)`. Each `TimeZoneComponents` member accepts exactly
/// one offset form; `.z` only makes `.Z` case-insensitive. An omitted offset is always accepted
/// (`timeZone` stays nil): requiring or imposing a zone is `Date`'s `TimeZoneOption`.
@Suite("DateComponents decoding: timezone")
struct DateComponentsTimeZoneTests {

    @Test("An omitted offset is accepted under any set", arguments: [
        DCCase("2020-02-02T08:00:34", timezone: [], expect: t080034(nil)),
        DCCase("2020-02-02T08:00:34", timezone: [.Z], expect: t080034(nil)),
        DCCase("2020-02-02T08:00:34", timezone: [.`hh:mm:ss`], expect: t080034(nil)),
        DCCase("2020-02-02T08:00:34", timezone: .anyNumeric, expect: t080034(nil)),
        DCCase("2020-02-02T08", timezone: [], expect: DateComponents(year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 0, nanosecond: 0)),
        DCCase("2020-02-02", timezone: [], expect: DateComponents(year: 2020, month: 2, day: 2, hour: 0, minute: 0, second: 0, nanosecond: 0)),
        DCCase("2020-02-02", timezone: [.Z], expect: DateComponents(year: 2020, month: 2, day: 2, hour: 0, minute: 0, second: 0, nanosecond: 0)),
    ])
    func omitted(_ c: DCCase) { c.check() }

    @Test("[] rejects every offset", arguments: [
        DCCase(reject: "2020-02-02T08:00:34Z", timezone: []),
        DCCase(reject: "2020-02-02T08:00:34z", timezone: []),
        DCCase(reject: "2020-02-02T08:00:34+05", timezone: []),
        DCCase(reject: "2020-02-02T08:00:34+0530", timezone: []),
        DCCase(reject: "2020-02-02T08:00:34+05:30", timezone: []),
        DCCase(reject: "2020-02-02T08:00:34+053045", timezone: []),
        DCCase(reject: "2020-02-02T08:00:34+05:30:45", timezone: []),
        DCCase(reject: "2020-02-02T08:00:34-00:00", timezone: []),
        DCCase(reject: "2020-02-02T12Z", timezone: []),
        DCCase(reject: "2020-02-02T12+05", timezone: []),
    ])
    func empty(_ c: DCCase) { c.check() }

    /// `.z` only modifies `.Z` (case-insensitive Zulu); on its own it accepts nothing
    @Test("Zulu: .Z, and .z modifying it", arguments: [
        DCCase("2020-02-02T08:00:34Z", timezone: [.Z], expect: t080034(gmt)),
        DCCase(reject: "2020-02-02T08:00:34z", timezone: [.Z]),
        DCCase("2020-02-02T08:00:34Z", timezone: [.z, .Z], expect: t080034(gmt)),
        DCCase("2020-02-02T08:00:34z", timezone: [.z, .Z], expect: t080034(gmt)),
        DCCase(reject: "2020-02-02T08:00:34Z", timezone: [.z]),
        DCCase(reject: "2020-02-02T08:00:34z", timezone: [.z]),
        DCCase("2020-02-02T12Z", timezone: [.Z], expect: DateComponents(timeZone: gmt, year: 2020, month: 2, day: 2, hour: 12, minute: 0, second: 0, nanosecond: 0)),
        DCCase("2020-02-02T12:30z", timezone: [.z, .Z], expect: DateComponents(timeZone: gmt, year: 2020, month: 2, day: 2, hour: 12, minute: 30, second: 0, nanosecond: 0)),
        // Zulu does not admit numeric offsets, even a zero one
        DCCase(reject: "2020-02-02T08:00:34+00:00", timezone: [.z, .Z]),
        DCCase(reject: "2020-02-02T08:00:34+00", timezone: [.z, .Z]),
        DCCase(reject: "2020-02-02T08:00:34+0000", timezone: [.z, .Z]),
        DCCase(reject: "2020-02-02T08:00:34+00:00:00", timezone: [.z, .Z]),
    ])
    func zulu(_ c: DCCase) { c.check() }

    @Test(".hh", arguments: [
        DCCase("2020-02-02T08:00:34+05", timezone: [.hh], expect: t080034(east(5))),
        DCCase("2020-02-02T08:00:34-11", timezone: [.hh], expect: t080034(west(11))),
        DCCase("2020-02-02T08:00:34-00", timezone: [.hh], expect: t080034(gmt)),
        DCCase("2020-02-02T08:00:34+18", timezone: [.hh], expect: t080034(east(18))),
        DCCase(reject: "2020-02-02T08:00:34Z", timezone: [.hh]),
        DCCase(reject: "2020-02-02T08:00:34+0530", timezone: [.hh]),
        DCCase(reject: "2020-02-02T08:00:34+05:30", timezone: [.hh]),
        DCCase(reject: "2020-02-02T08:00:34+053045", timezone: [.hh]),
        DCCase(reject: "2020-02-02T08:00:34+05:30:45", timezone: [.hh]),
        DCCase(reject: "2020-02-02T08:00:34+0500", timezone: [.hh]),  // zero minutes are still a different form
    ])
    func hh(_ c: DCCase) { c.check() }

    @Test(".hhmm", arguments: [
        DCCase("2020-02-02T08:00:34+0530", timezone: [.hhmm], expect: t080034(east(5, 30))),
        DCCase("2020-02-02T08:00:34-0800", timezone: [.hhmm], expect: t080034(west(8))),
        DCCase("2020-02-02T08:00:34+1800", timezone: [.hhmm], expect: t080034(east(18))),
        DCCase("2020-02-02T12+0530", timezone: [.hhmm], expect: DateComponents(timeZone: east(5, 30), year: 2020, month: 2, day: 2, hour: 12, minute: 0, second: 0, nanosecond: 0)),
        DCCase(reject: "2020-02-02T08:00:34Z", timezone: [.hhmm]),
        DCCase(reject: "2020-02-02T08:00:34+05", timezone: [.hhmm]),
        DCCase(reject: "2020-02-02T08:00:34+05:30", timezone: [.hhmm]),
        DCCase(reject: "2020-02-02T08:00:34+053045", timezone: [.hhmm]),
        DCCase(reject: "2020-02-02T08:00:34+05:30:45", timezone: [.hhmm]),
    ])
    func hhmm(_ c: DCCase) { c.check() }

    @Test(".hh:mm", arguments: [
        DCCase("2020-02-02T08:00:34+05:30", timezone: [.`hh:mm`], expect: t080034(east(5, 30))),
        DCCase("2020-02-02T08:00:34-03:30", timezone: [.`hh:mm`], expect: t080034(west(3, 30))),
        DCCase("2020-02-02T08:00:34-18:00", timezone: [.`hh:mm`], expect: t080034(west(18))),
        DCCase("2020-02-02T12:30+05:30", timezone: [.`hh:mm`], expect: DateComponents(timeZone: east(5, 30), year: 2020, month: 2, day: 2, hour: 12, minute: 30, second: 0, nanosecond: 0)),
        DCCase(reject: "2020-02-02T08:00:34Z", timezone: [.`hh:mm`]),
        DCCase(reject: "2020-02-02T08:00:34+05", timezone: [.`hh:mm`]),
        DCCase(reject: "2020-02-02T08:00:34+0530", timezone: [.`hh:mm`]),
        DCCase(reject: "2020-02-02T08:00:34+053045", timezone: [.`hh:mm`]),
        DCCase(reject: "2020-02-02T08:00:34+05:30:45", timezone: [.`hh:mm`]),
        DCCase(reject: "2020-02-02T08:00:34+05:30:00", timezone: [.`hh:mm`]),  // zero seconds are still a different form
    ])
    func hhColonMm(_ c: DCCase) { c.check() }

    @Test(".hhmmss", arguments: [
        DCCase("2020-02-02T08:00:34+053045", timezone: [.hhmmss], expect: t080034(east(5, 30, 45))),
        DCCase("2020-02-02T08:00:34-000001", timezone: [.hhmmss], expect: t080034(west(0, 0, 1))),
        DCCase("2020-02-02T08:00:34+053000", timezone: [.hhmmss], expect: t080034(east(5, 30))),
        DCCase("2020-02-02T08:00:34+180000", timezone: [.hhmmss], expect: t080034(east(18))),
        DCCase("2020-02-02T12+053045", timezone: [.hhmmss], expect: DateComponents(timeZone: east(5, 30, 45), year: 2020, month: 2, day: 2, hour: 12, minute: 0, second: 0, nanosecond: 0)),
        DCCase(reject: "2020-02-02T08:00:34Z", timezone: [.hhmmss]),
        DCCase(reject: "2020-02-02T08:00:34+05", timezone: [.hhmmss]),
        DCCase(reject: "2020-02-02T08:00:34+0530", timezone: [.hhmmss]),
        DCCase(reject: "2020-02-02T08:00:34+05:30", timezone: [.hhmmss]),
        DCCase(reject: "2020-02-02T08:00:34+05:30:45", timezone: [.hhmmss]),
        DCCase(reject: "2020-02-02T08:00:34+180001", timezone: [.hhmmss]),
        DCCase(reject: "2020-02-02T08:00:34+053060", timezone: [.hhmmss]),
    ])
    func hhmmss(_ c: DCCase) { c.check() }

    @Test(".hh:mm:ss", arguments: [
        DCCase("2020-02-02T08:00:34+05:30:45", timezone: [.`hh:mm:ss`], expect: t080034(east(5, 30, 45))),
        DCCase("2020-02-02T08:00:34-00:00:01", timezone: [.`hh:mm:ss`], expect: t080034(west(0, 0, 1))),
        DCCase("2020-02-02T08:00:34+05:30:00", timezone: [.`hh:mm:ss`], expect: t080034(east(5, 30))),
        DCCase("2020-02-02T08:00:34-18:00:00", timezone: [.`hh:mm:ss`], expect: t080034(west(18))),
        DCCase("2020-02-02T08:00:34.5+05:30:45", timezone: [.`hh:mm:ss`], expect: DateComponents(timeZone: east(5, 30, 45), year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 500_000_000)),
        DCCase(reject: "2020-02-02T08:00:34Z", timezone: [.`hh:mm:ss`]),
        DCCase(reject: "2020-02-02T08:00:34+05", timezone: [.`hh:mm:ss`]),
        DCCase(reject: "2020-02-02T08:00:34+0530", timezone: [.`hh:mm:ss`]),
        DCCase(reject: "2020-02-02T08:00:34+05:30", timezone: [.`hh:mm:ss`]),
        DCCase(reject: "2020-02-02T08:00:34+053045", timezone: [.`hh:mm:ss`]),
        DCCase(reject: "2020-02-02T08:00:34+05:3045", timezone: [.`hh:mm:ss`]),
        DCCase(reject: "2020-02-02T08:00:34+0530:45", timezone: [.`hh:mm:ss`]),
        DCCase(reject: "2020-02-02T08:00:34+18:00:01", timezone: [.`hh:mm:ss`]),
        DCCase(reject: "2020-02-02T08:00:34+05:30:60", timezone: [.`hh:mm:ss`]),
    ])
    func hhColonMmColonSs(_ c: DCCase) { c.check() }

    @Test("Combined sets accept the union of their forms", arguments: [
        // extended forms only
        DCCase("2020-02-02T08:00:34+05", timezone: [.hh, .`hh:mm`, .`hh:mm:ss`], expect: t080034(east(5))),
        DCCase("2020-02-02T08:00:34+05:30", timezone: [.hh, .`hh:mm`, .`hh:mm:ss`], expect: t080034(east(5, 30))),
        DCCase("2020-02-02T08:00:34+05:30:45", timezone: [.hh, .`hh:mm`, .`hh:mm:ss`], expect: t080034(east(5, 30, 45))),
        DCCase(reject: "2020-02-02T08:00:34+0530", timezone: [.hh, .`hh:mm`, .`hh:mm:ss`]),
        DCCase(reject: "2020-02-02T08:00:34+053045", timezone: [.hh, .`hh:mm`, .`hh:mm:ss`]),
        DCCase(reject: "2020-02-02T08:00:34Z", timezone: [.hh, .`hh:mm`, .`hh:mm:ss`]),
        // basic forms only
        DCCase("2020-02-02T08:00:34+05", timezone: [.hh, .hhmm, .hhmmss], expect: t080034(east(5))),
        DCCase("2020-02-02T08:00:34+0530", timezone: [.hh, .hhmm, .hhmmss], expect: t080034(east(5, 30))),
        DCCase("2020-02-02T08:00:34+053045", timezone: [.hh, .hhmm, .hhmmss], expect: t080034(east(5, 30, 45))),
        DCCase(reject: "2020-02-02T08:00:34+05:30", timezone: [.hh, .hhmm, .hhmmss]),
        DCCase(reject: "2020-02-02T08:00:34+05:30:45", timezone: [.hh, .hhmm, .hhmmss]),
        // Zulu alongside a numeric form
        DCCase("2020-02-02T08:00:34Z", timezone: [.Z, .`hh:mm`], expect: t080034(gmt)),
        DCCase("2020-02-02T08:00:34+05:30", timezone: [.Z, .`hh:mm`], expect: t080034(east(5, 30))),
        DCCase(reject: "2020-02-02T08:00:34z", timezone: [.Z, .`hh:mm`]),
        DCCase(reject: "2020-02-02T08:00:34+05", timezone: [.Z, .`hh:mm`]),
    ])
    func combined(_ c: DCCase) { c.check() }

    @Test(".anyNumeric accepts every numeric form but not Zulu", arguments: [
        DCCase("2020-02-02T08:00:34+05", timezone: .anyNumeric, expect: t080034(east(5))),
        DCCase("2020-02-02T08:00:34+0530", timezone: .anyNumeric, expect: t080034(east(5, 30))),
        DCCase("2020-02-02T08:00:34+05:30", timezone: .anyNumeric, expect: t080034(east(5, 30))),
        DCCase("2020-02-02T08:00:34+053045", timezone: .anyNumeric, expect: t080034(east(5, 30, 45))),
        DCCase("2020-02-02T08:00:34+05:30:45", timezone: .anyNumeric, expect: t080034(east(5, 30, 45))),
        DCCase(reject: "2020-02-02T08:00:34Z", timezone: .anyNumeric),
        DCCase(reject: "2020-02-02T08:00:34z", timezone: .anyNumeric),
    ])
    func anyNumeric(_ c: DCCase) { c.check() }

    @Test(".any accepts every form", arguments: [
        DCCase("2020-02-02T08:00:34Z", timezone: .any, expect: t080034(gmt)),
        DCCase("2020-02-02T08:00:34z", timezone: .any, expect: t080034(gmt)),
        DCCase("2020-02-02T08:00:34+05", timezone: .any, expect: t080034(east(5))),
        DCCase("2020-02-02T08:00:34+0530", timezone: .any, expect: t080034(east(5, 30))),
        DCCase("2020-02-02T08:00:34+05:30", timezone: .any, expect: t080034(east(5, 30))),
        DCCase("2020-02-02T08:00:34+053045", timezone: .any, expect: t080034(east(5, 30, 45))),
        DCCase("2020-02-02T08:00:34+05:30:45", timezone: .any, expect: t080034(east(5, 30, 45))),
    ])
    func any(_ c: DCCase) { c.check() }
}
