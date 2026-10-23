import Testing

@testable import SIMDate

#if canImport(FoundationEssentials)
    import FoundationEssentials
#else
    import Foundation
#endif

/// `Date(decodingISO8601:)` under default options:
/// `components: .datetimeOnly`, `timezone: .requirePresent(accept: .any)`,
/// everything else as for `DateComponents`.
///
/// Expected instants follow Foundation's ISO 8601 calendar, quirks included
/// (Julian before 1582-10-15, no year 0). Inputs Foundation deems invalid are rejected.
@Suite("Date decoding")
struct DateDecodingTests {

    @Test("Well-formed datetimes with an offset", arguments: [
        DateCase("1970-01-01T00:00:00Z", expect: Date(timeIntervalSince1970: 0)),
        DateCase("2001-01-01T00:00:00Z", expect: Date(timeIntervalSinceReferenceDate: 0)),
        DateCase("1969-12-31T23:59:59Z", expect: Date(timeIntervalSince1970: -1)),
        DateCase("2020-02-02T08:00:34Z", expect: at(2020, 2, 2, 8, 0, 34)),
        DateCase("2020-02-02t08:00:34z", expect: at(2020, 2, 2, 8, 0, 34)),
        DateCase("2020-02-02 08:00:34Z", expect: at(2020, 2, 2, 8, 0, 34)),
        DateCase("2020-02-02T08:00:34+00:00", expect: at(2020, 2, 2, 8, 0, 34)),
        DateCase("2020-02-02T08:00:34-00:00", expect: at(2020, 2, 2, 8, 0, 34)),
        DateCase("2020-02-02T08:00:34+05:30", expect: at(2020, 2, 2, 8, 0, 34, offset: east(5, 30))),
        DateCase("2020-02-02T08:00:34+0530", expect: at(2020, 2, 2, 8, 0, 34, offset: east(5, 30))),
        DateCase("2020-02-02T08:00:34+05", expect: at(2020, 2, 2, 8, 0, 34, offset: east(5))),
        DateCase("2020-02-02T08:00:34-08:00", expect: at(2020, 2, 2, 8, 0, 34, offset: west(8))),
        DateCase("2020-02-02T08:00:34-0330", expect: at(2020, 2, 2, 8, 0, 34, offset: west(3, 30))),
        DateCase("2020-02-02T08:00:34+18:00", expect: at(2020, 2, 2, 8, 0, 34, offset: east(18))),
        DateCase("2020-02-02T08:00:34-18:00", expect: at(2020, 2, 2, 8, 0, 34, offset: west(18))),
    ])
    func wellFormed(_ c: DateCase) { c.check() }

    @Test("Reduced precision is zero-filled", arguments: [
        DateCase("2020-02-02T12Z", expect: at(2020, 2, 2, 12)),
        DateCase("2020-02-02T12+05", expect: at(2020, 2, 2, 12, offset: east(5))),
        DateCase("2020-02-02T12:30Z", expect: at(2020, 2, 2, 12, 30)),
        DateCase("2020-02-02T12:30-03:30", expect: at(2020, 2, 2, 12, 30, offset: west(3, 30))),
    ])
    func reducedPrecision(_ c: DateCase) { c.check() }

    @Test("Fractional seconds", arguments: [
        DateCase("2020-02-02T08:00:34.5Z", expect: at(2020, 2, 2, 8, 0, 34, nanosecond: 500_000_000)),
        DateCase("2020-02-02T08:00:34.123Z", expect: at(2020, 2, 2, 8, 0, 34, nanosecond: 123_000_000)),
        DateCase("2020-02-02T08:00:34.123456Z", expect: at(2020, 2, 2, 8, 0, 34, nanosecond: 123_456_000)),
        DateCase("2020-02-02T08:00:34.123456789Z", expect: at(2020, 2, 2, 8, 0, 34, nanosecond: 123_456_789)),
        DateCase("2020-02-02T08:00:34.1234567899Z", expect: at(2020, 2, 2, 8, 0, 34, nanosecond: 123_456_789)),
        DateCase("2020-02-02T08:00:34.999999999Z", expect: at(2020, 2, 2, 8, 0, 34, nanosecond: 999_999_999)),
        DateCase("2020-02-02T08:00:34.5+05:30", expect: at(2020, 2, 2, 8, 0, 34, nanosecond: 500_000_000, offset: east(5, 30))),
        DateCase("1969-12-31T23:59:59.5Z", expect: Date(timeIntervalSince1970: -0.5)),
    ])
    func fractions(_ c: DateCase) { c.check() }

    @Test("Offsets crossing day, month, and year boundaries", arguments: [
        DateCase("2020-01-01T00:30:00+01:00", expect: at(2019, 12, 31, 23, 30)),
        DateCase("2019-12-31T23:30:00-01:00", expect: at(2020, 1, 1, 0, 30)),
        DateCase("2020-03-01T00:00:00+18:00", expect: at(2020, 2, 29, 6)),
        DateCase("2020-02-29T23:00:00-18:00", expect: at(2020, 3, 1, 17)),
    ])
    func offsetCrossings(_ c: DateCase) { c.check() }

    @Test("Historical and far-future dates follow Foundation's calendar", arguments: [
        DateCase("0001-01-01T00:00:00Z", expect: at(1, 1, 1)),
        DateCase("0004-02-29T00:00:00Z", expect: at(4, 2, 29)),
        DateCase("1000-02-29T00:00:00Z", expect: at(1000, 2, 29)),  // Julian leap day
        DateCase("1500-02-29T00:00:00Z", expect: at(1500, 2, 29)),
        DateCase("1582-10-04T00:00:00Z", expect: at(1582, 10, 4)),  // last Julian day
        DateCase("1582-10-15T00:00:00Z", expect: at(1582, 10, 15)),  // first Gregorian day
        DateCase("1600-02-29T00:00:00Z", expect: at(1600, 2, 29)),
        DateCase("2000-02-29T00:00:00Z", expect: at(2000, 2, 29)),
        DateCase("9999-12-31T23:59:59Z", expect: Date(timeIntervalSince1970: 253_402_300_799)),
        // Offsets may push the instant outside 0001–9999
        DateCase("0001-01-01T00:00:00+01:00", expect: at(1, 1, 1, offset: east(1))),
        DateCase("9999-12-31T23:59:59-01:00", expect: at(9999, 12, 31, 23, 59, 59, offset: west(1))),
    ])
    func foundationCalendar(_ c: DateCase) { c.check() }

    @Test("Bare dates are rejected (.datetimeOnly)", .disabled("There is not an option that would allow Date.init to customize the DateComponents.init in such a way"), arguments: [
        "2020-02-02",
        "2020-02-02Z",
        "2020-02-02+05:00",
    ])
    func bareDates(_ input: String) { DateCase(reject: input).check() }

    @Test("A missing offset is rejected (.requirePresent)", .disabled("There is not an option that would allow Date.init to customize the DateComponents.init in such a way"), arguments: [
        "2020-02-02T08:00:34",
        "2020-02-02T08:00:34.5",
        "2020-02-02T08:00",
        "2020-02-02T08",
        "2020-02-02 08:00:34",
    ])
    func missingOffset(_ input: String) { DateCase(reject: input).check() }

    @Test("Malformed input is rejected", arguments: Corpus.malformed)
    func malformed(_ input: String) { DateCase(reject: input).check() }
}

/// The `timezone: TimeZoneOption` parameter is live on `Date`.
@Suite("Date decoding: TimeZoneOption")
struct DateTimeZoneOptionTests {
    static let plus2 = east(2)
    static let plus3 = east(3)
    static let newYork = TimeZone(identifier: "America/New_York")!

    @Test(".assume: the assumed zone applies only when the offset is omitted", arguments: [
        DateCase("2020-02-02T08:00:34", timezone: .assume(plus2, accept: .any), expect: at(2020, 2, 2, 8, 0, 34, offset: east(2))),
        DateCase("2020-02-02T08", timezone: .assume(plus2, accept: .any), expect: at(2020, 2, 2, 8, offset: east(2))),
        DateCase("2020-02-02T08:00:34.5", timezone: .assume(plus2, accept: .any), expect: at(2020, 2, 2, 8, 0, 34, nanosecond: 500_000_000, offset: east(2))),
        DateCase("2020-02-02T08:00:34Z", timezone: .assume(plus2, accept: .any), expect: at(2020, 2, 2, 8, 0, 34)),
        DateCase("2020-02-02T08:00:34+05:00", timezone: .assume(plus2, accept: .any), expect: at(2020, 2, 2, 8, 0, 34, offset: east(5))),
        DateCase("2020-02-02T08:00:34-00:00", timezone: .assume(plus2, accept: .any), expect: at(2020, 2, 2, 8, 0, 34)),
        DateCase("2020-02-02T08:00:34+05:30:45", timezone: .assume(plus2, accept: .any), expect: at(2020, 2, 2, 8, 0, 34, offset: east(5, 30, 45))),
        DateCase("2020-02-02T08:00:34-053045", timezone: .assume(plus2, accept: .any), expect: at(2020, 2, 2, 8, 0, 34, offset: west(5, 30, 45))),
        // Named zone, outside any DST transition (EST = -05:00)
        DateCase("2020-01-15T12:00:00", timezone: .assume(newYork, accept: [.Z]), expect: at(2020, 1, 15, 12, offset: west(5))),
        DateCase("2020-07-15T12:00:00", timezone: .assume(newYork, accept: [.Z]), expect: at(2020, 7, 15, 12, offset: west(4))),
//        DateCase(reject: "2020-02-02", timezone: .assume(plus2, accept: .any)),
//        DateCase(reject: "2021-02-29T00:00:00", timezone: .assume(plus2, accept: [.Z])),
    ])
    func assume(_ c: DateCase) { c.check() }

    @Test(".assume: offsets outside the accepted set are rejected, not replaced by the assumed zone", arguments: [
        DateCase("2020-02-02T08:00:34Z", timezone: .assume(plus2, accept: [.Z]), expect: at(2020, 2, 2, 8, 0, 34)),
        DateCase(reject: "2020-02-02T08:00:34z", timezone: .assume(plus2, accept: [.Z])),
        DateCase(reject: "2020-02-02T08:00:34+05:00", timezone: .assume(plus2, accept: [.Z])),
        DateCase("2020-02-02T08:00:34+05:30", timezone: .assume(plus2, accept: [.`hh:mm`]), expect: at(2020, 2, 2, 8, 0, 34, offset: east(5, 30))),
        DateCase(reject: "2020-02-02T08:00:34+0530", timezone: .assume(plus2, accept: [.`hh:mm`])),
        DateCase(reject: "2020-02-02T08:00:34+05:30:45", timezone: .assume(plus2, accept: [.`hh:mm`])),
        DateCase(reject: "2020-02-02T08:00:34Z", timezone: .assume(newYork, accept: .anyNumeric)),
        DateCase("2020-01-15T12:00:00", timezone: .assume(newYork, accept: .anyNumeric), expect: at(2020, 1, 15, 12, offset: west(5))),
    ])
    func assumeAcceptSubset(_ c: DateCase) { c.check() }

    @Test(".assume(accept: []): offsets in the string are rejected", arguments: [
        DateCase("2020-02-02T08:00:34", timezone: .assume(plus2, accept: []), expect: at(2020, 2, 2, 8, 0, 34, offset: east(2))),
        DateCase(reject: "2020-02-02T08:00:34Z", timezone: .assume(plus2, accept: [])),
        DateCase(reject: "2020-02-02T08:00:34+02:00", timezone: .assume(plus2, accept: [])),
        DateCase(reject: "2020-02-02T08:00:34+02", timezone: .assume(plus2, accept: [])),
        DateCase(reject: "2020-02-02T08:00:34+0200", timezone: .assume(plus2, accept: [])),
        DateCase(reject: "2020-02-02T08:00:34+020000", timezone: .assume(plus2, accept: [])),
        DateCase(reject: "2020-02-02T08:00:34+02:00:00", timezone: .assume(plus2, accept: [])),
    ])
    func assumeAcceptingNothing(_ c: DateCase) { c.check() }

    @Test(".requireOmitted: imposes the zone, rejects any offset", arguments: [
        DateCase("2020-02-02T08:00:34", timezone: .requireOmitted(impose: plus3), expect: at(2020, 2, 2, 8, 0, 34, offset: east(3))),
        DateCase("2020-02-02T08", timezone: .requireOmitted(impose: plus3), expect: at(2020, 2, 2, 8, offset: east(3))),
        DateCase("2020-01-15T12:00:00", timezone: .requireOmitted(impose: newYork), expect: at(2020, 1, 15, 12, offset: west(5))),
        DateCase(reject: "2020-02-02T08:00:34Z", timezone: .requireOmitted(impose: plus3)),
        DateCase(reject: "2020-02-02T08:00:34z", timezone: .requireOmitted(impose: plus3)),
        DateCase(reject: "2020-02-02T08:00:34+03:00", timezone: .requireOmitted(impose: plus3)),
        DateCase(reject: "2020-02-02T08:00:34+03", timezone: .requireOmitted(impose: plus3)),
        DateCase(reject: "2020-02-02T08:00:34-00:00", timezone: .requireOmitted(impose: plus3)),
        DateCase(reject: "2020-02-02T08:00:34+0300", timezone: .requireOmitted(impose: plus3)),
        DateCase(reject: "2020-02-02T08:00:34+030000", timezone: .requireOmitted(impose: plus3)),
        DateCase(reject: "2020-02-02T08:00:34+03:00:00", timezone: .requireOmitted(impose: plus3)),
        // TODO: revisit once we gain proper `components:` param
        // DateCase(reject: "2020-02-02", timezone: .requireOmitted(impose: plus3)),
    ])
    func requireOmitted(_ c: DateCase) { c.check() }

    /// `.z` only modifies `.Z` (case-insensitive Zulu); on its own it accepts nothing
    @Test(".requirePresent: Zulu acceptance", arguments: [
        DateCase("2020-02-02T08:00:34Z", timezone: .requirePresent(accept: [.Z]), expect: at(2020, 2, 2, 8, 0, 34)),
        DateCase(reject: "2020-02-02T08:00:34z", timezone: .requirePresent(accept: [.Z])),
        DateCase(reject: "2020-02-02T08:00:34+00:00", timezone: .requirePresent(accept: [.Z])),
        DateCase(reject: "2020-02-02T08:00:34+00", timezone: .requirePresent(accept: [.Z])),
        DateCase("2020-02-02T08:00:34Z", timezone: .requirePresent(accept: [.z, .Z]), expect: at(2020, 2, 2, 8, 0, 34)),
        DateCase("2020-02-02T08:00:34z", timezone: .requirePresent(accept: [.z, .Z]), expect: at(2020, 2, 2, 8, 0, 34)),
        DateCase(reject: "2020-02-02T08:00:34Z", timezone: .requirePresent(accept: [.z])),
        DateCase(reject: "2020-02-02T08:00:34z", timezone: .requirePresent(accept: [.z])),
        DateCase(reject: "2020-02-02T08:00:34", timezone: .requirePresent(accept: [.z, .Z])),
    ])
    func zulu(_ c: DateCase) { c.check() }

    @Test(".requirePresent: each numeric form accepts only itself", arguments: [
        // .hh
        DateCase("2020-02-02T08:00:34+05", timezone: .requirePresent(accept: [.hh]), expect: at(2020, 2, 2, 8, 0, 34, offset: east(5))),
        DateCase(reject: "2020-02-02T08:00:34+05:00", timezone: .requirePresent(accept: [.hh])),
        DateCase(reject: "2020-02-02T08:00:34+0500", timezone: .requirePresent(accept: [.hh])),
        DateCase(reject: "2020-02-02T08:00:34Z", timezone: .requirePresent(accept: [.hh])),
        // .hhmm
        DateCase("2020-02-02T08:00:34+0530", timezone: .requirePresent(accept: [.hhmm]), expect: at(2020, 2, 2, 8, 0, 34, offset: east(5, 30))),
        DateCase(reject: "2020-02-02T08:00:34+05:30", timezone: .requirePresent(accept: [.hhmm])),
        DateCase(reject: "2020-02-02T08:00:34+05", timezone: .requirePresent(accept: [.hhmm])),
        DateCase(reject: "2020-02-02T08:00:34+053045", timezone: .requirePresent(accept: [.hhmm])),
        // .hh:mm
        DateCase("2020-02-02T08:00:34+05:30", timezone: .requirePresent(accept: [.`hh:mm`]), expect: at(2020, 2, 2, 8, 0, 34, offset: east(5, 30))),
        DateCase(reject: "2020-02-02T08:00:34+0530", timezone: .requirePresent(accept: [.`hh:mm`])),
        DateCase(reject: "2020-02-02T08:00:34+05", timezone: .requirePresent(accept: [.`hh:mm`])),
        DateCase(reject: "2020-02-02T08:00:34+05:30:45", timezone: .requirePresent(accept: [.`hh:mm`])),
        // .hhmmss
        DateCase("2020-02-02T08:00:34+053045", timezone: .requirePresent(accept: [.hhmmss]), expect: at(2020, 2, 2, 8, 0, 34, offset: east(5, 30, 45))),
        DateCase("2020-02-02T08:00:34-000001", timezone: .requirePresent(accept: [.hhmmss]), expect: at(2020, 2, 2, 8, 0, 34, offset: west(0, 0, 1))),
        DateCase(reject: "2020-02-02T08:00:34+05:30:45", timezone: .requirePresent(accept: [.hhmmss])),
        DateCase(reject: "2020-02-02T08:00:34+0530", timezone: .requirePresent(accept: [.hhmmss])),
        DateCase(reject: "2020-02-02T08:00:34+180001", timezone: .requirePresent(accept: [.hhmmss])),
        // .hh:mm:ss
        DateCase("2020-02-02T08:00:34+05:30:45", timezone: .requirePresent(accept: [.`hh:mm:ss`]), expect: at(2020, 2, 2, 8, 0, 34, offset: east(5, 30, 45))),
        DateCase("2020-02-02T08:00:34-00:00:01", timezone: .requirePresent(accept: [.`hh:mm:ss`]), expect: at(2020, 2, 2, 8, 0, 34, offset: west(0, 0, 1))),
        DateCase(reject: "2020-02-02T08:00:34+053045", timezone: .requirePresent(accept: [.`hh:mm:ss`])),
        DateCase(reject: "2020-02-02T08:00:34+05:30", timezone: .requirePresent(accept: [.`hh:mm:ss`])),
        DateCase(reject: "2020-02-02T08:00:34+05:30:60", timezone: .requirePresent(accept: [.`hh:mm:ss`])),
        DateCase(reject: "2020-02-02T08:00:34+18:00:01", timezone: .requirePresent(accept: [.`hh:mm:ss`])),
        DateCase(reject: "2020-02-02T08:00:34+05:3045", timezone: .requirePresent(accept: [.`hh:mm:ss`])),
        DateCase(reject: "2020-02-02T08:00:34+0530:45", timezone: .requirePresent(accept: [.`hh:mm:ss`])),
    ])
    func numericOffsets(_ c: DateCase) { c.check() }

    @Test(".requirePresent: .anyNumeric and .any", arguments: [
        DateCase("2020-02-02T08:00:34+05", timezone: .requirePresent(accept: .anyNumeric), expect: at(2020, 2, 2, 8, 0, 34, offset: east(5))),
        DateCase("2020-02-02T08:00:34+0530", timezone: .requirePresent(accept: .anyNumeric), expect: at(2020, 2, 2, 8, 0, 34, offset: east(5, 30))),
        DateCase("2020-02-02T08:00:34+05:30", timezone: .requirePresent(accept: .anyNumeric), expect: at(2020, 2, 2, 8, 0, 34, offset: east(5, 30))),
        DateCase("2020-02-02T08:00:34+053045", timezone: .requirePresent(accept: .anyNumeric), expect: at(2020, 2, 2, 8, 0, 34, offset: east(5, 30, 45))),
        DateCase("2020-02-02T08:00:34+05:30:45", timezone: .requirePresent(accept: .anyNumeric), expect: at(2020, 2, 2, 8, 0, 34, offset: east(5, 30, 45))),
        DateCase(reject: "2020-02-02T08:00:34Z", timezone: .requirePresent(accept: .anyNumeric)),
        DateCase(reject: "2020-02-02T08:00:34", timezone: .requirePresent(accept: .anyNumeric)),
        DateCase("2020-02-02T08:00:34Z", timezone: .requirePresent(accept: .any), expect: at(2020, 2, 2, 8, 0, 34)),
        DateCase("2020-02-02T08:00:34z", timezone: .requirePresent(accept: .any), expect: at(2020, 2, 2, 8, 0, 34)),
        DateCase("2020-02-02T08:00:34-053045", timezone: .requirePresent(accept: .any), expect: at(2020, 2, 2, 8, 0, 34, offset: west(5, 30, 45))),
        DateCase(reject: "2020-02-02T08:00:34", timezone: .requirePresent(accept: .any)),
    ])
    func anyOffset(_ c: DateCase) { c.check() }

}

/// `ComponentsOption.datetime(separator:range:)` on `Date`; the full matrix lives in `DateComponentsDateTimeSeparatorTests`
@Suite("Date decoding: datetime separator")
struct DateDateTimeSeparatorTests {

    @Test("The separator set is respected", arguments: [
        DateCase("2020-02-02T08:00:34Z", components: .datetime(separator: [.T], range: .hour ... .subsecond(precision: .arbitrary)), expect: at(2020, 2, 2, 8, 0, 34)),
        DateCase(reject: "2020-02-02t08:00:34Z", components: .datetime(separator: [.T], range: .hour ... .subsecond(precision: .arbitrary))),
        DateCase(reject: "2020-02-02 08:00:34Z", components: .datetime(separator: [.T], range: .hour ... .subsecond(precision: .arbitrary))),
        DateCase("2020-02-02 08:00:34+05:30", components: .datetime(separator: [.space], range: .hour ... .subsecond(precision: .arbitrary)), expect: at(2020, 2, 2, 8, 0, 34, offset: east(5, 30))),
        DateCase(reject: "2020-02-02T08:00:34+05:30", components: .datetime(separator: [.space], range: .hour ... .subsecond(precision: .arbitrary))),
        DateCase("2020-02-02t12Z", components: .datetime(separator: [.t, .T], range: .hour ... .subsecond(precision: .arbitrary)), expect: at(2020, 2, 2, 12)),
        DateCase(reject: "2020-02-02t12Z", components: .datetime(separator: [.t, .space], range: .hour ... .subsecond(precision: .arbitrary))),
        DateCase(reject: "2020-02-02T12Z", components: .datetime(separator: [.t, .space], range: .hour ... .subsecond(precision: .arbitrary))),
        DateCase("2020-02-02 08:00:34", components: .datetime(separator: [.space], range: .hour ... .subsecond(precision: .arbitrary)), timezone: .requireOmitted(impose: east(2)), expect: at(2020, 2, 2, 8, 0, 34, offset: east(2))),
        DateCase(reject: "2020-02-02T08:00:34", components: .datetime(separator: [.space], range: .hour ... .subsecond(precision: .arbitrary)), timezone: .requireOmitted(impose: east(2))),
    ])
    func separators(_ c: DateCase) { c.check() }
}
