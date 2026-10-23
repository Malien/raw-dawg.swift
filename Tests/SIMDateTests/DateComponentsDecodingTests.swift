import Testing

@testable import SIMDate

#if canImport(FoundationEssentials)
    import FoundationEssentials
#else
    import Foundation
#endif

/// `DateComponents(decodingISO8601:)` under default options:
/// `components: .acceptBareDate`, `datetimeSeparator: [.t, .T, .space]`,
/// `dateComponents: .separatedByDashWithLeadingZeros`, `timeComponents: .separatedByColonWithLeadingZeros`,
/// `decimalSeparator: .dot`, `timezone: .any`.
@Suite("DateComponents decoding")
struct DateComponentsDecodingTests {

    @Test("Precision levels zero-fill up to the range's upper bound", arguments: [
        DCCase("2020-02-02", expect: DateComponents(year: 2020, month: 2, day: 2, hour: 0, minute: 0, second: 0, nanosecond: 0)),
        DCCase("2020-02-02T12", expect: DateComponents(year: 2020, month: 2, day: 2, hour: 12, minute: 0, second: 0, nanosecond: 0)),
        DCCase("2020-02-02T12:30", expect: DateComponents(year: 2020, month: 2, day: 2, hour: 12, minute: 30, second: 0, nanosecond: 0)),
        DCCase("2020-02-02T08:00:34", expect: DateComponents(year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 0)),
        DCCase("2020-02-02T08:00:34.5", expect: DateComponents(year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 500_000_000)),
        DCCase("2020-02-02T12Z", expect: DateComponents(timeZone: gmt, year: 2020, month: 2, day: 2, hour: 12, minute: 0, second: 0, nanosecond: 0)),
        DCCase("2020-02-02T12:30Z", expect: DateComponents(timeZone: gmt, year: 2020, month: 2, day: 2, hour: 12, minute: 30, second: 0, nanosecond: 0)),
        DCCase("2020-02-02T12+05", expect: DateComponents(timeZone: east(5), year: 2020, month: 2, day: 2, hour: 12, minute: 0, second: 0, nanosecond: 0)),
        DCCase("2020-02-02T12:30-03:30", expect: DateComponents(timeZone: west(3, 30), year: 2020, month: 2, day: 2, hour: 12, minute: 30, second: 0, nanosecond: 0)),
    ])
    func precision(_ c: DCCase) { c.check() }

    @Test("Calendar date fields", arguments: [
        DCCase("1970-01-01", expect: DateComponents(year: 1970, month: 1, day: 1, hour: 0, minute: 0, second: 0, nanosecond: 0)),
        DCCase("2020-12-31", expect: DateComponents(year: 2020, month: 12, day: 31, hour: 0, minute: 0, second: 0, nanosecond: 0)),
        DCCase("2020-01-31", expect: DateComponents(year: 2020, month: 1, day: 31, hour: 0, minute: 0, second: 0, nanosecond: 0)),
        DCCase("2020-02-29", expect: DateComponents(year: 2020, month: 2, day: 29, hour: 0, minute: 0, second: 0, nanosecond: 0)),
        DCCase("0000-01-01", expect: DateComponents(year: 0, month: 1, day: 1, hour: 0, minute: 0, second: 0, nanosecond: 0)),
        DCCase("0001-01-01", expect: DateComponents(year: 1, month: 1, day: 1, hour: 0, minute: 0, second: 0, nanosecond: 0)),
        DCCase("0099-09-09", expect: DateComponents(year: 99, month: 9, day: 9, hour: 0, minute: 0, second: 0, nanosecond: 0)),
        DCCase("1582-10-10", expect: DateComponents(year: 1582, month: 10, day: 10, hour: 0, minute: 0, second: 0, nanosecond: 0)),  // inside the Julian→Gregorian gap
        DCCase("9999-12-31", expect: DateComponents(year: 9999, month: 12, day: 31, hour: 0, minute: 0, second: 0, nanosecond: 0)),
        DCCase("9999-12-31T23:59:59.999999999Z", expect: DateComponents(timeZone: gmt, year: 9999, month: 12, day: 31, hour: 23, minute: 59, second: 59, nanosecond: 999_999_999)),
    ])
    func calendarDate(_ c: DCCase) { c.check() }

    /// DateComponents only range-checks month 01–12 and day 01–31; `Date` rejects these.
    @Test("Calendar-invalid days are passed through", arguments: [
        DCCase("2021-02-29", expect: DateComponents(year: 2021, month: 2, day: 29, hour: 0, minute: 0, second: 0, nanosecond: 0)),
        DCCase("2020-02-30", expect: DateComponents(year: 2020, month: 2, day: 30, hour: 0, minute: 0, second: 0, nanosecond: 0)),
        DCCase("2020-02-31", expect: DateComponents(year: 2020, month: 2, day: 31, hour: 0, minute: 0, second: 0, nanosecond: 0)),
        DCCase("2020-04-31", expect: DateComponents(year: 2020, month: 4, day: 31, hour: 0, minute: 0, second: 0, nanosecond: 0)),
        DCCase("1900-02-29", expect: DateComponents(year: 1900, month: 2, day: 29, hour: 0, minute: 0, second: 0, nanosecond: 0)),
        DCCase("2021-02-29T08:00:00Z", expect: DateComponents(timeZone: gmt, year: 2021, month: 2, day: 29, hour: 8, minute: 0, second: 0, nanosecond: 0)),
    ])
    func lenientDays(_ c: DCCase) { c.check() }

    @Test("Time field bounds", arguments: [
        DCCase("2020-02-02T00:00:00", expect: DateComponents(year: 2020, month: 2, day: 2, hour: 0, minute: 0, second: 0, nanosecond: 0)),
        DCCase("2020-02-02T23:59:59", expect: DateComponents(year: 2020, month: 2, day: 2, hour: 23, minute: 59, second: 59, nanosecond: 0)),
        DCCase("2020-02-02T00", expect: DateComponents(year: 2020, month: 2, day: 2, hour: 0, minute: 0, second: 0, nanosecond: 0)),
        DCCase("2020-02-02T23", expect: DateComponents(year: 2020, month: 2, day: 2, hour: 23, minute: 0, second: 0, nanosecond: 0)),
        DCCase("2020-02-02T09:09:09", expect: DateComponents(year: 2020, month: 2, day: 2, hour: 9, minute: 9, second: 9, nanosecond: 0)),
    ])
    func timeBounds(_ c: DCCase) { c.check() }

    @Test("Date/time separators", arguments: [
        DCCase("2020-02-02T08:00:34", expect: DateComponents(year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 0)),
        DCCase("2020-02-02t08:00:34", expect: DateComponents(year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 0)),
        DCCase("2020-02-02 08:00:34", expect: DateComponents(year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 0)),
        DCCase("2020-02-02 12", expect: DateComponents(year: 2020, month: 2, day: 2, hour: 12, minute: 0, second: 0, nanosecond: 0)),
        DCCase("2020-02-02t12z", expect: DateComponents(timeZone: gmt, year: 2020, month: 2, day: 2, hour: 12, minute: 0, second: 0, nanosecond: 0)),
    ])
    func separators(_ c: DCCase) { c.check() }

    @Test("Fractional seconds (arbitrary precision, truncated to nanoseconds)", arguments: [
        DCCase("2020-02-02T08:00:34.0", expect: DateComponents(year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 0)),
        DCCase("2020-02-02T08:00:34.1", expect: DateComponents(year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 100_000_000)),
        DCCase("2020-02-02T08:00:34.12", expect: DateComponents(year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 120_000_000)),
        DCCase("2020-02-02T08:00:34.123", expect: DateComponents(year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 123_000_000)),
        DCCase("2020-02-02T08:00:34.0001", expect: DateComponents(year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 100_000)),
        DCCase("2020-02-02T08:00:34.123456", expect: DateComponents(year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 123_456_000)),
        DCCase("2020-02-02T08:00:34.000000001", expect: DateComponents(year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 1)),
        DCCase("2020-02-02T08:00:34.123456789", expect: DateComponents(year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 123_456_789)),
        DCCase("2020-02-02T08:00:34.000000000", expect: DateComponents(year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 0)),
        DCCase("2020-02-02T08:00:34.999999999", expect: DateComponents(year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 999_999_999)),
        // > 9 digits: truncated, never rounded
        DCCase("2020-02-02T08:00:34.1234567899", expect: DateComponents(year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 123_456_789)),
        DCCase("2020-02-02T08:00:34.0000000009", expect: DateComponents(year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 0)),
        DCCase(
            "2020-02-02T23:59:59.9999999999Z",
            expect: DateComponents(timeZone: gmt, year: 2020, month: 2, day: 2, hour: 23, minute: 59, second: 59, nanosecond: 999_999_999)),
        // We put a max 32 byte limit on the datetime string, cause everything larger than that is bound to be an attempt at a DoS
//        DCCase(
//            "2020-02-02T08:00:34." + String(repeating: "9", count: 64),
//            expect: DateComponents(year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 999_999_999)
//        ),
//        DCCase(
//            "2020-02-02T08:00:34." + String(repeating: "0", count: 300) + "1",
//            expect: DateComponents(year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 0)
//        ),
        DCCase("2020-02-02T08:00:34.5Z", expect: DateComponents(timeZone: gmt, year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 500_000_000)),
        DCCase("2020-02-02T08:00:34.5+05:30", expect: DateComponents(timeZone: east(5, 30), year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 500_000_000)),
        DCCase("2020-02-02T08:00:34.5-08", expect: DateComponents(timeZone: west(8), year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 500_000_000)),
    ])
    func fractions(_ c: DCCase) { c.check() }

    @Test("Offsets", arguments: [
        DCCase("2020-02-02T08:00:34", expect: DateComponents(year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 0)),
        DCCase("2020-02-02T08:00:34Z", expect: DateComponents(timeZone: gmt, year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 0)),
        DCCase("2020-02-02T08:00:34z", expect: DateComponents(timeZone: gmt, year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 0)),
        DCCase("2020-02-02T08:00:34+00:00", expect: DateComponents(timeZone: gmt, year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 0)),
        DCCase("2020-02-02T08:00:34-00:00", expect: DateComponents(timeZone: gmt, year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 0)),
        DCCase("2020-02-02T08:00:34+0000", expect: DateComponents(timeZone: gmt, year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 0)),
        DCCase("2020-02-02T08:00:34+00", expect: DateComponents(timeZone: gmt, year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 0)),
        DCCase("2020-02-02T08:00:34-00", expect: DateComponents(timeZone: gmt, year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 0)),
        // `.hh:mm` and `.hhmm`
        DCCase("2020-02-02T08:00:34+05:30", expect: DateComponents(timeZone: east(5, 30), year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 0)),
        DCCase("2020-02-02T08:00:34+0530", expect: DateComponents(timeZone: east(5, 30), year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 0)),
        DCCase("2020-02-02T08:00:34-08:00", expect: DateComponents(timeZone: west(8), year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 0)),
        DCCase("2020-02-02T08:00:34-0800", expect: DateComponents(timeZone: west(8), year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 0)),
        DCCase("2020-02-02T08:00:34-03:30", expect: DateComponents(timeZone: west(3, 30), year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 0)),
        DCCase("2020-02-02T08:00:34+05:45", expect: DateComponents(timeZone: east(5, 45), year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 0)),
        DCCase("2020-02-02T08:00:34-00:30", expect: DateComponents(timeZone: west(0, 30), year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 0)),
        DCCase("2020-02-02T08:00:34+00:59", expect: DateComponents(timeZone: east(0, 59), year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 0)),
        // `.hh`
        DCCase("2020-02-02T08:00:34+05", expect: DateComponents(timeZone: east(5), year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 0)),
        DCCase("2020-02-02T08:00:34-11", expect: DateComponents(timeZone: west(11), year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 0)),
        // `.hh:mm:ss` and `.hhmmss`
        DCCase("2020-02-02T08:00:34+05:30:45", expect: DateComponents(timeZone: east(5, 30, 45), year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 0)),
        DCCase("2020-02-02T08:00:34+053045", expect: DateComponents(timeZone: east(5, 30, 45), year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 0)),
        DCCase("2020-02-02T08:00:34+05:30:00", expect: DateComponents(timeZone: east(5, 30), year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 0)),
        DCCase("2020-02-02T08:00:34-00:00:01", expect: DateComponents(timeZone: west(0, 0, 1), year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 0)),
        DCCase("2020-02-02T08:00:34-000001", expect: DateComponents(timeZone: west(0, 0, 1), year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 0)),
        DCCase("2020-02-02T08:00:34+00:00:59", expect: DateComponents(timeZone: east(0, 0, 59), year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 0)),
        // Every offset form after every time precision
        DCCase("2020-02-02T12+05:30:45", expect: DateComponents(timeZone: east(5, 30, 45), year: 2020, month: 2, day: 2, hour: 12, minute: 0, second: 0, nanosecond: 0)),
        DCCase("2020-02-02T12+053045", expect: DateComponents(timeZone: east(5, 30, 45), year: 2020, month: 2, day: 2, hour: 12, minute: 0, second: 0, nanosecond: 0)),
        DCCase("2020-02-02T12+0530", expect: DateComponents(timeZone: east(5, 30), year: 2020, month: 2, day: 2, hour: 12, minute: 0, second: 0, nanosecond: 0)),
        DCCase("2020-02-02T12:30+05:30:45", expect: DateComponents(timeZone: east(5, 30, 45), year: 2020, month: 2, day: 2, hour: 12, minute: 30, second: 0, nanosecond: 0)),
        DCCase("2020-02-02T12:30-053045", expect: DateComponents(timeZone: west(5, 30, 45), year: 2020, month: 2, day: 2, hour: 12, minute: 30, second: 0, nanosecond: 0)),
        DCCase("2020-02-02T12:30-05", expect: DateComponents(timeZone: west(5), year: 2020, month: 2, day: 2, hour: 12, minute: 30, second: 0, nanosecond: 0)),
        DCCase("2020-02-02T08:00:34.5+05:30:45", expect: DateComponents(timeZone: east(5, 30, 45), year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 500_000_000)),
        DCCase("2020-02-02T08:00:34.123456789-053045", expect: DateComponents(timeZone: west(5, 30, 45), year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 123_456_789)),
        DCCase("2020-02-02T08:00:34.5+0530", expect: DateComponents(timeZone: east(5, 30), year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 500_000_000)),
        // Bounds: ±18:00 inclusive (Foundation's TimeZone limit)
        DCCase("2020-02-02T08:00:34+14:00", expect: DateComponents(timeZone: east(14), year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 0)),
        DCCase("2020-02-02T08:00:34+17:59", expect: DateComponents(timeZone: east(17, 59), year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 0)),
        DCCase("2020-02-02T08:00:34+18:00", expect: DateComponents(timeZone: east(18), year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 0)),
        DCCase("2020-02-02T08:00:34-18:00", expect: DateComponents(timeZone: west(18), year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 0)),
        DCCase("2020-02-02T08:00:34+1800", expect: DateComponents(timeZone: east(18), year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 0)),
        DCCase("2020-02-02T08:00:34-18", expect: DateComponents(timeZone: west(18), year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 0)),
        DCCase("2020-02-02T08:00:34+17:59:59", expect: DateComponents(timeZone: east(17, 59, 59), year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 0)),
        DCCase("2020-02-02T08:00:34+18:00:00", expect: DateComponents(timeZone: east(18), year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 0)),
        DCCase("2020-02-02T08:00:34-180000", expect: DateComponents(timeZone: west(18), year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 0)),
        // Offset wall-clock fields are kept as written, not normalized to UTC
        DCCase("2020-01-01T00:30:00+01:00", expect: DateComponents(timeZone: east(1), year: 2020, month: 1, day: 1, hour: 0, minute: 30, second: 0, nanosecond: 0)),
        DCCase("2020-12-31T23:30:00-01:00", expect: DateComponents(timeZone: west(1), year: 2020, month: 12, day: 31, hour: 23, minute: 30, second: 0, nanosecond: 0)),
    ])
    func offsets(_ c: DCCase) { c.check() }

    @Test("Malformed input is rejected", arguments: Corpus.malformed)
    func malformed(_ input: String) { DCCase(reject: input).check() }

    @Test("Results carry the ISO 8601 calendar")
    func calendar() throws {
        let c = try #require(DateComponents(decodingISO8601: "2020-02-02T08:00:34Z"))
        #expect(c.calendar?.identifier == .iso8601)
        #expect(c.date != nil)
    }

    @Test("Absent offset leaves timeZone nil")
    func absentOffset() throws {
        let c = try #require(DateComponents(decodingISO8601: "2020-02-02T08:00:34"))
        #expect(c.timeZone == nil)
    }
}
