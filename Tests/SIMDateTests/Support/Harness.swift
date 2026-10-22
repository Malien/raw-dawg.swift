import Testing

@testable import SIMDate

#if canImport(FoundationEssentials)
    import FoundationEssentials
#else
    import Foundation
#endif

// MARK: - DateComponents cases

struct DCCase: Sendable, CustomTestStringConvertible {
    let input: String
    /// `nil` means the input must be rejected
    let expected: DateComponents?
    /// Mirrors the (currently commented-out) parameters of `DateComponents(decodingISO8601:...)`.
    /// Defaults must match the ones documented in `Sources/SIMDate.swift`.
    let components: ClosedRange<ComponentPrecision>
    let datetimeSeparator: DateTimeSeparator
    let dateComponents: DateComponentEncoding
    let timeComponents: TimeComponentEncoding
    let decimalSeparator: DecimalSeparator
    let timezone: TimeZoneComponents
    let offsetComponents: TimeComponentEncoding
    let sourceLocation: SourceLocation

    var testDescription: String {
        "\(input.debugDescription) → \(expected.map(Fields.init).map(String.init(describing:)) ?? "reject")"
    }

    init(
        _ input: String,
        components: ClosedRange<ComponentPrecision> = .acceptBareDate,
        datetimeSeparator: DateTimeSeparator = [.t, .T, .space],
        dateComponents: DateComponentEncoding = .separatedByDashWithLeadingZeros,
        timeComponents: TimeComponentEncoding = .separatedByColonWithLeadingZeros,
        decimalSeparator: DecimalSeparator = .dot,
        timezone: TimeZoneComponents = [.z, .Z, .hh, .`hh:mm`],
        offsetComponents: TimeComponentEncoding = .anyWithLeadingZeros,
        expect expected: DateComponents,
        sourceLocation: SourceLocation = #_sourceLocation
    ) {
        self.input = input
        self.expected = expected
        self.components = components
        self.datetimeSeparator = datetimeSeparator
        self.dateComponents = dateComponents
        self.timeComponents = timeComponents
        self.decimalSeparator = decimalSeparator
        self.timezone = timezone
        self.offsetComponents = offsetComponents
        self.sourceLocation = sourceLocation
    }

    init(
        reject input: String,
        components: ClosedRange<ComponentPrecision> = .acceptBareDate,
        datetimeSeparator: DateTimeSeparator = [.t, .T, .space],
        dateComponents: DateComponentEncoding = .separatedByDashWithLeadingZeros,
        timeComponents: TimeComponentEncoding = .separatedByColonWithLeadingZeros,
        decimalSeparator: DecimalSeparator = .dot,
        timezone: TimeZoneComponents = [.z, .Z, .hh, .`hh:mm`],
        offsetComponents: TimeComponentEncoding = .anyWithLeadingZeros,
        sourceLocation: SourceLocation = #_sourceLocation
    ) {
        self.input = input
        self.expected = nil
        self.components = components
        self.datetimeSeparator = datetimeSeparator
        self.dateComponents = dateComponents
        self.timeComponents = timeComponents
        self.decimalSeparator = decimalSeparator
        self.timezone = timezone
        self.offsetComponents = offsetComponents
        self.sourceLocation = sourceLocation
    }

    func check() {
        let defaults = Self(reject: input)
        let isDefaulted =
            components == defaults.components
            && datetimeSeparator == defaults.datetimeSeparator
            && dateComponents == defaults.dateComponents
            && timeComponents == defaults.timeComponents
            && decimalSeparator == defaults.decimalSeparator
            && timezone == defaults.timezone
            && offsetComponents == defaults.offsetComponents
        if !isDefaulted {
            Issue.record(
                "DateComponents options are not wired into the decoder yet",
                sourceLocation: sourceLocation)
        }
        // TODO(options): forward the options once the parameters land
        let actual = DateComponents(decodingISO8601: input)
        guard let expected else {
            #expect(
                actual == nil,
                "expected rejection, got \(actual.map(Fields.init).map(String.init(describing:)) ?? "")",
                sourceLocation: sourceLocation)
            return
        }
        guard let actual else {
            Issue.record(
                "expected \(Fields(expected)), but the input was rejected",
                sourceLocation: sourceLocation)
            return
        }
        #expect(Fields(actual) == Fields(expected), sourceLocation: sourceLocation)
        #expect(actual.calendar?.identifier == .iso8601, sourceLocation: sourceLocation)
    }
}

/// The parts of `DateComponents` the decoder is specified to produce.
/// `calendar` is deliberately excluded; only its identifier is asserted (see `DCCase.check`).
struct Fields: Equatable, CustomStringConvertible {
    var year, month, day, hour, minute, second, nanosecond: Int?
    var timeZone: TimeZone?

    init(_ c: DateComponents) {
        (year, month, day) = (c.year, c.month, c.day)
        (hour, minute, second, nanosecond) = (c.hour, c.minute, c.second, c.nanosecond)
        timeZone = c.timeZone
    }

    var description: String {
        func f(_ v: Int?, _ w: Int = 2) -> String {
            guard let v else { return "nil" }
            let s = String(v)
            return String(repeating: "0", count: max(0, w - s.count)) + s
        }
        var s =
            "\(f(year, 4))-\(f(month))-\(f(day)) \(f(hour)):\(f(minute)):\(f(second)).\(f(nanosecond, 9))"
        s += " tz=\(timeZone.map { String($0.secondsFromGMT()) } ?? "nil")"
        return s
    }
}

/// UTC
let gmt = TimeZone(secondsFromGMT: 0)!
/// Fixed time zone ahead of UTC, e.g. `east(5, 30)` for `+05:30`
func east(_ h: Int, _ m: Int = 0, _ s: Int = 0) -> TimeZone {
    TimeZone(secondsFromGMT: h * 3600 + m * 60 + s)!
}
/// Fixed time zone behind UTC, e.g. `west(8)` for `-08:00`
func west(_ h: Int, _ m: Int = 0, _ s: Int = 0) -> TimeZone {
    TimeZone(secondsFromGMT: -(h * 3600 + m * 60 + s))!
}

// MARK: - Date cases

/// The instant Foundation's ISO 8601 calendar assigns to a wall-clock time in the given time zone. Foundation's quirks, like the Julian calendar before 1582-10-15, are intentionally kept.
func at(
    _ year: Int, _ month: Int, _ day: Int,
    _ hour: Int = 0, _ minute: Int = 0, _ second: Int = 0,
    nanosecond: Int = 0, offset timeZone: TimeZone = gmt
) -> Date {
    var calendar = Calendar(identifier: .iso8601)
    calendar.timeZone = timeZone
    let components = DateComponents(
        calendar: calendar, timeZone: timeZone,
        year: year, month: month, day: day,
        hour: hour, minute: minute, second: second, nanosecond: nanosecond
    )
    precondition(
        components.isValidDate, "test fixture is not a valid Foundation date: \(components)")
    return components.date!
}

struct DateCase: Sendable, CustomTestStringConvertible {
    let input: String
    /// `nil` means the input must be rejected
    let expected: Date?
    /// Mirrors the parameters of `Date(decodingISO8601:...)`.
    /// `timezone` is already live; everything else is still commented out in the source.
    let components: ClosedRange<ComponentPrecision>
    let datetimeSeparator: DateTimeSeparator
    let dateComponents: DateComponentEncoding
    let timeComponents: TimeComponentEncoding
    let decimalSeparator: DecimalSeparator
    let timezone: TimeZoneOption
    let offsetComponents: TimeComponentEncoding
    let sourceLocation: SourceLocation

    /// Date is Double-backed; fractional seconds compare within 1µs
    static let tolerance = 1e-6

    var testDescription: String {
        "\(input.debugDescription) → \(expected.map { "\($0.timeIntervalSince1970)s since 1970" } ?? "reject")"
    }

    init(
        _ input: String,
        components: ClosedRange<ComponentPrecision> = .datetimeOnly,
        datetimeSeparator: DateTimeSeparator = [.t, .T, .space],
        dateComponents: DateComponentEncoding = .separatedByDashWithLeadingZeros,
        timeComponents: TimeComponentEncoding = .separatedByColonWithLeadingZeros,
        decimalSeparator: DecimalSeparator = .dot,
        timezone: TimeZoneOption = .requirePresent(accept: [.z, .Z, .hh, .`hh:mm`]),
        offsetComponents: TimeComponentEncoding = .anyWithLeadingZeros,
        expect expected: Date,
        sourceLocation: SourceLocation = #_sourceLocation
    ) {
        self.input = input
        self.expected = expected
        self.components = components
        self.datetimeSeparator = datetimeSeparator
        self.dateComponents = dateComponents
        self.timeComponents = timeComponents
        self.decimalSeparator = decimalSeparator
        self.timezone = timezone
        self.offsetComponents = offsetComponents
        self.sourceLocation = sourceLocation
    }

    init(
        reject input: String,
        components: ClosedRange<ComponentPrecision> = .datetimeOnly,
        datetimeSeparator: DateTimeSeparator = [.t, .T, .space],
        dateComponents: DateComponentEncoding = .separatedByDashWithLeadingZeros,
        timeComponents: TimeComponentEncoding = .separatedByColonWithLeadingZeros,
        decimalSeparator: DecimalSeparator = .dot,
        timezone: TimeZoneOption = .requirePresent(accept: [.z, .Z, .hh, .`hh:mm`]),
        offsetComponents: TimeComponentEncoding = .anyWithLeadingZeros,
        sourceLocation: SourceLocation = #_sourceLocation
    ) {
        self.input = input
        self.expected = nil
        self.components = components
        self.datetimeSeparator = datetimeSeparator
        self.dateComponents = dateComponents
        self.timeComponents = timeComponents
        self.decimalSeparator = decimalSeparator
        self.timezone = timezone
        self.offsetComponents = offsetComponents
        self.sourceLocation = sourceLocation
    }

    func check() {
        let defaults = Self(reject: input)
        let isDefaulted =
            components == defaults.components
            && datetimeSeparator == defaults.datetimeSeparator
            && dateComponents == defaults.dateComponents
            && timeComponents == defaults.timeComponents
            && decimalSeparator == defaults.decimalSeparator
            && offsetComponents == defaults.offsetComponents
        if !isDefaulted {
            Issue.record(
                "Date options are not wired into the decoder yet", sourceLocation: sourceLocation)
        }
        // TODO(options): forward the remaining options once the parameters land
        let actual = Date(decodingISO8601: input, timezone: timezone)
        guard let expected else {
            #expect(
                actual == nil,
                "expected rejection, got \(actual.map { String($0.timeIntervalSince1970) } ?? "")",
                sourceLocation: sourceLocation)
            return
        }
        guard let actual else {
            Issue.record(
                "expected \(expected), but the input was rejected", sourceLocation: sourceLocation)
            return
        }
        let delta = abs(
            actual.timeIntervalSinceReferenceDate - expected.timeIntervalSinceReferenceDate)
        #expect(
            delta < Self.tolerance,
            "got \(actual.timeIntervalSince1970)s since 1970, expected \(expected.timeIntervalSince1970)s (off by \(delta)s)",
            sourceLocation: sourceLocation
        )
    }
}
