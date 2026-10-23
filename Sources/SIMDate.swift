#if canImport(FoundationEssentials)
    public import FoundationEssentials
#else
    public import Foundation
#endif

public enum ComponentPrecision: Sendable, Comparable {
    case date
    case hour
    case minute
    case second
    case subsecond(precision: SubsecondPrecision)

    public struct SubsecondPrecision: Sendable, Comparable {
        /// `UInt8.max` is a special value denoting `arbitrary`, so it sorts above any explicit digit count
        let digits: UInt8
        public static func < (lhs: borrowing Self, rhs: borrowing Self) -> Bool {
            lhs.digits < rhs.digits
        }

        public static func digits(_ count: UInt8) -> Self {
            precondition(
                count > 0 && count < UInt8.max,
                "SubsecondPrecision.digits(_:) requires 1...\(UInt8.max - 1) digits"
            )
            return Self(digits: count)
        }
        public static let arbitrary = Self(digits: .max)
        public static let millis = Self(digits: 3)
        public static let micros = Self(digits: 6)
        public static let nanos = Self(digits: 9)
    }
}

extension ClosedRange where Bound == ComponentPrecision {
    /// Will reject any bare date string without time components
    /// - Examples:
    ///   - "2020-02" rejected
    ///   - "2020-02-02" rejected
    ///   - "2020-02-02T12" accepted
    ///   - "2020-02-02T08:00:34" accepted
    public static let datetimeOnly: Self = .hour ... .subsecond(precision: .arbitrary)
    /// Will only accept bare date strings. Any time component will be considered invalid
    /// - Examples:
    ///   - "2020-02" rejected
    ///   - "2020-02-02" accepted
    ///   - "2020-02-02T12" rejected
    ///   - "2020-02-02T08:00:34" rejected
    public static let dateOnly: Self = .date ... .date
    /// Will accept any date or datetime string.
    /// - Examples:
    ///   - "2020-02" rejected
    ///   - "2020-02-02" accepted
    ///   - "2020-02-02T12" accepted
    ///   - "2020-02-02T08:00:34" accepted
    public static let acceptBareDate: Self = .date ... .subsecond(precision: .arbitrary)
}

public enum DateComponentSeparators: Sendable {
    case requireDash, requireOmitted, acceptAny
}

public enum TimeComponentSeparators: Sendable {
    case requireColon, requireOmitted, acceptAny
}

public struct DateTimeSeparator: Sendable, OptionSet {
    public let rawValue: UInt8
    public init(rawValue: UInt8) {
        self.rawValue = rawValue
    }

    public static let t = DateTimeSeparator(rawValue: 1 << 0)
    public static let T = DateTimeSeparator(rawValue: 1 << 1)
    public static let space = DateTimeSeparator(rawValue: 1 << 2)
}

public enum ZuluOffset: Sendable {
    case accept, acceptCaseSensitive, reject
}

public enum NumericOffset: Sendable {
    case accept(resolution: ClosedRange<Resolution>)
    case reject

    public enum Resolution: Sendable, Comparable {
        case hour, minute, second
    }
}

public enum DateComponentEncoding: Sendable {
    /// `YYYY-MM-DD`
    case separatedByDashWithLeadingZeros
    /// `YYYYMMDD`
    case conjoinedWithLeadingZeros
    /// `YYYY-MM-DD` or `YYYYMMDD`
    case anyWithLeadingZeros
    // /// `Y-M-D`. allows parsing non-standard dates like 123-8-12
    // case separatedByDashWithoutLeadingZeros
}

public enum TimeComponentEncoding: Sendable {
    /// `hh:mm:ss`
    case separatedByColonWithLeadingZeros
    /// `hhmmss`
    case conjoinedWithLeadingZeros
    /// `hh:mm:ss` or `hhmmss`
    case anyWithLeadingZeros
    // /// `h:m:s`. allows parsing non-standard times like 8:4:12
    // case separatedByColonWithoutLeadingZeros
}

public enum DecimalSeparator: Sendable {
    /// `.`
    case dot
    /// `,`
    case comma
    /// `.` or `,`
    case acceptAny
}

public struct TimeZoneComponents: Sendable, OptionSet {
    public let rawValue: UInt8
    public init(rawValue: UInt8) {
        self.rawValue = rawValue
    }

    /// case insensitivity when it comes to matching "zulu"
    public static let z = Self(rawValue: 1 << 0)
    /// Accept 'Z' offset
    public static let Z = Self(rawValue: 1 << 1)
    public static let `hh` = Self(rawValue: 1 << 2)
    public static let `hh:mm` = Self(rawValue: 1 << 3)
    public static let `hh:mm:ss` = Self(rawValue: 1 << 4)

    // zZhms
    // XX111 hh hh:mm hh:mm:ss
    // XX110 hh hh:mm
    // XX101 hh       hh:mm:ss
    // XX100 hh
    // XX011    hh:mm hh:mm:ss
    // XX010    hh:mm
    // XX001          hh:mm:ss
    // XX000
}

extension DateComponents {
    // @export(implementation)
    public init?(
        decodingISO8601 span: Span<UInt8>,
        // components: ClosedRange<ComponentPrecision> = .acceptBareDate,
        // datetimeSeparator: DateTimeSeparator = [.t, .T, .space],
        // dateComponents: DateComponentEncoding = .separatedByDashWithLeadingZeros,
        // timeComponents: TimeComponentEncoding = .separatedByColonWithLeadingZeros,
        // decimalSeparator: DecimalSeparator = .dot,
        // timezone: TimeZoneComponents = [.z, .Z, .hh, .`hh:mm`],
        // offsetComponents: TimeComponentEncoding = .anyWithLeadingZeros,
    ) {
        // assert(
        //     datetimeSeparator != [],
        //     "Empty `datetimeSeparator:` parameter will result in always-failing parsing"
        // )

        guard span.count >= 10 else { return nil }

        let datetime: Span<UInt8>
        let tz: TimeZone?

        if span.count == 10 {
            datetime = span
            tz = nil
        } else {
            switch parseTimezone(from: span) {
            case .malformed: return nil
            case .missing:
                datetime = span
                tz = nil
            case .present(let presentTz, let trailingByteCount):
                datetime = span.extracting(droppingLast: trailingByteCount)
                tz = presentTz
            }

            switch datetime.count {
            // YYYY-MM-DDThh
            case 13: break
            // YYYY-MM-DDThh:mm
            case 16: break
            // YYYY-MM-DDThh:mm:ss
            case 19: break
            // YYYY-MM-DDThh:mm:ss.ss
            case 21...32: break
            default: return nil
            }
        }

        let lo = SIMD32(constant: "0000-00-00 00:00:00.000000000000")
        let hi = SIMD32(constant: "9999-19-39 29:59:59.999999999999")

        var buffer = lo
        memcpy(datetime, intoSIMD: &buffer)

        if (buffer[10] | 0x20) == c("t") {
            buffer[10] = c(" ")
        }

        guard lanewise(buffer, liesBetween: lo, and: hi) else { return nil }

        // Checks are split between debug-only asserts and production-enforced guards
        // There should be no way for these asserts to trigger, they serve only as
        // programmer documentation and assumption enforcement
        // swift-format-ignore
        let year =
              buffer.asciiDigit(at: 0) * 1000
            + buffer.asciiDigit(at: 1) * 100
            + buffer.asciiDigit(at: 2) * 10
            + buffer.asciiDigit(at: 3)
        assert(year >= 0 && year < 10000)

        let month = buffer.asciiDigit(at: 5) * 10 + buffer.asciiDigit(at: 6)
        guard month <= 12 && month > 0 else { return nil }

        let day = buffer.asciiDigit(at: 8) * 10 + buffer.asciiDigit(at: 9)
        guard day > 0 && day <= 31 else { return nil }
        // day is an interesting one, since the validity of a date depends on the
        // year and month with specific rules DateComponents.isValidDate already
        // enforces, we can choose to forgo the upper bound check.

        let hour = buffer.asciiDigit(at: 11) * 10 + buffer.asciiDigit(at: 12)
        assert(hour >= 0)
        guard hour < 24 else { return nil }

        // minute and second are already validated with the simd comparisons
        let minute = buffer.asciiDigit(at: 14) * 10 + buffer.asciiDigit(at: 15)
        assert(minute >= 0 && minute < 60)

        let second = buffer.asciiDigit(at: 17) * 10 + buffer.asciiDigit(at: 18)
        assert(second >= 0 && second < 60)

        // swift-format-ignore
        let nanosecond =
              buffer.asciiDigit(at: 20) * 100_000_000
            + buffer.asciiDigit(at: 21) * 10_000_000
            + buffer.asciiDigit(at: 22) * 1_000_000
            + buffer.asciiDigit(at: 23) * 100_000
            + buffer.asciiDigit(at: 24) * 10_000
            + buffer.asciiDigit(at: 25) * 1_000
            + buffer.asciiDigit(at: 26) * 100
            + buffer.asciiDigit(at: 27) * 10
            + buffer.asciiDigit(at: 28)
        assert(nanosecond >= 0 && nanosecond < 1_000_000_000)

        self.init(
            calendar: .init(identifier: .iso8601),
            timeZone: tz,
            year: year,
            month: month,
            day: day,
            hour: hour,
            minute: minute,
            second: second,
            nanosecond: nanosecond,
        )
    }

    @export(implementation)
    public init?(
        decodingISO8601 string: String,
        // components: ClosedRange<ComponentPrecision> = .acceptBareDate,
        // datetimeSeparator: DateTimeSeparator = [.t, .T, .space],
        // dateComponents: DateComponentEncoding = .separatedByDashWithLeadingZeros,
        // timeComponents: TimeComponentEncoding = .separatedByColonWithLeadingZeros,
        // decimalSeparator: DecimalSeparator = .dot,
        // timezone: TimeZoneComponents = [.z, .Z, .hh, .`hh:mm`],
        // offsetComponents: TimeComponentEncoding = .anyWithLeadingZeros,
    ) {
        let date = string.withUTF8Span {
            Self.init(
                decodingISO8601: $0,
                // components: components,
                // datetimeSeparator: datetimeSeparator,
                // dateComponents: dateComponents,
                // timeComponents: timeComponents,
                // decimalSeparator: decimalSeparator,
                // timezone: timezone,
                // offsetComponents: offsetComponents,
            )
        }
        if let date { self = date } else { return nil }
    }
}

@usableFromInline
internal enum ParsedTimeZone {
    case missing
    case malformed
    case present(TimeZone, trailingByteCount: Int)
}

@usableFromInline
internal func parseTimezone(from span: Span<UInt8>) -> ParsedTimeZone {
    assert(span.count >= 8)

    // TODO:
    // ±hh:mm:ss
    // ±hhmmss

    if (span[span.count - 1] | 0x20) == c("z") {
        return .present(.init(secondsFromGMT: 0)!, trailingByteCount: 1)
    }

    // This is most likely an unaligned load, but like, what are you gonna do?
    // I mean, there are possibilities to do an aligned 16 byte load, with mask bits
    // set to not trigger a page fault, and shift the values around into proper places
    let buffer = span.bytes.load(fromByteOffset: span.count - 8, as: SIMD8<UInt8>.self)

    // In the ascii table it goes like:
    // - 0x2B: + (plus)
    // - 0x2C: , (comma)
    // - 0x2D: - (minus)

    // ±hh:mm
    if lanewise(buffer, liesBetween: SIMD8(constant: ",0+00:00"), and: SIMD8(constant: "99-19:59"))
    {
        let sign = Int(c(",")) - Int(buffer[2])
        guard sign != 0 else { return .malformed }
        let offset =
            buffer.asciiDigit(at: 3) * 10 * 3600
            + buffer.asciiDigit(at: 4) * 3600
            + buffer.asciiDigit(at: 6) * 10 * 60
            + buffer.asciiDigit(at: 7) * 60
        guard let tz = TimeZone(secondsFromGMT: offset * sign)
        else { return .malformed }
        return .present(tz, trailingByteCount: 6)
    }

    // ±hhmm
    if lanewise(buffer, liesBetween: SIMD8(constant: ",,0+0000"), and: SIMD8(constant: "t99-1959"))
    {
        let sign = Int(c(",")) - Int(buffer[3])
        guard sign != 0 else { return .malformed }
        let offset =
            buffer.asciiDigit(at: 4) * 10 * 3600
            + buffer.asciiDigit(at: 5) * 3600
            + buffer.asciiDigit(at: 6) * 10 * 60
            + buffer.asciiDigit(at: 7) * 60
        guard let tz = TimeZone(secondsFromGMT: offset * sign)
        else { return .malformed }
        return .present(tz, trailingByteCount: 5)
    }

    // 99:99-19
    // :01.1±hh
    // T00.1±hh
    // 89T12±hh
    // .0000±hh
    // ,0000±hh

    // ±hh
    if lanewise(buffer, liesBetween: SIMD8(constant: ",,,,0+00"), and: SIMD8(constant: "t9t99-19"))
    {
        let sign = Int(c(",")) - Int(buffer[5])
        guard sign != 0 else { return .malformed }
        let offset =
            buffer.asciiDigit(at: 6) * 10 * 3600
            + buffer.asciiDigit(at: 7) * 3600
        guard let tz = TimeZone(secondsFromGMT: offset * sign)
        else { return .malformed }
        return .present(tz, trailingByteCount: 3)
    }

    return .missing
}

extension SIMD where Scalar == UInt8 {
    /// A conventient way to init a constant simd vector.
    /// We care that the vector is composed at compile time, so we rely
    /// on optimising compiler inlining and constant folding
    @inline(__always)
    @usableFromInline
    internal init(constant string: StaticString) {
        self = string.withUTF8Buffer {
            assert($0.count == Self.scalarCount)
            return unsafe Self($0)
        }
    }

    @inline(__always)
    @usableFromInline
    internal func asciiDigit(at index: Int) -> Int {
        Int(self[index] - c("0"))
    }
}

/// c char
@inline(__always)
@usableFromInline
internal func c(_ char: Unicode.Scalar) -> UInt8 {
    UInt8(ascii: char)
}

@usableFromInline
internal func memcpy(_ span: Span<UInt8>, intoSIMD vector: inout some SIMD<UInt8>) {
    precondition(span.count <= vector.scalarCount, "input buffer exceeds SIMD vector capacity")
    span.withUnsafeBytes { source in
        withUnsafeMutableBytes(of: &vector) { target in
            _ = unsafe memcpy(target.baseAddress, source.baseAddress, source.count)
        }
    }
}

@usableFromInline
internal func lanewise<Vector>(
    _ x: Vector,
    liesBetween lowerBound: Vector,
    and higherBound: Vector
) -> Bool where Vector: SIMD, Vector.Scalar: Comparable {
    let cmp = (x .>= lowerBound) .& (x .<= higherBound)
    let allTrue = SIMDMask<Vector.MaskStorage>(repeating: true)
    return cmp == allTrue
}

extension String {
    /// Backporting the convenience method of .span on oses <26
    @usableFromInline
    internal func withUTF8Span<R>(_ body: (Span<UInt8>) -> R) -> R {
        if #available(anyAppleOS 26.0, *) {
            return body(self.utf8.span)
        } else {
            let result = self.utf8.withContiguousStorageIfAvailable {
                unsafe body($0.span)
            }
            if let result { return result }

            // Internal representation does not support non-contiguous normalization.
            // Reallocate when necessary.
            var mutable = self
            return mutable.withUTF8 { unsafe body($0.span) }
        }
    }
}

public struct TimeZoneOption: Sendable {
    @usableFromInline
    internal let assumedTimezone: TimeZone?
    @usableFromInline
    internal let accept: TimeZoneComponents

    @usableFromInline
    internal init(assumedTimezone: TimeZone?, accept pattern: TimeZoneComponents) {
        self.assumedTimezone = assumedTimezone
        self.accept = pattern
    }

    // assumedTimezone: nil and accept: [] is not a valid configuration,
    // it'll reject every possible datetime string

    /// Assign a default time zone if omitted
    @export(implementation)
    public static func assume(_ tz: TimeZone, accept pattern: TimeZoneComponents) -> Self {
        Self(assumedTimezone: tz, accept: pattern)
    }

    /// Reject any mention of a time zone offset.
    /// Assign an imposed timezone when creating a Date object
    @export(implementation)
    public static func requireOmitted(impose timeZone: TimeZone) -> Self {
        Self(assumedTimezone: timeZone, accept: [])
    }

    /// Reject any string without an explicitly set timezone offset
    @export(implementation)
    public static func requirePresent(accept pattern: TimeZoneComponents) -> Self {
        // Debug assert, since most systems will gracefully handle parsing failures upstream
        assert(
            pattern != [],
            "TimeZoneOption.requirePresent(accept: []) will always fail to parse any-and-all datetime strings"
        )
        return Self(assumedTimezone: nil, accept: pattern)

    }
}

extension Date {
    // @export(implementation)
    public init?(
        decodingISO8601 span: Span<UInt8>,
        // components: ClosedRange<ComponentPrecision> = .datetimeOnly,
        // datetimeSeparator: DateTimeSeparator = [.t, .T, .space],
        // dateComponents: DateComponentEncoding = .separatedByDashWithLeadingZeros,
        // timeComponents: TimeComponentEncoding = .separatedByColonWithLeadingZeros,
        // decimalSeparator: DecimalSeparator = .dot,
        timezone: TimeZoneOption = .requirePresent(accept: [.z, .Z, .hh, .`hh:mm`]),
        // offsetComponents: TimeComponentEncoding = .anyWithLeadingZeros,
    ) {
        guard
            var components = DateComponents(
                decodingISO8601: span,
                // components: components,
                // datetimeSeparator: datetimeSeparator,
                // dateComponents: dateComponents,
                // timeComponents: timeComponents,
                // decimalSeparator: decimalSeparator,
                // timezone: timezone.accept,
                // offsetComponents: offsetComponents
            )
        else { return nil }

        components.timeZone ??= timezone.assumedTimezone

        if let date = components.date { self = date } else { return nil }
    }

    @export(implementation)
    public init?(
        decodingISO8601 string: String,
        // components: ClosedRange<ComponentPrecision> = .datetimeOnly,
        // datetimeSeparator: DateTimeSeparator = [.t, .T, .space],
        // dateComponents: DateComponentEncoding = .separatedByDashWithLeadingZeros,
        // timeComponents: TimeComponentEncoding = .separatedByColonWithLeadingZeros,
        // decimalSeparator: DecimalSeparator = .dot,
        timezone: TimeZoneOption = .requirePresent(accept: [.z, .Z, .hh, .`hh:mm`]),
        // offsetComponents: TimeComponentEncoding = .anyWithLeadingZeros,
    ) {
        let date = string.withUTF8Span {
            Self.init(
                decodingISO8601: $0,
                // components: components,
                // datetimeSeparator: datetimeSeparator,
                // dateComponents: dateComponents,
                // timeComponents: timeComponents,
                // decimalSeparator: decimalSeparator,
                timezone: timezone,
                // offsetComponents: offsetComponents,
            )
        }
        if let date { self = date } else { return nil }
    }
}

infix operator ??= : AssignmentPrecedence

@usableFromInline
@discardableResult
func ??= <T>(lhs: inout T?, rhs: @autoclosure () -> T) -> T {
    if let lhs { return lhs }
    let res = rhs()
    lhs = res
    return res
}

@usableFromInline
@discardableResult
func ??= <T, E>(lhs: inout T?, rhs: @autoclosure () throws(E) -> T) throws(E) -> T {
    if let lhs { return lhs }
    let res = try rhs()
    lhs = res
    return res
}

@usableFromInline
@discardableResult
func ??= <T>(lhs: inout T?, rhs: @autoclosure () -> T?) -> T? {
    if let lhs { return lhs }
    let res = rhs()
    lhs = res
    return res
}

@usableFromInline
@discardableResult
func ??= <T, E>(lhs: inout T?, rhs: @autoclosure () throws(E) -> T?) throws(E) -> T? {
    if let lhs { return lhs }
    let res = try rhs()
    lhs = res
    return res
}
