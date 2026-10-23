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

/// Which components a string may contain, and what joins the date to the time.
public struct ComponentsOption: Sendable {
    /// Characters accepted between the date and the time. `[]` admits no time part.
    @usableFromInline
    internal let separator: DateTimeSeparator
    /// Precision range of the components. `range.lowerBound == .date` makes the time part optional.
    @usableFromInline
    internal let range: ClosedRange<ComponentPrecision>

    @usableFromInline
    internal init(separator: DateTimeSeparator, range: ClosedRange<ComponentPrecision>) {
        self.separator = separator
        self.range = range
    }

    /// A date, optionally followed by a time joined with any of `separator`.
    /// `separator: []` or `range: .date ... .date` admit no time part, and are the same as `.dateOnly`.
    @export(implementation)
    public static func datetime(
        separator: DateTimeSeparator = [.t, .T, .space],
        range: ClosedRange<ComponentPrecision>
    ) -> Self {
        if separator == [] || separator == .t || range.upperBound == .date { return .dateOnly }
        return Self(separator: separator, range: range)
    }

    /// Will only accept bare date strings. Any time component will be considered invalid
    /// - Examples:
    ///   - "2020-02" rejected
    ///   - "2020-02-02" accepted
    ///   - "2020-02-02T12" rejected
    ///   - "2020-02-02T08:00:34" rejected
    public static let dateOnly = Self(separator: [], range: .date ... .date)
    /// Will reject any bare date string without time components
    /// - Examples:
    ///   - "2020-02" rejected
    ///   - "2020-02-02" rejected
    ///   - "2020-02-02T12" accepted
    ///   - "2020-02-02T08:00:34" accepted
    public static let datetimeOnly = datetime(range: .hour ... .subsecond(precision: .arbitrary))
    /// Will accept any date or datetime string.
    /// - Examples:
    ///   - "2020-02" rejected
    ///   - "2020-02-02" accepted
    ///   - "2020-02-02T12" accepted
    ///   - "2020-02-02T08:00:34" accepted
    public static let acceptBareDate = datetime(range: .date ... .subsecond(precision: .arbitrary))
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

    public static let hh = Self(rawValue: 1 << 2)
    public static let hhmm = Self(rawValue: 1 << 3)
    public static let hhmmss = Self(rawValue: 1 << 4)
    public static let `hh:mm` = Self(rawValue: 1 << 5)
    public static let `hh:mm:ss` = Self(rawValue: 1 << 6)

    public static let anyNumeric: Self = [.hh, .hhmm, .hhmmss, .`hh:mm`, .`hh:mm:ss`]
    public static let any: Self = [.z, .Z, .anyNumeric]

    // non-compliant foundation-like behaviour.
    // No longer fits UInt8, unless we collapse these cases as modifiers of previous 5
    // public static let h = Self(rawValue: 1 << 7)
    // public static let hhm = Self(rawValue: 1 << 8)
    // public static let hhmms = Self(rawValue: 1 << 9)
    // public static let `h:m` = Self(rawValue: 1 << 10)
    // public static let `h:m:s` = Self(rawValue: 1 << 11)
}

extension DateComponents {
    @export(implementation)
    public init?(
        decodingISO8601 span: Span<UInt8>,
        components: ComponentsOption = .acceptBareDate,
        // dateComponents: DateComponentEncoding = .separatedByDashWithLeadingZeros,
        // timeComponents: TimeComponentEncoding = .separatedByColonWithLeadingZeros,
        // decimalSeparator: DecimalSeparator = .dot,
        timezone: TimeZoneComponents = .any,
    ) {
        guard span.count >= 10 else { return nil }

        let datetime: Span<UInt8>
        let tz: TimeZone?

        if span.count == 10 {
            datetime = span
            tz = nil
        } else {
            switch parseTimezone(from: span, accept: timezone) {
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

        let lo: SIMD32<UInt8>
        let hi: SIMD32<UInt8>
        var buffer: SIMD32<UInt8>
        // switch components.separator {
        // case [], [.t]: preconditionFailure("TODO")
        //
        // case [.T]:
        //     lo = SIMD32(constant: "0000-00-00T00:00:00.000000000000")
        //     hi = SIMD32(constant: "9999-19-39T29:59:59.999999999999")
        //     buffer = lo
        //     memcpy(datetime, intoSIMD: &buffer)
        //
        // case [.space], [.space, .t]:
        //     lo = SIMD32(constant: "0000-00-00 00:00:00.000000000000")
        //     hi = SIMD32(constant: "9999-19-39 29:59:59.999999999999")
        //     buffer = lo
        //     memcpy(datetime, intoSIMD: &buffer)
        //
        // case [.T, .t]:
        //     lo = SIMD32(constant: "0000-00-00t00:00:00.000000000000")
        //     hi = SIMD32(constant: "9999-19-39t29:59:59.999999999999")
        //     buffer = lo
        //     memcpy(datetime, intoSIMD: &buffer)
        //     buffer[10] |= 0x20
        //
        // case [.space, .T]:
        //     lo = SIMD32(constant: "0000-00-00 00:00:00.000000000000")
        //     hi = SIMD32(constant: "9999-19-39 29:59:59.999999999999")
        //     buffer = lo
        //     memcpy(datetime, intoSIMD: &buffer)
        //     if buffer[10] == c("T") {
        //         buffer[10] = c(" ")
        //     }
        //
        // case [.space, .t, .T]:
        //     lo = SIMD32(constant: "0000-00-00 00:00:00.000000000000")
        //     hi = SIMD32(constant: "9999-19-39 29:59:59.999999999999")
        //     buffer = lo
        //     memcpy(datetime, intoSIMD: &buffer)
        //     if (buffer[10] | 0x20) == c("t") {
        //         buffer[10] = c(" ")
        //     }
        //
        // default: assertionFailure("components.separator contains an invalid bitpattern")
        // }

        // switch components.separator {
        // case [], [.t]: preconditionFailure("TODO")
        //
        // case [.T], [.T, .t]:
        //     lo = SIMD32(constant: "0000-00-00T00:00:00.000000000000")
        //     hi = SIMD32(constant: "9999-19-39T29:59:59.999999999999")
        //     buffer = lo
        //     memcpy(datetime, intoSIMD: &buffer)
        //     if components.separator.contains(.t) {
        //         buffer[10] &= ~0x20
        //     }
        //
        // case [.space], [.space, .t], [.space, .T], [.space, .T, .t]:
        //     lo = SIMD32(constant: "0000-00-00 00:00:00.000000000000")
        //     hi = SIMD32(constant: "9999-19-39 29:59:59.999999999999")
        //     buffer = lo
        //     memcpy(datetime, intoSIMD: &buffer)
        //
        //     var actualSeparator = buffer[10]
        //     if components.separator.contains(.t) {
        //         actualSeparator &= ~0x20
        //     }
        //     if components.separator.contains(.T) && actualSeparator == c("T") {
        //         buffer[10] = c(" ")
        //     }
        //
        // default: assertionFailure("components.separator contains an invalid bitpattern")
        // }

        if components.separator.contains(.space) || datetime.count == 10 {
            lo = SIMD32(constant: "0000-00-00 00:00:00.000000000000")
            hi = SIMD32(constant: "9999-19-39 29:59:59.999999999999")
            buffer = lo
            memcpy(datetime, intoSIMD: &buffer)

            var actualSeparator = buffer[10]
            if components.separator.contains(.t) {
                actualSeparator &= ~0x20
            }
            if components.separator.contains(.T) && actualSeparator == c("T") {
                buffer[10] = c(" ")
            }
        } else if components.separator.contains(.T) {
            lo = SIMD32(constant: "0000-00-00T00:00:00.000000000000")
            hi = SIMD32(constant: "9999-19-39T29:59:59.999999999999")
            buffer = lo
            memcpy(datetime, intoSIMD: &buffer)
            if components.separator.contains(.t) {
                buffer[10] &= ~0x20
            }
        } else {
            // assertionFailure(
            //     """
            //     components.separator of [] or [.t] should've been handled uptop in the bare-date 
            //     case, since this bitpattern of TimeZoneOption(separator: [], range: range) where 
            //     range != .date ... .date, should be illegal
            //     """
            // )
            return nil
        }

        // T       01010100
        // t       01110100
        // <space> 00100000
        // 0x20    00100000

        // if (buffer[10] | 0x20) == c("t") {
        //     buffer[10] = c(" ")
        // }

        // if buffer[10] == c(" ") {
        //     buffer[10] = c("T")
        // } else {
        //     buffer[10] &= ~0x20
        // }

        // read -> or -> branch -> write

        //   lda r1 (buffer[10])
        //   or r1 0x20
        //   test 't'
        //   jne skip
        //   str (buffer[10]) ' '
        // skip:
        //   ...rest

        // read -> branch -> write
        //                -> and -> write

        //   lda r1 (buffer[10])
        //   test ' '
        //   jne not_space
        //   str (buffer[10]) 'T'
        //   jmp rest
        // not_space:
        //   and r1 (~0x20)
        //   str (buffer[10]) r1
        // rest:
        //   ...rest

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
        components: ComponentsOption = .acceptBareDate,
        // dateComponents: DateComponentEncoding = .separatedByDashWithLeadingZeros,
        // timeComponents: TimeComponentEncoding = .separatedByColonWithLeadingZeros,
        // decimalSeparator: DecimalSeparator = .dot,
        timezone: TimeZoneComponents = .any,
    ) {
        let date = string.withUTF8Span {
            Self.init(
                decodingISO8601: $0,
                components: components,
                // dateComponents: dateComponents,
                // timeComponents: timeComponents,
                // decimalSeparator: decimalSeparator,
                timezone: timezone,
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
internal func parseTimezone(from span: Span<UInt8>, accept: TimeZoneComponents) -> ParsedTimeZone {
    assert(span.count >= 9)

    var maybeZ = span[span.count - 1]
    if accept.contains(.z) { maybeZ &= ~0x20 }
    if accept.contains(.Z) && maybeZ == c("Z") {
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

    // ±hh
    // This SIMD vector pattern is very particular to cover the following possibilities:
    // 99:99-19
    // :01.1±hh
    // T00.1±hh
    // 89T12±hh
    // .0000±hh
    // ,0000±hh

    // swift-format-ignore
    if accept.contains(.hh) && lanewise(buffer,
        liesBetween: SIMD8(constant: ",,,,0+00"),
                and: SIMD8(constant: "t9t99-19"))
    {
        let sign = Int(c(",")) - Int(buffer[5])
        guard sign != 0 else { return .malformed }
        // swift-format-ignore
        let offset =
              buffer.asciiDigit(at: 6) * 10 * 3600
            + buffer.asciiDigit(at: 7) * 3600
        guard let tz = TimeZone(secondsFromGMT: offset * sign)
        else { return .malformed }
        return .present(tz, trailingByteCount: 3)
    }

    // ±hh:mm
    // swift-format-ignore
    if accept.contains(.`hh:mm`) && lanewise(buffer,
        liesBetween: SIMD8(constant: ",0+00:00"),
                and: SIMD8(constant: "99-19:59"))
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
    // swift-format-ignore
    if accept.contains(.hhmm) && lanewise(buffer,
        liesBetween: SIMD8(constant: ",,0+0000"),
                and: SIMD8(constant: "t99-1959"))
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

    // ±hhmmss
    // swift-format-ignore
    if accept.contains(.hhmmss) && lanewise(buffer,
        liesBetween: SIMD8(constant: "0+000000"),
                and: SIMD8(constant: "9-195959"))
    {
        let sign = Int(c(",")) - Int(buffer[1])
        guard sign != 0 else { return .malformed }
        let offset =
              buffer.asciiDigit(at: 2) * 10 * 3600
            + buffer.asciiDigit(at: 3) * 3600
            + buffer.asciiDigit(at: 4) * 10 * 60
            + buffer.asciiDigit(at: 5) * 60
            + buffer.asciiDigit(at: 6) * 10
            + buffer.asciiDigit(at: 7)
        guard let tz = TimeZone(secondsFromGMT: offset * sign)
        else { return .malformed }
        return .present(tz, trailingByteCount: 7)
    }

    // ±hh:mm:ss
    // swift-format-ignore
    let `follows hh:mm:ss` = accept.contains(.`hh:mm:ss`) && lanewise(buffer,
        liesBetween: SIMD8(constant: "00:00:00"),
                and: SIMD8(constant: "19:59:59"))
    if span[span.count - 9] == c("+") && `follows hh:mm:ss` {
        // swift-format-ignore
        let offset =
              buffer.asciiDigit(at: 0) * 10 * 3600
            + buffer.asciiDigit(at: 1) * 3600
            + buffer.asciiDigit(at: 3) * 10 * 60
            + buffer.asciiDigit(at: 4) * 60
            + buffer.asciiDigit(at: 6) * 10
            + buffer.asciiDigit(at: 7)
        guard let tz = TimeZone(secondsFromGMT: offset)
        else { return .malformed }
        return .present(tz, trailingByteCount: 9)
    }

    if span[span.count - 9] == c("-") && `follows hh:mm:ss` {
        // swift-format-ignore
        let offset =
              buffer.asciiDigit(at: 0) * 10 * 3600
            + buffer.asciiDigit(at: 1) * 3600
            + buffer.asciiDigit(at: 3) * 10 * 60
            + buffer.asciiDigit(at: 4) * 60
            + buffer.asciiDigit(at: 6) * 10
            + buffer.asciiDigit(at: 7)
        guard let tz = TimeZone(secondsFromGMT: -offset)
        else { return .malformed }
        return .present(tz, trailingByteCount: 9)
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
        components: ComponentsOption = .datetimeOnly,
        // dateComponents: DateComponentEncoding = .separatedByDashWithLeadingZeros,
        // timeComponents: TimeComponentEncoding = .separatedByColonWithLeadingZeros,
        // decimalSeparator: DecimalSeparator = .dot,
        timezone: TimeZoneOption = .requirePresent(accept: .any),
    ) {
        guard
            var components = DateComponents(
                decodingISO8601: span,
                components: components,
                // dateComponents: dateComponents,
                // timeComponents: timeComponents,
                // decimalSeparator: decimalSeparator,
                timezone: timezone.accept,
            )
        else { return nil }

        switch (components.timeZone, timezone.assumedTimezone) {
        // .requirePresent and was not present
        case (nil, nil): return nil
        // .impose/.assume
        case (nil, let tz?): components.timeZone = tz
        // explicitly set, and successfully parsed
        case (_?, _): break
        }

        if let date = components.date { self = date } else { return nil }
    }

    @export(implementation)
    public init?(
        decodingISO8601 string: String,
        components: ComponentsOption = .datetimeOnly,
        // dateComponents: DateComponentEncoding = .separatedByDashWithLeadingZeros,
        // timeComponents: TimeComponentEncoding = .separatedByColonWithLeadingZeros,
        // decimalSeparator: DecimalSeparator = .dot,
        timezone: TimeZoneOption = .requirePresent(accept: .any),
    ) {
        let date = string.withUTF8Span {
            Self.init(
                decodingISO8601: $0,
                components: components,
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
