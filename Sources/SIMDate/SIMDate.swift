#if canImport(FoundationEssentials)
    public import FoundationEssentials
#else
    public import Foundation
#endif

// FoundationEssentials doesn't re-export libc, so memcpy needs it imported explicitly.
#if canImport(Darwin)
    import Darwin
#elseif canImport(Glibc)
    import Glibc
#elseif canImport(Musl)
    import Musl
#elseif canImport(ucrt)
    import ucrt
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

// TODO: Implement these later
// public enum DateComponentEncoding: Sendable {
//     /// `YYYY-MM-DD`
//     case separatedByDashWithLeadingZeros
//     /// `YYYYMMDD`
//     case conjoinedWithLeadingZeros
//     /// `YYYY-MM-DD` or `YYYYMMDD`
//     case anyWithLeadingZeros
//     // /// `Y-M-D`. allows parsing non-standard dates like 123-8-12
//     // case separatedByDashWithoutLeadingZeros
// }
//
// public enum TimeComponentEncoding: Sendable {
//     /// `hh:mm:ss`
//     case separatedByColonWithLeadingZeros
//     /// `hhmmss`
//     case conjoinedWithLeadingZeros
//     /// `hh:mm:ss` or `hhmmss`
//     case anyWithLeadingZeros
//     // /// `h:m:s`. allows parsing non-standard times like 8:4:12
//     // case separatedByColonWithoutLeadingZeros
// }
//
// public enum DecimalSeparator: Sendable {
//     /// `.`
//     case dot
//     /// `,`
//     case comma
//     /// `.` or `,`
//     case acceptAny
// }

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
    // I rely on constant folding to produce decent code. This is basically a template
    // to eliminate as many branches as necessary, given a static set of configuration
    // options. If you care for code size, the client may define a single symbol with
    // set parameters to ensure it is only compiled once.
    @export(implementation)
    @inline(__always)
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

        if components.range.lowerBound == .date && span.count == 10 {
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
            case 13 where components.range.contains(.hour): break
            // YYYY-MM-DDThh:mm
            case 16 where components.range.contains(.minute): break
            // YYYY-MM-DDThh:mm:ss
            case 19 where components.range.contains(.second): break
            // YYYY-MM-DDThh:mm:ss.ss
            case 21...32
            where components.range.contains(
                .subsecond(precision: .digits(UInt8(datetime.count) - 20))
            ):
                break
            default: return nil
            }
        }

        // Depending on the allowed separators, pick an optimal SIMD constant
        // and conform the buffer from the ascii range to the pure numeric repr
        // of YYYY0MM0DD0hh0mm0ss0ssssssssssss
        var buffer: SIMD32<UInt8>
        if components.separator.contains(.space) || datetime.count == 10 {
            let base = SIMD32(constant: "0000-00-00 00:00:00.000000000000")
            buffer = base
            memcpy(datetime, intoSIMD: &buffer)

            var actualSeparator = buffer[10]
            if components.separator.contains(.t) {
                actualSeparator &= ~0x20
            }
            if components.separator.contains(.T) && actualSeparator == c("T") {
                buffer[10] = c(" ")
            }
            buffer &-= base
        } else if components.separator.contains(.T) {
            let base = SIMD32(constant: "0000-00-00T00:00:00.000000000000")
            buffer = base
            memcpy(datetime, intoSIMD: &buffer)
            if components.separator.contains(.t) {
                buffer[10] &= ~0x20
            }
            buffer &-= base
        } else {
            // Not a pure date, nor
            return nil
        }

        let upperBound = SIMD32<UInt8>(
            9, 9, 9, 9,  // year
            0,  // -
            1, 9,  // month
            0,  // -
            3, 9,  // day
            0,  // T
            2, 9,  // hour
            0,  // :
            5, 9,  // minute
            0,  // :
            5, 9,  // second
            0,  // .
            9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9,  // subsecond
        )

        guard (buffer .<= upperBound) == SIMDMask(repeating: true) else { return nil }

        // buffer =            | Y    Y    Y    Y    -  M  M  -  D   D   T  h  h  :  m   m   :  S   S  .  s    s    s    s    s    s    s    s    s    s    s    s
        // w1     &*           | 10   1    10   1    0  10 1  0  10  1   0  10 1  0  10  1   0  10  1  0  10   1    10   1    10   1    10   1    1    0    0    0
        //        even &+ odd  | ^^^^^^^^  ^^^^^^^^^ ^^^^^ ^^^^^ ^^^^^^^ ^^^^^ ^^^^^ ^^^^^^^ ^^^^^^ ^^^^^ ^^^^^^^^^ ^^^^^^^^^ ^^^^^^^^^ ^^^^^^^^^ ^^^^^^^^^ ^^^^^^^^^
        // a      =            | 10Y+Y     10Y+Y     10M   M     [10D+D] 10h   h     [10m+m] 10S    S     10s+s     10s+s     10s+s     10s+s     s         0
        //        widen to u16 |
        // w2     &*           | 100       1         1     1     0       1     1     0       1      1     100       1         100       1         1         0
        //        even &+ odd  | ^^^^^^^^^^^^^^^^^^^ ^^^^^^^^^^^ ^^^^^^^^^^^^^ ^^^^^^^^^^^^^ ^^^^^^^^^^^^ ^^^^^^^^^^^^^^^^^^^ ^^^^^^^^^^^^^^^^^^^ ^^^^^^^^^^^^^^^^^^^
        // b      =            | [1000Y+100Y+10Y+Y]  [10M+M]     10h           h             [10S+S]      1000s+100s+10s+s    1000s+100s+10s+s    s
        //        high         |                                                             ^^^^^^^^^^^^ ^^^^^^^^^^^^^^^^^^^ ^^^^^^^^^^^^^^^^^^^ ^^^^^^^^^^^^^^^^^^^
        // c      &*           |                                                             0            100000              10                  1
        //        sum          |                                                             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
        //        =            |                                                             [100000000s+10000000s+1000000s+100000s+10000s+1000s+100s+10s+s]

        let w1 = SIMD32<UInt8>(
            10, 1, 10, 1,
            0,
            10, 1,
            0,
            10, 1,
            0,
            10, 1,
            0,
            10, 1,
            0,
            10, 1,
            0,
            10, 1, 10, 1, 10, 1, 10, 1, 1, 0, 0, 0
        )
        let a = condense(buffer &* w1)

        let w2 = SIMD16<UInt16>(100, 1, 1, 1, 0, 1, 1, 0, 1, 1, 100, 1, 100, 1, 1, 0)
        let b = condense(a.widen(to: UInt16.self) &* w2)

        let c = b.highHalf.widen(to: UInt32.self) &* SIMD4(0, 100_000, 10, 1)

        // Checks are split between debug-only asserts and production-enforced guards
        // There should be no way for these asserts to trigger, they serve only as
        // programmer documentation and assumption enforcement
        let year = b[0]
        assert(year < 10000)

        let month = b[1]
        guard month <= 12 && month > 0 else { return nil }

        let day = a[4]
        guard day > 0 && day <= 31 else { return nil }
        // day is an interesting one, since the validity of a date depends on the
        // year and month with specific rules DateComponents.isValidDate already
        // enforces, we can choose to forgo the upper bound check.

        let hour: Int?
        if components.range.upperBound >= .hour {
            let h = b[2] + b[3]
            guard h < 24 else { return nil }
            hour = Int(h)
        } else {
            hour = nil
        }

        // minute and second are already validated with the simd comparisons
        let minute: Int?
        if components.range.upperBound >= .minute {
            let m = a[7]
            assert(m < 60)
            minute = Int(m)
        } else {
            minute = nil
        }

        let second: Int?
        if components.range.upperBound >= .second {
            let s = b[4]
            assert(s < 60)
            second = Int(s)
        } else {
            second = nil
        }

        var nanosecond: Int? = nil
        if case .subsecond(_) = components.range.upperBound {
            let ns = c.wrappedSum()
            assert(ns < 1_000_000_000)
            nanosecond = Int(ns)
        } else {
            nanosecond = nil
        }

        self.init(
            calendar: .init(identifier: .iso8601),
            timeZone: tz,
            year: Int(year),
            month: Int(month),
            day: Int(day),
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
@inline(__always)
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

    if accept.contains(.hh),
        let buffer = impose(buffer, lowerBound: ",,,,0+00", upperBound: "t9t99-19")
    {
        let sign = 1 - Int(buffer[5])
        guard sign != 0 else { return .malformed }
        let offset = buffer.highHalf.highHalf.widen(to: UInt32.self) &* SIMD2(10 * 60 * 60, 60 * 60)
        guard let tz = TimeZone(secondsFromGMT: Int(offset.wrappedSum()) * sign)
        else { return .malformed }
        return .present(tz, trailingByteCount: 3)
    }

    // ±hh:mm
    if accept.contains(.`hh:mm`),
        let buffer = impose(buffer, lowerBound: ",0+00:00", upperBound: "99-19:59")
    {
        let sign = 1 - Int(buffer[2])
        guard sign != 0 else { return .malformed }
        let offset =
            buffer.widen(to: UInt32.self) &* SIMD8(0, 0, 0, 60 * 60 * 10, 60 * 60, 0, 60 * 10, 60)
        guard let tz = TimeZone(secondsFromGMT: Int(offset.wrappedSum()) * sign)
        else { return .malformed }
        return .present(tz, trailingByteCount: 6)
    }

    // ±hhmm
    if accept.contains(.hhmm),
        let buffer = impose(buffer, lowerBound: ",,0+0000", upperBound: "t99-1959")
    {
        let sign = 1 - Int(buffer[3])
        guard sign != 0 else { return .malformed }
        let offset =
            buffer.highHalf.widen(to: UInt32.self) &* SIMD4(60 * 60 * 10, 60 * 60, 60 * 10, 60)
        guard let tz = TimeZone(secondsFromGMT: Int(offset.wrappedSum()) * sign)
        else { return .malformed }
        return .present(tz, trailingByteCount: 5)
    }

    // ±hhmmss
    if accept.contains(.hhmmss),
        let buffer = impose(buffer, lowerBound: "0+000000", upperBound: "9-195959")
    {
        let sign = 1 - Int(buffer[1])
        guard sign != 0 else { return .malformed }
        let offset =
            buffer.widen(to: UInt32.self) &* SIMD8(0, 0, 60 * 60 * 10, 60 * 60, 60 * 10, 60, 10, 1)
        guard let tz = TimeZone(secondsFromGMT: Int(offset.wrappedSum()) * sign)
        else { return .malformed }
        return .present(tz, trailingByteCount: 7)
    }

    // ±hh:mm:ss
    if accept.contains(.`hh:mm:ss`) {
        let sign: Int
        if span[span.count - 9] == c("+") {
            sign = 1
        } else if span[span.count - 9] == c("-") {
            sign = -1
        } else {
            return .missing
        }

        guard let buffer = impose(buffer, lowerBound: "00:00:00", upperBound: "19:59:59")
        else { return .missing }
        let offset =
            buffer.widen(to: UInt32.self) &* SIMD8(60 * 60 * 10, 60 * 60, 0, 60 * 10, 60, 0, 10, 1)
        guard let tz = TimeZone(secondsFromGMT: Int(offset.wrappedSum()) * sign)
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
}

extension SIMD16 where Scalar: FixedWidthInteger {
    @usableFromInline
    internal func widen<Other: FixedWidthInteger>(to: Other.Type) -> SIMD16<Other> {
        assert(Scalar.bitWidth <= Other.bitWidth)
        return SIMD16<Other>(truncatingIfNeeded: self)
    }
}

extension SIMD8 where Scalar: FixedWidthInteger {
    @usableFromInline
    internal func widen<Other: FixedWidthInteger>(to: Other.Type) -> SIMD8<Other> {
        assert(Scalar.bitWidth <= Other.bitWidth)
        return SIMD8<Other>(truncatingIfNeeded: self)
    }
}

extension SIMD4 where Scalar: FixedWidthInteger {
    @usableFromInline
    internal func widen<Other: FixedWidthInteger>(to: Other.Type) -> SIMD4<Other> {
        assert(Scalar.bitWidth <= Other.bitWidth)
        return SIMD4<Other>(truncatingIfNeeded: self)
    }
}

extension SIMD2 where Scalar: FixedWidthInteger {
    @usableFromInline
    internal func widen<Other: FixedWidthInteger>(to: Other.Type) -> SIMD2<Other> {
        assert(Scalar.bitWidth <= Other.bitWidth)
        return SIMD2<Other>(truncatingIfNeeded: self)
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

/// The name is weird. Checks that the simd vector lies between lower and upper
/// bounds, returns an x - lowerBound on success.
@usableFromInline
@inline(__always)
internal func impose<Vector>(
    _ x: Vector,
    lowerBound asciiLo: StaticString,
    upperBound asciiHi: StaticString,
) -> Vector? where Vector: SIMD<UInt8> {
    let lowerBound = Vector(constant: asciiLo)
    let upperBound = Vector(constant: asciiHi)
    let subtracted = x &- lowerBound

    guard (subtracted .<= upperBound &- lowerBound) == SIMDMask(repeating: true)
    else { return nil }

    return subtracted
}

@usableFromInline
internal func condense<Scalar: FixedWidthInteger>(_ vector: SIMD32<Scalar>) -> SIMD16<Scalar> {
    vector.evenHalf &+ vector.oddHalf
}
@usableFromInline
internal func condense<Scalar: FixedWidthInteger>(_ vector: SIMD16<Scalar>) -> SIMD8<Scalar> {
    vector.evenHalf &+ vector.oddHalf
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

    @export(implementation)
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
    @export(implementation)
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
