#if canImport(FoundationEssentials)
    import FoundationEssentials
#else
    import Foundation
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
    @export(implementation)
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

        guard span.count > 10 else { return nil }
        guard span.count <= 32 else { return nil }

        let lo = SIMD32(constant: "0000-00-00T00:00:00.000000000000")
        let hi = SIMD32(constant: "9999-19-39T29:59:59.999999999999")

        var buffer = lo
        memcpy(span, intoSIMD: &buffer)

        guard lanewise(buffer, liesBetween: lo, and: hi) else { return nil }

        return nil
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

extension SIMD where Scalar == UInt8 {
    /// A conventient way to init a constant simd vector.
    /// We care that the vector is composed at compile time, so we rely
    /// on optimising compiler inlining and constant folding
    @inline(__always)
    @usableFromInline
    internal init(constant string: StaticString) {
        self = string.withUTF8Buffer {
            assert($0.count == Self.scalarCount)
            return Self($0)
        }
    }
}

@usableFromInline
internal func memcpy(_ span: Span<UInt8>, intoSIMD vector: inout some SIMD<UInt8>) {
    precondition(span.count <= vector.scalarCount, "input buffer exceeds SIMD vector capacity")
    span.withUnsafeBytes { source in
        withUnsafeMutableBytes(of: &vector) { target in
            _ = memcpy(target.baseAddress, source.baseAddress, source.count)
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
                body($0.span)
            }
            if let result { return result }

            // Internal representation does not support non-contiguous normalization.
            // Reallocate when necessary.
            var mutable = self
            return mutable.withUTF8 { body($0.span) }
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
    @export(implementation)
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
