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
        /// 0 is a special value
        let digits: UInt8
        public static func < (lhs: borrowing Self, rhs: borrowing Self) -> Bool {
            lhs.digits < rhs.digits
        }

        public static func digits(_ count: UInt8) -> Self {
            Self(digits: count)
        }
        public static let arbitrary = Self(digits: 0)
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

public enum TimeZoneOption: Sendable {
    case assume(TimeZone)
    case requireOmitted(impose: TimeZone?)
    case rejectOmitted
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

public enum DateComponents: Sendable {
    /// `YYYY-MM-DD`
    case separatedByDashWithLeadingZeros
    /// `YYYYMMDD`
    case conjoinedWithLeadingZeros
    /// `YYYY-MM-DD` or `YYYYMMDD`
    case anyWithLeadingZeros
    // /// `Y-M-D`. allows parsing non-standard dates like 123-8-12
    // case separatedByDashWithoutLeadingZeros
}

public enum TimeComponents: Sendable {
    /// `hh:mm:ss`
    case separatedByColonWithLeadingZeros
    /// `hhmmss`
    case conjoinedWithLeadingZeros
    /// `hh:mm:ss` or `hhmmss`
    case anyWithLeadingZeros
    // /// `h:m:s`. allows parsing non-standard times like 8:4:12
    // case separatedByColonWithoutLeadingZeros
}

public struct DateComponentsTimezoneOption: Sendable, OptionSet {
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
        components: ClosedRange<ComponentPrecision> = .acceptBareDate,
        datetimeSeparator: DateTimeSeparator = [.t, .T, .space],
        dateComponents: DateComponents = .separatedByDashWithLeadingZeros,
        timeComponents: TimeComponents = .separatedByColonWithLeadingZeros,
        timezone: DateComponentsTimezoneOption = [.z, .Z, .hh, .`hh:mm`],
        offsetComponents: TimeComponents = .anyWithLeadingZeros,
    ) {
        assert(
            datetimeSeparator != [],
            "Empty `datetimeSeparator:` parameter will result in always-failing parsing"
        )
        return nil
    }

    // Ideal defaults for later
    // decodingISO8601 span: Span<UInt8>,
    // components: ClosedRange<ComponentPrecision> = .acceptBareDate,
    // datetimeSeparator: DateTimeSeparator = [.t, .T, .space],
    // dateComponents: DateComponents = .anyWithLeadingZeros,
    // timeComponents: TimeComponents = .anyWithLeadingZeros,
    // timezone: DateComponentsTimezoneOption = [.z, .Z, .hh, .`hh:mm`, .`hh:mm:ss`],
    // offsetComponents: TimeComponents = .anyWithLeadingZeros,
}

extension Date {
    @export(implementation)
    public init?(
        decodingISO8601 span: Span<UInt8>,
        components: ClosedRange<ComponentPrecision> = .acceptBareDate,
        datetimeSeparator: DateTimeSeparator = [.t, .T, .space],
        dateComponents: DateComponents = .separatedByDashWithLeadingZeros,
        timeComponents: TimeComponents = .separatedByColonWithLeadingZeros,
        timezone: DateComponentsTimezoneOption = [.z, .Z, .hh, .`hh:mm`],
        offsetComponents: TimeComponents = .anyWithLeadingZeros,
    ) {
        return nil
    }
}
