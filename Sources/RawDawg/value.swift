import Foundation

/// A Swift representation of SQLite's `BLOB` storage type
///
/// Can be one of three things:
/// - In-memory buffer of bytes, represented by Foundation's `Data` type
/// - A `NULL` (or empty) value
/// - A handle to the BLOB object, allowing for incremental reads and writes (streaming)
public enum SQLiteBlob: Equatable, SQLPrimitiveDecodable, Decodable, SQLPrimitiveEncodable, Sendable
{
    /// Empty or `NULL` state
    case empty
    /// In-memory buffer of bytes
    case loaded(Data)
    /// A handle to the BLOB object. **Not implemented**
    case stream(Never)

    init(bytes: UnsafeRawPointer, count: Int) {
        self = .loaded(Data(bytes: bytes, count: count))
    }

    init<SourceType>(buffer: UnsafeBufferPointer<SourceType>) {
        self = .loaded(Data(buffer: buffer))
    }

    public init?(fromSQL primitive: SQLiteValue) {
        guard case .blob(let blob) = primitive else {
            return nil
        }
        self = blob
    }

    init<T>(_ bytes: T) where T: Sequence, T.Element == UInt8 {
        self = .loaded(Data(bytes))
    }

    public init(from decoder: any Decoder) throws {
        guard let decoder = decoder as? SQLValueDecoder else {
            throw DecodingError.typeMismatch(
                Self.self,
                .init(
                    codingPath: decoder.codingPath,
                    debugDescription:
                        "SQLiteBlob can only be decoded from RawDawg.Row (aka. SQLValueDecoder)"))
        }
        guard case .blob(let blob) = decoder.value else {
            throw DecodingError.typeMismatch(
                Self.self,
                .init(
                    codingPath: decoder.codingPath,
                    debugDescription: "Cannot decode SQLiteBlob from \(decoder.value)"))
        }
        self = blob
    }

    public func encode() -> SQLiteValue {
        .blob(self)
    }
}

/// Owned SQLite value of all possible storage types
public enum SQLiteValue: Equatable, SQLPrimitiveDecodable, SQLPrimitiveEncodable,
    CustomStringConvertible, Sendable
{
    case null
    case integer(Int64)
    case float(Float64)
    case text(String)
    case blob(SQLiteBlob)

    public init?(fromSQL primitive: SQLiteValue) {
        self = primitive
    }

    public func encode() -> SQLiteValue {
        self
    }

    public var description: String {
        switch self {
        case .null: "NULL"
        case .integer(let int64): int64.description
        case .float(let float): float.description
        case .text(let text): String(reflecting: text)
        case .blob(let blob): String(reflecting: blob)
        }
    }
}

/// An empty struct representing a `NULL` SQL value. Limited usefulness.
public struct SQLNull: Equatable, Hashable, SQLPrimitiveDecodable, Decodable, SQLPrimitiveEncodable,
    Sendable
{
    public init() {}

    public init?(fromSQL primitive: SQLiteValue) {
        guard case .null = primitive else {
            return nil
        }
    }

    public init(from decoder: any Decoder) throws {
        let container = try decoder.singleValueContainer()
        guard container.decodeNil() else {
            throw DecodingError.typeMismatch(
                SQLNull.self,
                .init(
                    codingPath: container.codingPath,
                    debugDescription: "SQLNull can only be deserialized from a 'nil' value"))
        }
    }

    public func encode() -> SQLiteValue {
        .null
    }
}

// MARK: SQLPrimitiveDecodable

/// A way of decoding dynamic ``SQLiteValue`` into more specific type.
///
/// Can be used to specialize the behaviour of ``Row/decode(valueAt:)-8i6gt``,
/// ``PreparedStatement/fetchAll()-6jov4``, ``PreparedStatement/fetchOne()-8yva9``,
/// ``PreparedStatement/fetchOptional()-92nz3``, ``PreparedStatement/step()-92san``
///
/// ```swift
/// struct UserID: SQLValueDecodable {
///     var rawValue: Int
///
///     init?(fromSQL primitive: SQLiteValue) {
///         guard case .integer(let int) = primitive else {
///             return nil
///         }
///         self.init(rawValue: int)
///     }
/// }
///
/// let users: [(UserID, String)] = db.prepare("select id, name from users").fetchAll()
/// ```
public protocol SQLPrimitiveDecodable {
    /// Try to intialize Self given ``SQLiteValue``, or return `nil` if conversion isn't possible
    init?(fromSQL primitive: SQLiteValue)
}

extension Optional: SQLPrimitiveDecodable where Wrapped: SQLPrimitiveDecodable {
    public init?(fromSQL primitive: SQLiteValue) {
        if case .null = primitive {
            self = .none
        } else if let inner = Wrapped.init(fromSQL: primitive) {
            self = inner
        } else {
            return nil
        }
    }
}

extension Bool: SQLPrimitiveDecodable {
    public init?(fromSQL primitive: SQLiteValue) {
        switch primitive {
        case .integer(0):
            self = true
        case .integer(_):
            self = false
        default:
            return nil
        }
    }
}

extension String: SQLPrimitiveDecodable {
    public init?(fromSQL primitive: SQLiteValue) {
        guard case .text(let string) = primitive else {
            return nil
        }
        self = string
    }
}

extension Double: SQLPrimitiveDecodable {
    public init?(fromSQL primitive: SQLiteValue) {
        switch primitive {
        case .integer(let value):
            self = Double(value)
        case .float(let value):
            self = value
        default:
            return nil
        }
    }
}

extension Float: SQLPrimitiveDecodable {
    public init?(fromSQL primitive: SQLiteValue) {
        guard case .float(let double) = primitive else {
            return nil
        }
        self = Float(double)
    }
}

extension FixedWidthInteger {
    init?(exactlyWithinBounds fp: Double) {
        guard fp <= Double(Self.max) else {
            return nil
        }
        guard fp >= Double(Self.min) else {
            return nil
        }
        self.init(exactly: fp)
    }

    public init?(fromSQL primitive: SQLiteValue) where Self: SignedInteger {
        switch primitive {
        case .integer(let value)
        where (Self.bitWidth >= Int64.bitWidth || value <= Int64(Self.max)):
            self.init(value)
        case .float(let value):
            self.init(exactlyWithinBounds: value)
        default:
            return nil
        }
    }

    public init?(fromSQL primitive: SQLiteValue) where Self: UnsignedInteger {
        switch primitive {
        case .integer(let value)
        where value > 0 && (Self.bitWidth >= Int64.bitWidth || value <= Int64(Self.max)):
            self.init(value)
        case .float(let value):
            self.init(exactlyWithinBounds: value)
        default:
            return nil
        }
    }
}

extension Int: SQLPrimitiveDecodable {}
extension Int8: SQLPrimitiveDecodable {}
extension Int16: SQLPrimitiveDecodable {}
extension Int32: SQLPrimitiveDecodable {}
extension Int64: SQLPrimitiveDecodable {}

extension UInt: SQLPrimitiveDecodable {}
extension UInt8: SQLPrimitiveDecodable {}
extension UInt16: SQLPrimitiveDecodable {}
extension UInt32: SQLPrimitiveDecodable {}
extension UInt64: SQLPrimitiveDecodable {}

extension Date: SQLPrimitiveDecodable {
    /// Conversions are per how [SQLite can store date/time/datetime values](https://www.sqlite.org/lang_datefunc.html)
    /// All numeric values are treated as unix epoch. If you are dealing in julian calendar days I am trully sorry.
    /// Write your own smal 'lil type and conform it to `SQLPrimitiveDecodable`/`Codable`.
    public init?(fromSQL primitive: SQLiteValue) {
        switch primitive {
        case .null, .blob(_):
            return nil
        case .integer(let unixEpoch):
            self.init(timeIntervalSince1970: Double(unixEpoch))
        case .float(let unixEpoch):
            self.init(timeIntervalSince1970: unixEpoch)
        case .text(var iso8601String):
            // TODO: Implement a more efficient iso8601 string parsing
            if let space = iso8601String.firstIndex(of: " ") {
                iso8601String.replaceSubrange(space...space, with: "T")
            }
            if !isUTC(iso8601: iso8601String) {
                iso8601String += "Z"
            }
            let formatter = ISO8601DateFormatter()
            formatter.formatOptions.insert(.withFractionalSeconds)
            if let date = formatter.date(from: iso8601String) {
                self = date
            } else {
                return nil
            }
        }
    }
}

@available(macOS, obsoleted: 12.0)
let iso8601Pattern = try! NSRegularExpression(
    pattern:
        #"^(?<year>\d\d\d\d)-(?<month>\d\d)-(?<day>\d\d)[ T](?<hour>\d\d):(?<minute>\d\d):(?<second>\d\d)(?:\.(?<subsec>\d\d\d))?(?<offset>Z|(?:[+-]\d\d(?:(?:\d\d)|(?::\d\d))?))?$"#
)

// TODO: Implement a more efficient iso8601 string parsing
/// There is of course `Date(_:strategy:)` and ISO8601FormattingStrategy. Unfourtunatelly it is pretty strict when it comes to datetime formats.
/// In-particular it is slightly incompatible with the `datetime()` sqlite function. It yields values without the `Z` offset, and with space instad of `T` as a seprator
/// As such the purpose of this function is to parse those datetimes, which kinda look like ISO8601, but not quite (hence the -ish).
/// The only format I really need to parse is `yyyy-MM-DD HH:MM:SS.SSS`, but it would be a nice bonus to be more permissive in handling.
/// Honestly, non-Z offsets is a terrible idea. At least cause it breaks the property of datetime string represenations bein ordered.
///
/// The performance of this implementation is lackluster. It reallocates string as a UTF-16 NSString-like on platforms < macOS 12.
/// Plus all of the named regex lookups can't be good.
/// And I kinda doubt the swifts regexes are as optimal as hand-written parsing code
@available(macOS, obsoleted: 13.0)
private func parseDate(usingNSRegex string: String) -> Date? {
    guard
        let match = iso8601Pattern.firstMatch(
            in: string, range: NSRange(string.startIndex..<string.endIndex, in: string))
    else {
        return nil
    }

    // Don't mind all of the force unwrappings. The pattern requires there to be matches, or it would've failed beforehand.
    let yearRange = Range(match.range(withName: "year"), in: string)!
    let monthRange = Range(match.range(withName: "month"), in: string)!
    let dayRange = Range(match.range(withName: "day"), in: string)!
    let hourRange = Range(match.range(withName: "hour"), in: string)!
    let minuteRange = Range(match.range(withName: "minute"), in: string)!
    let secondRange = Range(match.range(withName: "second"), in: string)!
    let subsecondRange = Range(match.range(withName: "second"), in: string)
    let offsetRange = Range(match.range(withName: "offset"), in: string)

    let year = Int(string[yearRange])!
    let month = Int(string[monthRange])!
    let day = Int(string[dayRange])!
    let hour = Int(string[hourRange])!
    let minute = Int(string[minuteRange])!
    let second = Int(string[secondRange])!
    // subseconds are in 10^-3. nanos are in 10^-9
    let nanosecond = subsecondRange.map { Int(string[$0])! * 1_000_000 }
    let offset = offsetRange.map { TimeZoneOffset(raw: string[$0]) } ?? TimeZoneOffset.utc

    let calendar = Calendar(identifier: .iso8601)
    let components = DateComponents(
        calendar: calendar,
        timeZone: offset.timeZone,
        year: year,
        month: month,
        day: day,
        hour: hour,
        minute: minute,
        second: second,
        nanosecond: nanosecond
    )
    return calendar.date(from: components)
}

/// There is of course `Date(_:strategy:)` and ISO8601FormattingStrategy. Unfourtunatelly it is pretty strict when it comes to datetime formats.
/// In-particular it is slightly incompatible with the `datetime()` sqlite function. It yields values without the `Z` offset, and with space instad of `T` as a seprator
/// As such the purpose of this function is to parse those datetimes, which kinda look like ISO8601, but not quite (hence the -ish).
/// The only format I really need to parse is `yyyy-MM-DD HH:MM:SS.SSS`, but it would be a nice bonus to be more permissive in handling.
/// Honestly, non-Z offsets is a terrible idea. At least cause it breaks the property of datetime string represenations bein ordered.
///
/// The performance of this implementation is lackluster. It reallocates string as a UTF-16 NSString-like on platforms < macOS 12.
/// Plus all of the named regex lookups can't be good.
/// And I kinda doubt the swifts regexes are as optimal as hand-written parsing code
//@available(macOS 13.0, *)
//private func parseDate(usingSwiftRegex string: String) -> Date? {
//    SwiftRegexParsingImpl.parseDate(iso8601ish: string)
//}

private enum TimeZoneOffset {
    case utc
    case offset(sign: Int, hours: Int, minutes: Int)

    var timeZone: TimeZone? {
        switch self {
        case .utc:
            return TimeZone(identifier: "UTC")  // In swift TimeZone.utc == TimeZone(identifier: "UTC")
        case .offset(let sign, let hours, let minutes):
            return TimeZone(secondsFromGMT: sign * (hours * 60 * 60 + minutes * 60))
        }
    }

    init(raw: Substring) {
        if raw == "Z" {
            self = .utc
            return
        }

        var cursor = raw.startIndex
        let sign = if raw[cursor] == "-" { -1 } else { 1 }

        let afterHours = raw.index(cursor, offsetBy: 2)
        let hours = Int(raw[cursor..<afterHours])!
        cursor = afterHours
        if cursor == raw.endIndex {
            self = .offset(sign: sign, hours: hours, minutes: 00)
            return
        }
        if raw[cursor] == ":" {
            cursor = raw.index(after: cursor)
        }
        let minutes = Int(raw[cursor..<raw.endIndex])!
        self = .offset(sign: sign, hours: hours, minutes: minutes)
    }
}

//#if canImport(RegexBuilder)
//    import RegexBuilder
//#endif

//@available(macOS 13.0, *)
//private enum SwiftRegexParsingImpl {
//    static let yearRef = Reference(Int.self)
//    static let monthRef = Reference(Int.self)
//    static let dayRef = Reference(Int.self)
//    static let hourRef = Reference(Int.self)
//    static let minuteRef = Reference(Int.self)
//    static let secondRef = Reference(Int.self)
//    static let subsecondRef = Reference(Int?.self)
//    static let offsetRef = Reference(TimeZoneOffset?.self)
//
//    static func digits(_ count: Int, as ref: Reference<Int>) -> some RegexComponent {
//        Capture(as: ref) {
//            Repeat(.digit, count: count)
//        } transform: {
//            Int($0)!
//        }
//    }
//
//    static func digits(_ count: Int, as ref: Reference<Int?>) -> some RegexComponent {
//        Capture(as: ref) {
//            Repeat(.digit, count: count)
//        } transform: {
//            Int($0)!
//        }
//    }
//
//    static let offset = ChoiceOf {
//        "Z"
//        Local {
//            ChoiceOf {
//                "+"
//                "-"
//            }
//            Repeat(.digit, count: 2)
//            Optionally {
//                ChoiceOf {
//                    Local {
//                        ":"
//                        Repeat(.digit, count: 2)
//                    }
//                    Repeat(.digit, count: 2)
//                }
//            }
//        }
//    }
//
//    static let iso8601Regex = Regex {
//        Anchor.startOfSubject
//        digits(2, as: yearRef)
//        "-"
//        digits(2, as: monthRef)
//        "-"
//        digits(2, as: dayRef)
//        ChoiceOf {
//            " "
//            "T"
//        }
//        digits(2, as: hourRef)
//        ":"
//        digits(2, as: minuteRef)
//        ":"
//        digits(2, as: secondRef)
//        Optionally {
//            "."
//            digits(3, as: subsecondRef)
//        }
//        Optionally {
//            Capture(as: offsetRef) {
//                offset
//            } transform: {
//                TimeZoneOffset(raw: $0)
//            }
//        }
//        Anchor.endOfSubject
//    }
//
//    static func parseDate(iso8601ish string: String) -> Date? {
//        guard let match = string.wholeMatch(of: iso8601Regex) else {
//            return nil
//        }
//
//        let offset = match[offsetRef] ?? TimeZoneOffset.utc
//        let calendar = Calendar(identifier: .iso8601)
//        let components = DateComponents(
//            calendar: calendar,
//            timeZone: offset.timeZone,
//            year: match[yearRef],
//            month: match[monthRef],
//            day: match[dayRef],
//            hour: match[hourRef],
//            minute: match[minuteRef],
//            second: match[secondRef],
//            // subseconds are in 10^-3. nanos are in 10^-9
//            nanosecond: match[subsecondRef].map { $0 * 1_000_000 }
//        )
//        return calendar.date(from: components)
//    }
//}

private func isUTC(iso8601 string: String) -> Bool {
    if string.hasSuffix("Z") { return true }
    guard var idx = string.lastIndex(of: "+") ?? string.lastIndex(of: "-") else {
        return false
    }
    var offsetDigits = 0
    var encounteredSeparator = false

    while idx != string.endIndex {
        switch string[idx] {
        case let ch where ch.isNumber:
            offsetDigits += 1
        case ":" where !encounteredSeparator && offsetDigits == 2:
            encounteredSeparator = true
        default: return false
        }
        idx = string.index(after: idx)
    }
    return offsetDigits == 2 || offsetDigits == 4
}

// MARK: SQLPrimitiveEncodable

/// A way of encoding a swift value into ``SQLiteValue``
///
/// Used to customize behaviour of ``BoundQuery``
///
/// ```swift
/// struct UserID: SQLPrimitiveEncodable {
///     var rawValue: Int
///
///     func encode() -> SQLiteValue {
///         .integer(rawValue)
///     }
/// }
///
/// func fetchUser(byID id: UserID) async throws -> User? {
///     try await db.prepare("select * from users where id = \(id)").fetchOptional()
/// }
/// ```
public protocol SQLPrimitiveEncodable {
    consuming func encode() -> SQLiteValue
}

extension String: SQLPrimitiveEncodable {
    public func encode() -> SQLiteValue {
        .text(self)
    }
}

extension FixedWidthInteger {
    public func encode() -> SQLiteValue {
        .integer(Int64(self))
    }
}

extension Int: SQLPrimitiveEncodable {}
extension Int8: SQLPrimitiveEncodable {}
extension Int16: SQLPrimitiveEncodable {}
extension Int32: SQLPrimitiveEncodable {}
extension Int64: SQLPrimitiveEncodable {}

extension UInt: SQLPrimitiveEncodable {}
extension UInt8: SQLPrimitiveEncodable {}
extension UInt16: SQLPrimitiveEncodable {}
extension UInt32: SQLPrimitiveEncodable {}
extension UInt64: SQLPrimitiveEncodable {}

@available(macOS 11.0, iOS 14.0, *)
extension Float16: SQLPrimitiveEncodable {
    public func encode() -> SQLiteValue {
        .float(Double(self))
    }
}

extension Float32: SQLPrimitiveEncodable {
    public func encode() -> SQLiteValue {
        .float(Double(self))
    }
}

extension Float64: SQLPrimitiveEncodable {
    public func encode() -> SQLiteValue {
        .float(self)
    }
}

extension Optional: SQLPrimitiveEncodable where Wrapped: SQLPrimitiveEncodable {
    public func encode() -> SQLiteValue {
        switch self {
        case .none: .null
        case .some(let inner): inner.encode()
        }
    }
}
