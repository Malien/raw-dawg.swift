import Foundation
import SIMDate

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
            self = false
        case .integer(_):
            self = true
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
        case .text(let iso8601String):
            self.init(
                decodingISO8601: iso8601String,
                components: .datetime(
                    separator: [.T, .space],
                    range: .date ... .subsecond(precision: .nanos)
                ),
                timezone: .assume(.init(secondsFromGMT: 0)!, accept: [.Z, .`hh:mm`, .hhmm, .hh])
            )

        }
    }
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

extension Bool: SQLPrimitiveEncodable {
    public func encode() -> SQLiteValue {
        .integer(self ? 1 : 0)
    }
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
