import Foundation

public enum SQLiteBlob: Equatable, SQLPrimitiveDecodable, Decodable, SQLPrimitiveEncodable {
    case empty
    case loaded(Data)
    case stream(Never)
    
    init(bytes: UnsafeRawPointer, count: Int) {
        self = .loaded(Data(bytes: bytes, count: count))
    }
    
    init<SourceType>(buffer: UnsafeBufferPointer<SourceType>) {
        self = .loaded(Data(buffer: buffer))
    }
    
    init?(fromSQL primitive: SQLiteValue) {
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
            throw DecodingError.typeMismatch(Self.self, .init(codingPath: decoder.codingPath, debugDescription: "SQLiteBlob can only be decoded from RawDawg.Row (aka. SQLValueDecoder)"))
        }
        guard case .blob(let blob) = decoder.value else {
            throw DecodingError.typeMismatch(Self.self, .init(codingPath: decoder.codingPath, debugDescription: "Cannot decode SQLiteBlob from \(decoder.value)"))
        }
        self = blob
    }
    
    public func encode() -> SQLiteValue {
        .blob(self)
    }
}

public struct ConversionError: Error, CustomStringConvertible {
    var from: SQLiteValue
    var to: SQLPrimitiveDecodable.Type
    
    public var description: String {
        "Cannot convert SQLite value \(from) to type \(to)"
    }
}

public enum SQLiteValue: Equatable, SQLPrimitiveDecodable, SQLPrimitiveEncodable, CustomStringConvertible {
    case null
    case integer(Int64)
    case float(Float64)
    case text(String)
    case blob(SQLiteBlob)
    
    init?(fromSQL primitive: SQLiteValue) {
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

public struct SQLNull: Equatable, Hashable, SQLPrimitiveDecodable, Decodable, SQLPrimitiveEncodable  {
    public init() {}
    
    init?(fromSQL primitive: SQLiteValue) {
        guard case .null = primitive else {
            return nil
        }
    }

    public init(from decoder: any Decoder) throws {
        let container = try decoder.singleValueContainer()
        guard container.decodeNil() else {
            throw DecodingError.typeMismatch(SQLNull.self, .init(codingPath: container.codingPath, debugDescription: "SQLNull can only be deserialized from a 'nil' value"))
        }
    }
    
    public func encode() -> SQLiteValue {
        .null
    }
}

protocol SQLPrimitiveDecodable {
    init?(fromSQL primitive: SQLiteValue)
}

extension Optional: SQLPrimitiveDecodable where Wrapped: SQLPrimitiveDecodable {
    init?(fromSQL primitive: SQLiteValue) {
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
    init?(fromSQL primitive: SQLiteValue) {
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
    init?(fromSQL primitive: SQLiteValue) {
        guard case .text(let string) = primitive else {
            return nil
        }
        self = string
    }
}

extension Double: SQLPrimitiveDecodable {
    init?(fromSQL primitive: SQLiteValue) {
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
    init?(fromSQL primitive: SQLiteValue) {
        guard case .float(let double) = primitive else {
            return nil
        }
        self = Float(double)
    }
}

internal extension FixedWidthInteger {
    init?(exactlyWithinBounds fp: Double) {
        guard fp <= Double(Self.max) else {
            return nil
        }
        guard fp >= Double(Self.min) else {
            return nil
        }
        self.init(exactly: fp)
    }
    
    init?(fromSQL primitive: SQLiteValue) where Self: SignedInteger {
        switch primitive {
        case .integer(let value) where (Self.bitWidth >= Int64.bitWidth || value <= Int64(Self.max)):
            self.init(value)
        case .float(let value):
            self.init(exactlyWithinBounds: value)
        default:
            return nil
        }
    }
    
    init?(fromSQL primitive: SQLiteValue) where Self: UnsignedInteger {
        switch primitive {
        case .integer(let value) where value > 0 && (Self.bitWidth >= Int64.bitWidth || value <= Int64(Self.max)):
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

@available(macOS 11.0, *)
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
