import Foundation

public enum SQLiteBlob: Equatable, SQLPrimitiveDecodable, Decodable {
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
    
    public init(from decoder: any Decoder) throws {
        guard let decoder = decoder as? SQLValueDecoder else {
            throw DecodingError.typeMismatch(Self.self, .init(codingPath: decoder.codingPath, debugDescription: "SQLiteBlob can only be decoded from RawDawg.Row (aka. SQLValueDecoder)"))
        }
        guard case .blob(let blob) = decoder.value else {
            throw DecodingError.typeMismatch(Self.self, .init(codingPath: decoder.codingPath, debugDescription: "Cannot decode SQLiteBlob from \(decoder.value)"))
        }
        self = blob
    }
}

public struct ConversionError: Error, CustomStringConvertible {
    var from: SQLiteValue
    var to: SQLPrimitiveDecodable.Type
    
    public var description: String {
        "Cannot convert SQLite value \(from) to type \(to)"
    }
}

public enum SQLiteValue: Equatable {
    case null
    case integer(Int64)
    case float(Float64)
    case text(String)
    case blob(SQLiteBlob)
    
    func get<T: SQLPrimitiveDecodable>() throws -> T {
        if let value = T.init(fromSQL: self) {
            return value
        } else {
            throw ConversionError(from: self, to: T.self)
        }
    }
    
    func decode<T: SQLPrimitiveDecodable>() -> T? {
        T.init(fromSQL: self)
    }
}

public struct SQLNull: Equatable, Hashable {}

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

extension SQLNull: SQLPrimitiveDecodable {
    init?(fromSQL primitive: SQLiteValue) {
        guard case .null = primitive else {
            return nil
        }
    }
}

extension SQLNull: Decodable {
    public init(from decoder: any Decoder) throws {
        let container = try decoder.singleValueContainer()
        guard container.decodeNil() else {
            throw DecodingError.typeMismatch(SQLNull.self, .init(codingPath: container.codingPath, debugDescription: "SQLNull can only be deserialized from a 'nil' value"))
        }
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
