public struct SQLDecoder: Decoder {
    public var codingPath: [any CodingKey] = []
    let row: Row
    
    public var userInfo: [CodingUserInfoKey : Any] = [:]
    
    public func container<Key>(keyedBy type: Key.Type) throws -> KeyedDecodingContainer<Key> where Key : CodingKey {
        KeyedDecodingContainer(RowDecodingContainer(row: row))
    }
    
    public func unkeyedContainer() throws -> any UnkeyedDecodingContainer {
        throw DecodingError.dataCorrupted(.init(codingPath: [], debugDescription: "Cannot decode an unkeyed container. Decoding from SQL row supports only keyed container. For now"))
    }
    
    public func singleValueContainer() throws -> any SingleValueDecodingContainer {
        throw DecodingError.dataCorrupted(.init(codingPath: [], debugDescription: "Cannot decode a single value container. Decoding from SQL row supports only keyed container. For now"))
    }
    
    struct RowDecodingContainer<Key: CodingKey>: KeyedDecodingContainerProtocol {
        fileprivate let row: Row
        
        var codingPath: [any CodingKey] = []
        
        var allKeys: [Key] {
            row.columns.compactMap { Key(stringValue: $0) }
        }
        
        func contains(_ key: Key) -> Bool {
            row.columns.contains(key.stringValue)
        }
        
        private func get(key: Key) throws -> SQLiteValue {
            guard let (_, value) = row.first(where: { $0.columnName == key.stringValue }) else {
                throw DecodingError.keyNotFound(key, .init(codingPath: [key], debugDescription: "There is no column \(key) in result set"))
            }
            return value
        }
        
        private static func typeError(forConversionTo type: Any.Type, of value: SQLiteValue, forKey key: Key) -> DecodingError {
            .typeMismatch(type, .init(codingPath: [key], debugDescription: "Cannot convert sqlite value of \(value) to \(type)"))
        }
        
        private func decode<T: SQLPrimitiveDecodable>(primitiveForKey key: Key) throws -> T {
            let value = try get(key: key)
            guard let decoded = T.init(fromSQL: value) else {
                throw Self.typeError(forConversionTo: Bool.self, of: value, forKey: key)
            }
            return decoded
        }
        
        func decodeNil(forKey key: Key) throws -> Bool {
            return try get(key: key) == .null
        }
        
        func decode(_ type: Bool.Type, forKey key: Key) throws -> Bool {
            try decode(primitiveForKey: key)
        }
        
        func decode(_ type: String.Type, forKey key: Key) throws -> String {
            try decode(primitiveForKey: key)
        }
        
        func decode(_ type: Double.Type, forKey key: Key) throws -> Double {
            try decode(primitiveForKey: key)
        }
        
        func decode(_ type: Float.Type, forKey key: Key) throws -> Float {
            try decode(primitiveForKey: key)
        }
        
        func decode(_ type: Int.Type, forKey key: Key) throws -> Int {
            try decode(primitiveForKey: key)
        }

        func decode(_ type: Int8.Type, forKey key: Key) throws -> Int8 {
            try decode(primitiveForKey: key)
        }
        
        func decode(_ type: Int16.Type, forKey key: Key) throws -> Int16 {
            try decode(primitiveForKey: key)
        }
        
        func decode(_ type: Int32.Type, forKey key: Key) throws -> Int32 {
            try decode(primitiveForKey: key)
        }
        
        func decode(_ type: Int64.Type, forKey key: Key) throws -> Int64 {
            try decode(primitiveForKey: key)
        }

        func decode(_ type: UInt.Type, forKey key: Key) throws -> UInt {
            try decode(primitiveForKey: key)
        }
        
        func decode(_ type: UInt8.Type, forKey key: Key) throws -> UInt8 {
            try decode(primitiveForKey: key)
        }
        
        func decode(_ type: UInt16.Type, forKey key: Key) throws -> UInt16 {
            try decode(primitiveForKey: key)
        }
        
        func decode(_ type: UInt32.Type, forKey key: Key) throws -> UInt32 {
            try decode(primitiveForKey: key)
        }
        
        func decode(_ type: UInt64.Type, forKey key: Key) throws -> UInt64 {
            try decode(primitiveForKey: key)
        }
        
        func decode<T>(_ type: T.Type, forKey key: Key) throws -> T where T : Decodable {
            let decoder = SQLValueDecoder(value: try get(key: key),
                                          codingPath: [key])
            return try T.init(from: decoder)
        }
        
        func nestedContainer<NestedKey>(keyedBy type: NestedKey.Type, forKey key: Key) throws -> KeyedDecodingContainer<NestedKey> where NestedKey : CodingKey {
            throw DecodingError.dataCorrupted(.init(codingPath: [key], debugDescription: "SQLite column values cannot be nested"))
        }
        
        func nestedUnkeyedContainer(forKey key: Key) throws -> any UnkeyedDecodingContainer {
            throw DecodingError.dataCorrupted(.init(codingPath: [key], debugDescription: "SQLite column values cannot be nested"))
        }
        
        func superDecoder() throws -> any Decoder {
            guard let key = Key(stringValue: "super") else {
                throw DecodingError.dataCorrupted(.init(codingPath: [], debugDescription: "Can't construct 'super' key"))
            }
            throw DecodingError.dataCorruptedError(forKey: key, in: self, debugDescription: "Decoding super from SQL row isn't supported")
        }
        
        func superDecoder(forKey key: Key) throws -> any Decoder {
            throw DecodingError.dataCorruptedError(forKey: key, in: self, debugDescription: "Decoding super from SQL row isn't supported")
        }
    }
}

public struct SQLValueDecoder: Decoder {
    let value: SQLiteValue
    
    public var codingPath: [any CodingKey]

    public var userInfo: [CodingUserInfoKey : Any] = [:]
    
    public func container<Key>(keyedBy type: Key.Type) throws -> KeyedDecodingContainer<Key> where Key : CodingKey {
        throw DecodingError.dataCorrupted(.init(codingPath: codingPath, debugDescription: "SQLValueDecoder cannot decode a Codable keyed container, only single value one"))
    }
    
    public func unkeyedContainer() throws -> any UnkeyedDecodingContainer {
        throw DecodingError.dataCorrupted(.init(codingPath: codingPath, debugDescription: "SQLValueDecoder cannot decode a Codable unkeyed container, only single value one"))
    }
    
    public func singleValueContainer() throws -> any SingleValueDecodingContainer {
        ValueDecodingContainer(value: value, codingPath: codingPath)
    }
    
    struct ValueDecodingContainer: SingleValueDecodingContainer {
        var value: SQLiteValue
        var codingPath: [any CodingKey]
        
        private func decode<T: SQLPrimitiveDecodable>(fromPrimitive value: SQLiteValue) throws -> T {
            guard let decoded = T(fromSQL: value) else {
                throw DecodingError.typeMismatch(T.self, .init(codingPath: codingPath, debugDescription: "Cannot convert sqlite value of \(value) to \(T.self)"))
            }
            return decoded
        }
        
        func decodeNil() -> Bool {
            value == .null
        }
        
        func decode(_ type: Bool.Type) throws -> Bool {
            try decode(fromPrimitive: value)
        }
        
        func decode(_ type: String.Type) throws -> String {
            try decode(fromPrimitive: value)
        }
        
        func decode(_ type: Double.Type) throws -> Double {
            try decode(fromPrimitive: value)
        }
        
        func decode(_ type: Float.Type) throws -> Float {
            try decode(fromPrimitive: value)
        }
        
        func decode(_ type: Int.Type) throws -> Int {
            try decode(fromPrimitive: value)
        }
        
        func decode(_ type: Int8.Type) throws -> Int8 {
            try decode(fromPrimitive: value)
        }
        
        func decode(_ type: Int16.Type) throws -> Int16 {
            try decode(fromPrimitive: value)
        }
        
        func decode(_ type: Int32.Type) throws -> Int32 {
            try decode(fromPrimitive: value)
        }
        
        func decode(_ type: Int64.Type) throws -> Int64 {
            try decode(fromPrimitive: value)
        }
        
        func decode(_ type: UInt.Type) throws -> UInt {
            try decode(fromPrimitive: value)
        }
        
        func decode(_ type: UInt8.Type) throws -> UInt8 {
            try decode(fromPrimitive: value)
        }
        
        func decode(_ type: UInt16.Type) throws -> UInt16 {
            try decode(fromPrimitive: value)
        }
        
        func decode(_ type: UInt32.Type) throws -> UInt32 {
            try decode(fromPrimitive: value)
        }
        
        func decode(_ type: UInt64.Type) throws -> UInt64 {
            try decode(fromPrimitive: value)
        }
        
        func decode<T>(_ type: T.Type) throws -> T where T : Decodable {
            try T.init(from: SQLValueDecoder(value: value, codingPath: codingPath))
        }
    }
}
