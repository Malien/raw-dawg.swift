import Foundation

internal struct SQLDecoder: Decoder {
    var codingPath: [any CodingKey] = []
    let row: Row

    var userInfo: [CodingUserInfoKey: Any] = [:]

    func container<Key>(keyedBy type: Key.Type) throws -> KeyedDecodingContainer<Key>
    where Key: CodingKey {
        KeyedDecodingContainer(RowDecodingContainer(row: row))
    }

    func unkeyedContainer() throws -> any UnkeyedDecodingContainer {
        throw DecodingError.dataCorrupted(
            .init(
                codingPath: [],
                debugDescription:
                    "Cannot decode an unkeyed container. Decoding from SQL row supports only keyed container. For now"
            ))
    }

    func singleValueContainer() throws -> any SingleValueDecodingContainer {
        guard row.values.count == 1 else {
            throw DecodingError.dataCorrupted(
                .init(
                    codingPath: [],
                    debugDescription:
                        "Single value container for SQL row deserialization expects to be ran on result set of size 1, not \(row.values.count)"
                ))
        }
        return ValueDecodingContainer(value: row[valueAt: 0], codingPath: [])
    }
}

internal struct SQLValueDecoder: Decoder {
    let value: SQLiteValue

    var codingPath: [any CodingKey]

    var userInfo: [CodingUserInfoKey: Any] = [:]

    func container<Key>(keyedBy type: Key.Type) throws -> KeyedDecodingContainer<Key>
    where Key: CodingKey {
        throw DecodingError.dataCorrupted(
            .init(
                codingPath: codingPath,
                debugDescription:
                    "SQLValueDecoder cannot decode a Codable keyed container, only single value one"
            ))
    }

    func unkeyedContainer() throws -> any UnkeyedDecodingContainer {
        throw DecodingError.dataCorrupted(
            .init(
                codingPath: codingPath,
                debugDescription:
                    "SQLValueDecoder cannot decode a Codable unkeyed container, only single value one"
            ))
    }

    func singleValueContainer() throws -> any SingleValueDecodingContainer {
        ValueDecodingContainer(value: value, codingPath: codingPath)
    }
}

private struct RowDecodingContainer<Key: CodingKey>: KeyedDecodingContainerProtocol {
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
            throw DecodingError.keyNotFound(
                key,
                .init(
                    codingPath: [key], debugDescription: "There is no column \(key) in result set"))
        }
        return value
    }

    private static func typeError(
        forConversionTo type: Any.Type, of value: SQLiteValue, forKey key: Key
    ) -> DecodingError {
        .typeMismatch(
            type,
            .init(
                codingPath: [key],
                debugDescription: "Cannot convert sqlite value of \(value) to \(type)"))
    }

    private func decode<T: SQLPrimitiveDecodable>(primitiveForKey key: Key) throws -> T {
        let value = try get(key: key)
        guard let decoded = T.init(fromSQL: value) else {
            throw Self.typeError(forConversionTo: T.self, of: value, forKey: key)
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

    func decode<T>(_ type: T.Type, forKey key: Key) throws -> T where T: Decodable {
        if type == Date.self {
            let date = try decode(primitiveForKey: key) as Date
            return date as! T
        }

        let decoder = SQLValueDecoder(
            value: try get(key: key),
            codingPath: [key])
        return try T.init(from: decoder)
    }

    func nestedContainer<NestedKey>(keyedBy type: NestedKey.Type, forKey key: Key) throws
        -> KeyedDecodingContainer<NestedKey> where NestedKey: CodingKey
    {
        throw DecodingError.dataCorrupted(
            .init(codingPath: [key], debugDescription: "SQLite column values cannot be nested"))
    }

    func nestedUnkeyedContainer(forKey key: Key) throws -> any UnkeyedDecodingContainer {
        throw DecodingError.dataCorrupted(
            .init(codingPath: [key], debugDescription: "SQLite column values cannot be nested"))
    }

    func superDecoder() throws -> any Decoder {
        guard let key = Key(stringValue: "super") else {
            throw DecodingError.dataCorrupted(
                .init(codingPath: [], debugDescription: "Can't construct 'super' key"))
        }
        throw DecodingError.dataCorruptedError(
            forKey: key, in: self, debugDescription: "Decoding super from SQL row isn't supported")
    }

    func superDecoder(forKey key: Key) throws -> any Decoder {
        throw DecodingError.dataCorruptedError(
            forKey: key, in: self, debugDescription: "Decoding super from SQL row isn't supported")
    }
}

private struct ValueDecodingContainer: SingleValueDecodingContainer {
    var value: SQLiteValue
    var codingPath: [any CodingKey]

    private func decodePrimitive<T: SQLPrimitiveDecodable>() throws -> T {
        guard let decoded = T(fromSQL: value) else {
            throw DecodingError.typeMismatch(
                T.self,
                .init(
                    codingPath: codingPath,
                    debugDescription: "Cannot convert sqlite value of \(value) to \(T.self)"))
        }
        return decoded
    }

    func decodeNil() -> Bool {
        value == .null
    }

    func decode(_ type: Bool.Type) throws -> Bool {
        try decodePrimitive()
    }

    func decode(_ type: String.Type) throws -> String {
        try decodePrimitive()
    }

    func decode(_ type: Double.Type) throws -> Double {
        try decodePrimitive()
    }

    func decode(_ type: Float.Type) throws -> Float {
        try decodePrimitive()
    }

    func decode(_ type: Int.Type) throws -> Int {
        try decodePrimitive()
    }

    func decode(_ type: Int8.Type) throws -> Int8 {
        try decodePrimitive()
    }

    func decode(_ type: Int16.Type) throws -> Int16 {
        try decodePrimitive()
    }

    func decode(_ type: Int32.Type) throws -> Int32 {
        try decodePrimitive()
    }

    func decode(_ type: Int64.Type) throws -> Int64 {
        try decodePrimitive()
    }

    func decode(_ type: UInt.Type) throws -> UInt {
        try decodePrimitive()
    }

    func decode(_ type: UInt8.Type) throws -> UInt8 {
        try decodePrimitive()
    }

    func decode(_ type: UInt16.Type) throws -> UInt16 {
        try decodePrimitive()
    }

    func decode(_ type: UInt32.Type) throws -> UInt32 {
        try decodePrimitive()
    }

    func decode(_ type: UInt64.Type) throws -> UInt64 {
        try decodePrimitive()
    }

    func decode<T>(_ type: T.Type) throws -> T where T: Decodable {
        if T.self == Date.self {
            return try decodePrimitive() as Date as! T
        }
        return try T.init(from: SQLValueDecoder(value: value, codingPath: codingPath))
    }
}
