public struct Row: Equatable, Sequence, Sendable, Collection, RandomAccessCollection {
    private let _columns: [String]
    private let _values: [SQLiteValue].SubSequence

    init(columns: [String], values: [SQLiteValue].SubSequence) {
        assert(
            columns.count == values.count,
            "Tried to construct a row with number of columns (\(columns.count)) being different to the number of provided values (\(values.count)"
        )
        self._columns = columns
        self._values = values
    }

    public typealias Element = (columnName: String, value: SQLiteValue)

    public struct Iterator: IteratorProtocol {
        public typealias Element = Row.Element
        var inner: Zip2Sequence<[String], [SQLiteValue].SubSequence>.Iterator

        public mutating func next() -> Self.Element? {
            return if let (name, value) = inner.next() {
                (name, value)
            } else {
                nil
            }
        }
    }

    public typealias Index = Int

    public func makeIterator() -> Iterator {
        Iterator(inner: zip(_columns, _values).makeIterator())
    }

    public var startIndex: Int { _columns.startIndex }
    public var endIndex: Int { _columns.endIndex }

    public subscript(_ position: Int) -> (columnName: String, value: SQLiteValue) {
        (columnName: _columns[position], value: _values[_values.startIndex + position])
    }

    public func index(after i: Int) -> Int {
        _columns.index(after: i)
    }

    var columns: some RandomAccessCollection<String> { self._columns }
    var values: some RandomAccessCollection<SQLiteValue> { self._values }

    public subscript(column: String) -> SQLiteValue? {
        _columns.firstIndex(of: column).map { idx in
            _values[_values.startIndex + idx]
        }
    }

    public subscript(valueAt index: Int) -> SQLiteValue {
        _values[_values.startIndex + index]
    }

    public func decode<T: SQLPrimitiveDecodable>(valueAt index: Int) throws -> T {
        return try self.decode(valueAt: index, as: T.self)
    }

    public func decode<T: SQLPrimitiveDecodable>(valueAt index: Int, as: T.Type) throws -> T {
        guard let result = T.init(fromSQL: self[valueAt: index]) else {
            throw DecodingError.typeMismatch(
                T.self,
                .init(
                    codingPath: [],
                    debugDescription:
                        "Tried to decode \(T.self) from SQLite value \(self[valueAt: index])"))
        }
        return result
    }

    private struct ColumnName: CodingKey {
        var stringValue: String
        var intValue: Int? { nil }
        init?(intValue: Int) { return nil }
        init?(stringValue: String) { self.stringValue = stringValue }
        init(_ value: String) { self.stringValue = value }
    }

    public func decode<T: SQLPrimitiveDecodable>(valueAt column: String) throws -> T {
        return try self.decode(valueAt: column, as: T.self)
    }

    public func decode<T: SQLPrimitiveDecodable>(valueAt column: String, as: T.Type) throws -> T {
        guard let value = self[column] else {
            throw DecodingError.keyNotFound(
                ColumnName(column), .init(codingPath: [], debugDescription: ""))
        }
        guard let decoded = T.init(fromSQL: value) else {
            throw DecodingError.typeMismatch(
                T.self,
                .init(
                    codingPath: [],
                    debugDescription: "Tried to decode \(T.self) from SQLite value \(value)"))
        }
        return decoded
    }

    public func decode<T: Decodable>() throws -> T {
        try T(from: SQLDecoder(row: self))
    }
}
