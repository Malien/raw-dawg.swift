/// Decoded, owned row, with column values
///
/// ## Topics
/// ### Decode into Swift value
/// - ``decode()-5gu78``
/// - ``decode()-73lf8``
/// ### Get value given the column name
/// - ``subscript(_:)-8ic3s``
/// - ``decode(valueAt:)-8i6gt``
/// - ``decode(valueAt:as:)-5zo4x``
/// ### Get value at index
/// - ``subscript(valueAt:)``
/// - ``decode(valueAt:)-797yx``
/// - ``decode(valueAt:as:)-5maig``
/// ### Key-value pairs
/// - ``subscript(_:)-maj6``
/// - ``Iterator``
/// - ``makeIterator()``
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

    /// An interator of `(columnName:, value:)` pairs
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

    /// Get the value in the column, by it's name
    /// - Returns: `nil` if the column doesn't exist, ``SQLiteValue`` othervise (even if the value itself if ``SQLiteValue/null``)
    public subscript(column: String) -> SQLiteValue? {
        _columns.firstIndex(of: column).map { idx in
            _values[_values.startIndex + idx]
        }
    }

    /// Get the value in the column, by it's index in the result set
    ///
    /// Will panic if the index is out of bounds
    /// - Precondition: `index` is in bounds
    public subscript(valueAt index: Int) -> SQLiteValue {
        _values[_values.startIndex + index]
    }

    /// Decode the ``SQLPrimitiveDecodable`` value in the colum, by it's index
    ///
    /// Will panic if the index is out of bounds
    /// - Precondition: `index` is in bounds
    public func decode<Column: SQLPrimitiveDecodable>(valueAt index: Int) throws -> Column {
        return try self.decode(valueAt: index, as: Column.self)
    }

    /// Decode the ``SQLPrimitiveDecodable`` value in the colum indexed by it's position in the result set
    ///
    /// Will panic if the index is out of bounds
    /// - Precondition: `index` is in bounds
    public func decode<Column: SQLPrimitiveDecodable>(valueAt index: Int, as: Column.Type) throws
        -> Column
    {
        guard let result = Column.init(fromSQL: self[valueAt: index]) else {
            throw DecodingError.typeMismatch(
                Column.self,
                .init(
                    codingPath: [],
                    debugDescription:
                        "Tried to decode \(Column.self) from SQLite value \(self[valueAt: index])"))
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

    /// Decode the ``SQLPrimitiveDecodable`` value in the column indexed by it's name
    ///
    /// - Throws: `DecodingError` if the column with specified name doesn't exist
    public func decode<Column: SQLPrimitiveDecodable>(valueAt column: String) throws -> Column {
        return try self.decode(valueAt: column, as: Column.self)
    }

    /// Decode the ``SQLPrimitiveDecodable`` value in the column indexed by it's name
    ///
    /// - Throws: `DecodingError` if the column with specified name doesn't exist
    public func decode<Column: SQLPrimitiveDecodable>(valueAt column: String, as: Column.Type)
        throws -> Column
    {
        guard let value = self[column] else {
            throw DecodingError.keyNotFound(
                ColumnName(column), .init(codingPath: [], debugDescription: ""))
        }
        guard let decoded = Column.init(fromSQL: value) else {
            throw DecodingError.typeMismatch(
                Column.self,
                .init(
                    codingPath: [],
                    debugDescription: "Tried to decode \(Column.self) from SQLite value \(value)"))
        }
        return decoded
    }

    /// Decode row into a tuple of ``SQLPrimitiveDecodable`` values
    // I think I might've cooked here. I feel so smart 🤓.
    public func decode<each Column: SQLPrimitiveDecodable>() throws -> (repeat each Column) {
        var counter = 0
        return (repeat try decodePackElement(as: (each Column).self, counter: &counter))
    }

    /// Decode row into a `Decodable` value
    public func decode<T: Decodable>() throws -> T {
        try T(from: SQLDecoder(row: self))
    }

    private func decodePackElement<T: SQLPrimitiveDecodable>(as witness: T.Type, counter: inout Int)
        throws -> T
    {
        let res = try self.decode(valueAt: counter, as: witness)
        counter += 1
        return res
    }
}
