/// It is EXPLICITLY unsendable!
@available(*, unavailable)
extension SyncConnection: @unchecked Sendable {}

public struct SyncConnection: ~Copyable {
    private let conn: UnmanagedSyncConnection
    
    init(filename: String, mode: OpenMode = .readWrite) throws {
        self.conn = try .init(filename: filename, mode: mode)
    }

    /// ## SAFETY
    ///  the underlying connection has to be alive and not shared with any other ``SyncConnection`` or ``SharedConnection`` instance
    internal init(unsafeFromUnmanaged conn: UnmanagedSyncConnection) {
        self.conn = conn
    }
    
    /// ## SAFETY
    ///  returned ``SyncPreparedStatement`` must not be alive past the call to ``close()``
    private mutating func unsafePrepare(_ query: BoundQuery) throws -> SyncPreparedStatement {
        let (ptr, columns) = try conn.prepare(query)
        return SyncPreparedStatement(conn: conn, stmt: ptr, columnNames: columns)
    }
    
    /// This is the only safe way to get hands onto the ``SyncPreparedStatement``, since it can only life for the duration of the closure.
    /// The lifetme is bound to the ``SyncConnection`` lifetime (aka. it cannot exceed it). Also you shouldn't leak (don't think you can) the statement.
    @discardableResult
    public mutating func preparing<T>(_ query: BoundQuery, block: (inout SyncPreparedStatement) throws -> T) throws -> T {
        return try unsafePrepare(query).finalizeAfter { stmt in
            return try block(&stmt)
        }
    }
    
    public mutating func execute(_ query: String) throws {
        try conn.execute(query)
    }
    
    @discardableResult
    public mutating func run(_ query: BoundQuery) throws -> InsertionStats {
        try unsafePrepare(query).run()
    }

    /// Collect all of the rows, statment yields, into an array of ``Row`` values
    ///
    /// There are alternative overloads that [decode rows into typed tuples](doc:RawDawg/PreparedStatement/fetchAll()-6jov4)
    /// or [use the `Decodable` protocol](doc:RawDawg/PreparedStatement/fetchAll()-3h0eg), rather than dealing in raw ``Row`` objects
    ///
    /// This action will consume and finalize the statement, yielding any errors that rise from doing so back to the caller
    public mutating func fetchAll(_ query: BoundQuery) throws -> [Row] {
        try unsafePrepare(query).fetchAll()
    }

    /// Collect all of the rows, statement yields, into an array of tuples of ``SQLPrimitiveDecodable`` values.
    ///
    /// This is a wrapper on top of the ``fetchAll()-7rh3m`` method, that decodes the ``Row`` into a tuple of columns.
    /// It is a variadic method that is restricted to tuples of length 2 or more. Tuples of length 0 (aka. `()`)
    /// are not allowed, ~~as there is no way to craft a query that yields no columns~~ Insert/update statements without returning clause
    /// (which yield 0 values) are better served by ``run()`` method. Tuples of length 1 are not allowed
    /// as they conflict with the ``fetchAll()-3h0eg`` overload that decodes a single `Decodable` type for types that are both
    /// `Decodable` and ``SQLPrimitiveDecodable`` (_which is the all of them_).
    ///
    /// If you instead want to decode a row into a `Decodable`, [there is an overload just for that](doc:RawDawg/PreparedStatement/fetchAll()-7rh3m)
    ///
    /// This action will consume and finalize the statement, yielding any errors that rise from doing so back to the caller
    public mutating func fetchAll<Column1, Column2, each Column>(_ query: BoundQuery) throws -> [(
        Column1, Column2, repeat each Column
    )]
    where
        Column1: SQLPrimitiveDecodable, Column2: SQLPrimitiveDecodable,
        repeat each Column: SQLPrimitiveDecodable
    {
        try unsafePrepare(query).fetchAll()
    }

    /// Collect all of the rows, statment yields, into an array of `Decodable` values.
    ///
    /// This is a wrapper on top of the ``fetchAll()-7rh3m`` method, that decodes the row into a single `Decodable` type.
    /// Three handled cases are:
    /// - A single column is expected, and the type is ``SQLPrimitiveDecodable``, it will be decoded as such.
    /// - A single column is expected, and the type uses `Decoder/singleValueContainer` to decode itself.
    /// - Multiple columns are expected, and the type uses `Decoder/container(keyedBy:)` to decode itself.
    /// Any other case (aka. `Decoder/unkeyedContainer` and `Decoder/singleValueContainer` on multiple columns)
    /// is not supported and will throw a runtime error.
    ///
    /// If you don't want to create a `Codable` struct just to have a type-safe decodes of row values, there is [an overload
    /// that is just right](doc:RawDawg/PreparedStatement/fetchAll()-3h0eg) for those ad-hoc style of queries
    ///
    /// This action will consume and finalize the statement, yielding any errors that rise from doing so back to the caller
    public mutating func fetchAll<T: Decodable>(_ query: BoundQuery) throws -> [T] {
        try unsafePrepare(query).fetchAll()
    }
    
    // MARK: Fetch One overloads

    /// Return the first ``Row``. Error out if the statment yields 0 rows
    ///
    /// - Throws: ``SQLiteError/noRowsFetched`` if no rows were selected by the statement
    ///
    /// There are alternative overloads that [decode rows into typed tuples](doc:RawDawg/PreparedStatement/fetchOne()-8yva9)
    /// or [use the `Decodable` protocol](doc:RawDawg/PreparedStatement/fetchOne()-4grfr), rather than dealing in raw ``Row`` objects
    ///
    /// This action will consume and finalize the statement, yielding any errors that rise from doing so back to the caller
    public mutating func fetchOne(_ query: BoundQuery) throws -> Row {
        try unsafePrepare(query).fetchOne()
    }

    /// Return the first row decoded as a tuple of ``SQLPrimitiveDecodable`` values. Error out if the statment yields 0 rows
    ///
    /// - Throws: ``SQLiteError/noRowsFetched`` if no rows were selected by the statement
    ///
    /// This is a wrapper on top of the ``fetchOne()-o1ui`` method, that decodes the ``Row`` into a tuple of columns.
    /// It is a variadic method that is restricted to tuples of length 1 or more. Tuples of length 0 (aka. `()`)
    /// are not allowed, ~~as there is no way to craft a query that yields no columns~~ Insert/update statements without returning clause
    /// (which yield 0 values) are better served by ``run()`` method.
    ///
    /// If you instead want to decode a row into a `Decodable`, [there is an overload just for that](doc:RawDawg/PreparedStatement/fetchOne()-4grfr)
    ///
    /// This action will consume and finalize the statement, yielding any errors that rise from doing so back to the caller
    ///
    /// _Aside: there is no limitation on this overload firing for 1 value, as it doesn't conflict with ``fetchOne()-4grfr``_
    // Curiously there is no second "padding" generic argument for this one as it doesn't seem
    // to conflict with <T: Decodable> overload. Hella curious. Basically any applied wrapper
    // (aka. Optiona<(repeat each Column)> or [(repeat each Column)]) breaks resolution.
    // Still there is one to prevent forming let _: () = statement.fetchOne() from happenning
    public mutating func fetchOne<Column1, each Column>(_ query: BoundQuery) throws -> (
        Column1, repeat each Column
    )
    where Column1: SQLPrimitiveDecodable, repeat each Column: SQLPrimitiveDecodable {
        try unsafePrepare(query).fetchOne()
    }

    /// Return the first row decoded using `Decodable` protocol. Error out if the statement yields 0 rows
    ///
    /// - Throws: ``SQLiteError/noRowsFetched`` if no rows were selected by the statement
    ///
    /// This is a wrapper on top of the ``fetchOne()-o1ui`` method, that decodes the row into a single `Decodable` type.
    /// Three handled cases are:
    /// - A single column is expected, and the type is ``SQLPrimitiveDecodable``, it will be decoded as such.
    /// - A single column is expected, and the type uses `Decoder/singleValueContainer` to decode itself.
    /// - Multiple columns are expected, and the type uses `Decoder/container(keyedBy:)` to decode itself.
    /// Any other case (aka. `Decoder/unkeyedContainer` and `Decoder/singleValueContainer` on multiple columns)
    /// is not supported and will throw a runtime error.
    ///
    /// This action will consume and finalize the statement, yielding any errors that rise from doing so back to the caller
    /// If you don't want to create a `Codable` struct just to have a type-safe decodes of row values, there is [an overload
    /// that is just right](doc:RawDawg/PreparedStatement/fetchOne()-8yva9) for those ad-hoc style of queries
    public mutating func fetchOne<T: Decodable>(_ query: BoundQuery) throws -> T {
        try unsafePrepare(query).fetchOne()
    }
    
    // MARK: Fetch Optional overloads
    
    /// Return the first ``Row`` or `nil` if the statement yields 0 rows
    ///
    /// There are alternative overloads that [decode rows into typed tuples](doc:RawDawg/PreparedStatement/fetchOptional()-92nz3)
    /// or [use the `Decodable` protocol](doc:RawDawg/PreparedStatement/fetchOptional()-1sp53), rather than dealing in raw ``Row`` objects
    ///
    /// This action will consume and finalize the statement, yielding any errors that rise from doing so back to the caller
    public mutating func fetchOptional(_ query: BoundQuery) throws -> Row? {
        try unsafePrepare(query).fetchOptional()
    }

    /// Return the frist row decoded as a tuple of ``SQLPrimitiveDecodable`` values, or `nil` if the statement yields 0 rows
    ///
    /// This is a wrapper on top of the ``fetchOptional()-30gfy`` method, that decodes the ``Row`` into a tuple of columns.
    /// It is a variadic method that is restricted to tuples of length 2 or more. Tuples of length 0 (aka. `()`)
    /// are not allowed, ~~as there is no way to craft a query that yields no columns~~ Insert/update statements without returning clause
    /// (which yield 0 values) are better served by ``run()`` method. Tuples of length 1 are not allowed
    /// as they conflict with the ``fetchOptional()-1sp53`` overload that decodes a single `Decodable` type for types that are both
    /// `Decodable` and ``SQLPrimitiveDecodable`` (_which is the all of them_).
    ///
    /// If you instead want to decode a row into a `Decodable`, [there is an overload just for that](doc:RawDawg/PreparedStatement/fetchOptional()-1sp53)
    ///
    /// This action will consume and finalize the statement, yielding any errors that rise from doing so back to the caller
    public mutating func fetchOptional<Column1, Column2, each Column>(_ query: BoundQuery) throws -> (
        Column1, Column2, repeat each Column
    )?
    where
        Column1: SQLPrimitiveDecodable, Column2: SQLPrimitiveDecodable,
        repeat each Column: SQLPrimitiveDecodable
    {
        try unsafePrepare(query).fetchOptional()
    }

    /// Return the first row decoded using a `Decodable` protocol or `nil` if the statement yields 0 rows
    ///
    /// This is a wrapper on top of the ``fetchOptional()-30gfy`` method, that decodes the row into a single `Decodable` type.
    /// Three handled cases are:
    /// - A single column is expected, and the type is ``SQLPrimitiveDecodable``, it will be decoded as such.
    /// - A single column is expected, and the type uses `Decoder/singleValueContainer` to decode itself.
    /// - Multiple columns are expected, and the type uses `Decoder/container(keyedBy:)` to decode itself.
    /// Any other case (aka. `Decoder/unkeyedContainer` and `Decoder/singleValueContainer` on multiple columns)
    /// is not supported and will throw a runtime error.
    /// If you don't want to create a `Codable` struct just to have a type-safe decodes of row values, there is [an overload
    /// that is just right](doc:RawDawg/PreparedStatement/fetchOptional()-92nz3) for those ad-hoc style of queries
    ///
    /// This action will consume and finalize the statement, yielding any errors that rise from doing so back to the caller
    public mutating func fetchOptional<T: Decodable>(_ query: BoundQuery) throws -> T? {
        try unsafePrepare(query).fetchOptional()
    }
    
    public mutating func transaction<T>(_ kind: Transaction.Kind = .deferred, block: (inout Transaction) throws -> T) throws -> T {
        try self.run("begin \(raw: kind.rawValue)")
        var tx = Transaction(conn: self.conn)
        let result: T
        do {
            result = try block(&tx)
        } catch {
            try tx.rollback()
            throw error
        }
        try tx.commit()
        return result
    }

    public consuming func close() throws {
        let db = self.conn
        discard self
        try db.close()
    }

    /// Release the underlying unmanaged connection without closing it.
    ///
    /// ## SAFETY
    /// Please don't share the underlying connection. It is not safe to do so.
    internal consuming func unsafeReleaseUnmanaged() -> UnmanagedSyncConnection {
        let db = self.conn
        discard self
        return db
    }
    
    deinit {
        do {
            try conn.close()
        } catch {
            log.error("Database close failed. \(error)")
        }
    }
}

