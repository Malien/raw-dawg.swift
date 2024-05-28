import Algorithms
import Foundation

#if canImport(SQLite3)
    import SQLite3
#else
    import CSQLite
#endif

/// Prepared statement returned from the ``Database/prepare(_:)`` function.
///
/// Holds and manages the underlying `sqlite3_stmt` pointer. It is a non-copyable type,
/// and upon deinitialization, it will automatically destroy any resources associated with it.
///
/// The ``PreparedStatement/step()-76bxz`` is the low-level way to incrementally fetch rows from the database.
/// Alternatively, there are higher-level methods like ``PreparedStatement/fetchAll()-7rh3m``
/// and ``PreparedStatement/run()``
///
/// Higher-level APIs are marked as `consuming` and as such, they will automatically finalize the statement
/// upon completion. ``PreparedStatement/step()-76bxz`` on the other hand will not do so. Even though the 
/// `PreparedStatement` type is non-copyable, and will cleanup after itself, it is still recommended to
/// call ``PreparedStatement/finalize()`` explicitly, since the deinitialization is done asynchronously and
/// any errors that might occur during the finalization will be logged, but are ultimately ignored.
///
/// All of the methods run on the ``Database`` actor that created the prepared statement, and thus two
/// are tied together.
///
/// ## Topics
/// - ``run()``
/// ### Incrementally stepping through rows
/// - ``step()-76bxz``
/// - ``step()-92san``
/// - ``step()-3wy2j``
/// ### Fetching all rows
/// - ``fetchAll()-7rh3m``
/// - ``fetchAll()-6jov4``
/// - ``fetchAll()-3h0eg``
/// ### Fetching a single row
/// - ``fetchOne()-o1ui``
/// - ``fetchOne()-8yva9``
/// - ``fetchOne()-4grfr``
/// ### Fetching an optional row
/// - ``fetchOptional()-30gfy``
/// - ``fetchOptional()-92nz3``
/// - ``fetchOptional()-1sp53``
public struct PreparedStatement: ~Copyable, Sendable {
    private let conn: SharedConnection
    private let stmt: PreparedStatementPtr
    /// I don't really understand swift's restriction on `discard self` being restricted to trivially destructible types.
    /// Since we can't use `discard self`, here's the flag, that `.finalize()` was called explicitly before.
    private var finalized = false
    /// Whether or not we've exhausted all of the rows via the `.step()` function.
    /// As per sqlite docs. After `SQLITE_DONE` is yielded from `sqlite3_step`, further calls to step are prohibited
    /// (given that `sqlite3_reset` isn't called before that)
    private var finished = false
    /// ~~C Strings are valid until the call to `sqlite3_finalize`~~
    /// These are Swift string now. Just to avoid conversions on every `.step()` call
    private let columnNames: [String]

    internal init(conn: SharedConnection, stmt: PreparedStatementPtr, columnNames: [CString]) {
        self.conn = conn
        self.stmt = stmt
        // We might not need to copy names into swift strings in the future, for now, we do
        self.columnNames = columnNames.map { String(cString: $0.ptr) }
    }

    /// Cleans up the sqlite3 resources associated with the prepared statement.
    ///
    /// This method is called automatically upon deinitialization of the `PreparedStatement` instance.
    /// However, it is recommended to call it explicitly, as the deinitialization is done asynchronously,
    /// and any errors that might occur during the finalization will be logged, but are ultimately ignored.
    ///
    /// Higher-level APIs like ``fetchAll()-7rh3m``, ``fetchOne()-o1ui``, ``fetchOptional()-30gfy`` and ``run()`` 
    /// will automatically finilize the statement upon completion, and surface any errors that might occur.
    public consuming func finalize() async throws {
        self.finalized = true
        try await self.conn.finalize(statement: self.stmt)
    }
    
    /// Run the statement, for when the query result doesn't matter.
    ///
    /// This is THE method to use for insert/update statements that don't return any relevant information (aka. don't have `returning` clauses)
    ///
    /// ```swift
    /// func logNewSale(ofProductWithID id: Int, amount: Int, price: Int) async throws {
    ///     try await db
    ///         .prepare("insert into sales (product_id, amount, price) values (\(id), \(amount), \(price))")
    ///         .run()
    /// }
    /// ```
    /// - Returns: What was the last inserted rowid and how many rows were modified by the statement
    @discardableResult
    public consuming func run() async throws -> InsertionStats {
        try await finalizeAfter { statement in
            try await statement.conn.run(statement: statement.stmt)
        }
    }

    // MARK: Step overloads

    /// Poll-style of fetching results, that yields a single ``Row`` at a time.
    ///
    /// There are also overloads that allow you to decode the [row into a tuple of columns](doc:RawDawg/PreparedStatement/step()-92san),
    /// or [a single `Decodable` type](doc:RawDawg/PreparedStatement/step()-3wy2j) insteaf of raw ``Row``.
    ///
    /// This is kind of like an iterator, but async, and might error out. When the statement is exhausted,
    /// it will return `nil`. Otherwise, it will return the next row. You may not call `step` again after
    /// it has returned `nil`. ALthough there is a check, that will continue yielding `nil`s if you do.
    /// this might be changed in the future, so don't rely on that behavior.
    public mutating func step() async throws -> Row? {
        if finished {
            return nil
        }
        if let values = try await self.conn.step(
            statement: self.stmt, columnCount: self.columnNames.count)
        {
            return Row(columns: columnNames, values: values[...])
        } else {
            finished = true
            return nil
        }
    }

    /// Poll-style of fetching results, that yields a single tuple of ``SQLPrimitiveDecodable`` values (a row) at a time.
    ///
    /// This is a wrapper on top of the ``step()-76bxz`` method, that decodes the ``Row`` into a tuple of columns.
    /// It is a variadic method that is restricted to tuples of length 2 or more. Tuples of length 0 (aka. `()`)
    /// are not allowed, ~~as there is no way to craft a query that yields no columns~~ Insert/update statements without returning clause
    /// (which yield 0 values) are better served by ``run()`` method. Tuples of length 1 are not allowed
    /// as they conflict with the ``step()-3wy2j`` overload that decodes a single `Decodable` type for types that are both
    /// `Decodable` and ``SQLPrimitiveDecodable`` (_which is the all of them_).
    ///
    /// If you instead want to decode a row into a `Decodable`, [there is an overload just for that](doc:RawDawg/PreparedStatement/step()-3wy2j)
    ///
    /// This is kind of like an iterator, but async, and might error out. When the statement is exhausted,
    /// it will return `nil`. Otherwise, it will return the next row. You may not call `step` again after
    /// it has returned `nil`. ALthough there is a check, that will continue yielding `nil`s if you do.
    /// this might be changed in the future, so don't rely on that behavior.
    public mutating func step<Column1, Column2, each Column>() async throws -> (
        Column1, Column2, repeat each Column
    )?
    where
        Column1: SQLPrimitiveDecodable, Column2: SQLPrimitiveDecodable,
        repeat each Column: SQLPrimitiveDecodable
    {
        try guardColumnCount(expected: 2 + packLength(repeat (each Column).self))
        let row: Row? = try await self.step()
        return try row.map { try $0.decode() }
    }

    /// Poll-style of fetching results, that yields a single `Decodable` value (row) at a time.
    ///
    /// This is a wrapper on top of the ``step()-76bxz`` method, that decodes the row into a single `Decodable` type.
    /// Three handled cases are:
    /// - A single column is expected, and the type is ``SQLPrimitiveDecodable``, it will be decoded as such.
    /// - A single column is expected, and the type uses `Decoder/singleValueContainer` to decode itself.
    /// - Multiple columns are expected, and the type uses `Decoder/container(keyedBy:)` to decode itself.
    /// Any other case (aka. `Decoder/unkeyedContainer` and `Decoder/singleValueContainer` on multiple columns)
    /// is not supported and will throw a runtime error.
    ///
    /// If you don't want to create a `Codable` struct just to have a type-safe decodes of row values, there is [an overload
    /// that is just right](doc:RawDawg/PreparedStatement/step()-76bxz) for those ad-hoc style of queries
    ///
    /// This is kind of like an iterator, but async, and might error out. When the statement is exhausted,
    /// it will return `nil`. Otherwise, it will return the next row. You may not call `step` again after
    /// it has returned `nil`. ALthough there is a check, that will continue yielding `nil`s if you do.
    /// this might be changed in the future, so don't rely on that behavior.
    public mutating func step<T: Decodable>() async throws -> T? {
        if let type = T.self as? SQLPrimitiveDecodable.Type {
            try guardColumnCount(expected: 1)
            return try await step().map { try $0.decode(valueAt: 0, as: type) as! T }
        }
        return try await step().map { try $0.decode() }
    }

    // MARK: Fetch All overloads
    
    /// Collect all of the rows, statment yields, into an array of ``Row`` values
    ///
    /// There are alternative overloads that [decode rows into typed tuples](doc:RawDawg/PreparedStatement/fetchAll()-6jov4)
    /// or [use the `Decodable` protocol](doc:RawDawg/PreparedStatement/fetchAll()-3h0eg), rather than dealing in raw ``Row`` objects
    ///
    /// This action will consume and finalize the statement, yielding any errors that rise from doing so back to the caller
    public consuming func fetchAll() async throws -> [Row] {
        let columns = self.columnNames
        return try await finalizeAfter { statement in
            try await statement.conn.fetchAll(
                statement: statement.stmt, columnCount: statement.columnNames.count
            )
            .chunks(ofCount: columns.count).map {
                Row(columns: columns, values: $0)
            }
        }
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
    public consuming func fetchAll<Column1, Column2, each Column>() async throws -> [(
        Column1, Column2, repeat each Column
    )]
    where
        Column1: SQLPrimitiveDecodable, Column2: SQLPrimitiveDecodable,
        repeat each Column: SQLPrimitiveDecodable
    {
        try guardColumnCount(expected: 2 + packLength(repeat (each Column).self))
        let row: [Row] = try await self.fetchAll()
        return try row.map { try $0.decode() }
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
    public consuming func fetchAll<T: Decodable>() async throws -> [T] {
        if let type = T.self as? SQLPrimitiveDecodable.Type {
            try guardColumnCount(expected: 1)
            return try await fetchAll().map { try $0.decode(valueAt: 0, as: type) as! T }
        }
        return try await fetchAll().map { try $0.decode() }
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
    public consuming func fetchOne() async throws -> Row {
        try await finalizeAfter { statement in
            guard let row = try await statement.step() else {
                throw SQLiteError.noRowsFetched
            }
            return row
        }
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
    public consuming func fetchOne<Column1, each Column>() async throws -> (
        Column1, repeat each Column
    )
    where Column1: SQLPrimitiveDecodable, repeat each Column: SQLPrimitiveDecodable {
        try guardColumnCount(expected: 1 + packLength(repeat (each Column).self))
        let row: Row = try await self.fetchOne()
        return try row.decode()
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
    public consuming func fetchOne<T: Decodable>() async throws -> T {
        if let type = T.self as? SQLPrimitiveDecodable.Type {
            try guardColumnCount(expected: 1)
            return try await fetchOne().decode(valueAt: 0, as: type) as! T
        }
        return try await fetchOne().decode()
    }
    
    // MARK: Fetch Optional overloads
    
    /// Return the first ``Row`` or `nil` if the statement yields 0 rows
    ///
    /// There are alternative overloads that [decode rows into typed tuples](doc:RawDawg/PreparedStatement/fetchOptional()-92nz3)
    /// or [use the `Decodable` protocol](doc:RawDawg/PreparedStatement/fetchOptional()-1sp53), rather than dealing in raw ``Row`` objects
    ///
    /// This action will consume and finalize the statement, yielding any errors that rise from doing so back to the caller
    public consuming func fetchOptional() async throws -> Row? {
        try await finalizeAfter { statement in
            try await statement.step()
        }
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
    public consuming func fetchOptional<Column1, Column2, each Column>() async throws -> (
        Column1, Column2, repeat each Column
    )?
    where
        Column1: SQLPrimitiveDecodable, Column2: SQLPrimitiveDecodable,
        repeat each Column: SQLPrimitiveDecodable
    {
        try guardColumnCount(expected: 2 + packLength(repeat (each Column).self))
        let row: Row? = try await self.fetchOptional()
        return try row.map { try $0.decode() }
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
    public consuming func fetchOptional<T: Decodable>() async throws -> T? {
        if let type = T.self as? SQLPrimitiveDecodable.Type {
            try guardColumnCount(expected: 1)
            return try await fetchOptional().map { try $0.decode(valueAt: 0, as: type) as! T }
        }
        return try await fetchOptional().map { try $0.decode() }
    }

    //     I'm not comfortable with this API
    //    /// Push-style of fetching results
    //    public consuming func stream() -> AsyncThrowingStream<Row, any Error>
    //    public consuming func stream<T: Decodable>() -> some AsyncSequence
    
    // MARK: Private helpers
    
    private func guardColumnCount(expected: Int) throws {
        guard columnNames.count == expected else {
            throw SQLiteError.columnCountMissmatch(expected: expected, got: columnNames.count)
        }
    }
    
    /// Execute the block, and finalize the statement even if the block throws.
    ///
    /// This exists only because there are no async defer blocks in swift 😡. Yet.
    private consuming func finalizeAfter<T>(action: (inout Self) async throws -> T) async throws
        -> T
    {
        // There is no async defer blocks, so this mess is emulating it (or the finally block)
        var result: T
        do {
            result = try await action(&self)
        } catch let e {
            try await finalize()
            throw e
        }
        try await finalize()
        return result
    }

    deinit {
        if self.finalized { return }

        let stmnt = self.stmt
        let db = self.conn

        // Requirement on Task is the only thing preventing this from building on macOS < 10.15
        Task {
            do {
                try await db.finalize(statement: stmnt)
            } catch let error {
                log.error("Couldn't finalize prepared statement: \(error)")
            }
        }
    }
}
