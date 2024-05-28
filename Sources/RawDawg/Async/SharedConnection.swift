import Dispatch
import Logging

#if canImport(SQLite3)
    import SQLite3
#else
    import CSQLite
#endif

/// A description of how to extract the rowid from the result set.
///
/// ~~Incremental reads (or streaming) of blobs requires a special calls into `sqlite3_blob_open`.
/// Unlike `sqlite3_column_*` functions, `sqlite3_blob_open` cannot be extracted from a prepared
/// statement. As such, it requires the rowid and the column index to be known. When specified in
/// ``Database/prepare(_:persistent:rowid:)``, the library gains an ability to do incremental IO
/// of blobs. This is useful for large blobs that don't fit into memory.~~
///
/// Originaly there was the code below. But I realized there is no convenient way to provide
/// incremental I/O of blobs on top of the prepared statements. As such incremental I/O is probably
/// better handled by something like `Database/blobStream(table:column:rowid:readOnly:)` method.
///
/// ```swift
/// public enum RowIDColumnSelector: Sendable {
///     case none
///     case column(named: String)
///     case column(indexed: Int32)
///
///     static let id: Self = .column(named: "id")
///     static let rowid: Self = .column(named: "rowid")
/// }
/// ```

/// The result of an insert/update operation, achieved from ``PreparedStatement/run()``.
///
/// ```swift
/// let stats = try db.prepare("INSERT INTO table (column) VALUES (1), (2)").run()
/// print("Last inserted rowid: \(stats.lastInsertedRowid)")
/// print("Rows inserted: \(stats.rowsAffected)")
/// ```
public struct InsertionStats: Equatable, Hashable, Sendable {
    /// The rowid of the last inserted row.
    public var lastInsertedRowid: sqlite3_int64
    /// The number of rows affected by the last operation.
    ///
    /// This can be either the number of rows inserted, updated or deleted.
    public var rowsAffected: sqlite3_int64
    /// The total number of rows affected by the last operation, including foregin key cascades.
    public var totalRowsAffected: sqlite3_int64
}

/// An asyncronous sqlite3 database connection.
///
/// Encapsulates the coordination and the synchronization of the sqlite3 database connection.
/// Managed database connection is itself is not thread-safe; actor isolation makes it safe to
/// access the database APIs from any thread.
///
/// The primary way to interact with the database is through the ``Database/prepare(_:)`` method.
/// ```swift
/// let db = try Database(filename: "file.sqlite")
/// let statement = try db.prepare("SELECT * FROM table")
/// let rows = try statement.fetchAll()
/// ```
///
/// Dropping all the references to the Database actor will close the database connection. Errors
/// encountered during the closing of the database connection are logged, but ultimately ignored.
///
/// ## Thread-safety and Concurrency
/// `Database` type itself is safe to use across threads, the access is synchronized via swift actor
/// isolation model. Does not provide concurrency. Operations are serialized, but not necessarily
/// executed in order as swift actor model does not guarantee the order of execution.
public actor SharedConnection {
    fileprivate let unsafeConn: UnmanagedSyncConnection
    
    /// Opens a database connection to the file at the given path. With the given read/write mode.
    ///
    /// The default read/write mode is `.readWrite(create: true)`, which creates the database file
    /// if it doesn't already exist.
    ///
    /// On non-Apple platforms, the sqlite3 library is statically linked into the binary. As such,
    /// call to ``init(filename:mode:)`` will initialize the sqlite3 library. On Apple platforms,
    /// sqlite3 library is dynamically linked and is initialized by the system.
    ///
    /// - Parameters:
    ///   - filename: The path to the database file. Can `":memory:"` for an in-memory database.
    ///   - mode: The read/write mode of the database connection. Defaults to `.readWrite(create: true)`.
    ///
    /// - Throws: An error if the database connection cannot be opened.
    // MAINTAINER NOTE: Database.init is called synchronously from the thread that creates the actor.
    //                  this might be an issue as the underlying connection is not thread-safe.
    //                  It should be ok, since there is no concurrent access to the actor state anyway.
    public init(filename: String, mode: OpenMode = .readWrite) throws {
        self.unsafeConn = try .init(filename: filename, mode: mode)
    }

    /// Prepares a SQL query for execution.
    ///
    /// This is the primary way to interact with the database. The returned ``PreparedStatement`` can
    /// be used to execute the query and fetch the results. The query can contain interpolated values
    /// that will be bound to the statement. It is safe to interpolate values into the query string.
    ///
    /// For more interpolation information, see ``BoundQuery``.
    /// 
    /// ```swift
    /// let db = try Database(filename: "file.sqlite")
    ///
    /// func fetchAllUsers(olderThan age: Int) async throws -> [User] {
    ///     try await db.prepare("select * from users where age > \(age)").fetchAll()
    /// }
    /// 
    /// try await fetchAllUsers(olderThan: 18)
    /// try await fetchAllUsers(olderThan: 21)
    /// ```
    // MAINTAINER NOTE: This also does the collection of column names, which might not be neccessary 
    //                  for all cases. Alternative solutions would be to:
    //                  - Defer column name retrieval until the results are needed. 
    //                  - Provide `fetch*` APIs for tuples directly on db instance which 
    //                    skips column name resolution.
    //                  - Provide `unnamed*` set of APIs for that casa.
    //                  - Don't re-allocate C strings into swift ones from calls to `sqlite3_column_name`.
    public func prepare(_ query: BoundQuery) throws -> PreparedStatement {
        let (ptr, columns) = try unsafeConn.prepare(query)
        return PreparedStatement(conn: self, stmt: ptr, columnNames: columns)
    }

    /// Executes a series of SQL statements separated by semicolons.
    ///
    /// NOTE: This method is **not safe to use with untrusted input**. It is recommended to use
    /// ``Database/prepare(_:)`` for executing queries with interpolated values.
    public func execute(_ query: String) throws {
        try unsafeConn.execute(query)
    }

    internal func finalize(statement: PreparedStatementPtr) throws {
        try unsafeConn.finalize(statement: statement)
    }

    internal func step(statement: PreparedStatementPtr, columnCount: Int) throws -> [SQLiteValue]? {
        try unsafeConn.step(statement: statement, columnCount: columnCount)
    }

    internal func fetchAll(statement: PreparedStatementPtr, columnCount: Int) throws
        -> [SQLiteValue]
    {
        try unsafeConn.fetchAll(statement: statement, columnCount: columnCount)
    }

    internal func run(statement: PreparedStatementPtr) throws -> InsertionStats {
        try unsafeConn.run(statement: statement)
    }

    deinit {
        // This might be hella unsafe as the thread on which deinit is called synchronously 
        // is not neccessarily the one that opened and maanges the connection. Don't know. 
        // Solving this would require doing own DispatchQueue synchronisation. I don't want it. 
        // I'd like to stay in the realm of swift actors. Or this might be a non-issue, since 
        // the is no concurrent/parallel access to the actor/database state.
        do {
            try unsafeConn.close()
        } catch {
            log.error("Database close failed. \(error)")
        }
    }
}
