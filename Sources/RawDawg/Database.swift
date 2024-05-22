import Dispatch
import Logging

#if canImport(SQLite3)
    import SQLite3
#else
    import CSQLite
#endif

// C functions, but with swift's named parameters
private func sqlite3_open_v2(
    filename: String, ppDb: UnsafeMutablePointer<OpaquePointer?>?, flags: Int32,
    zVfs: UnsafePointer<Int8>?
) -> Int32 {
    return sqlite3_open_v2(filename, ppDb, flags, zVfs)
}

@available(macOS 10.14, *)
private func sqlite3_prepare_v3(
    db: OpaquePointer!, zSql: UnsafePointer<CChar>!, nByte: Int32, prepFlags: UInt32,
    ppStmt: UnsafeMutablePointer<OpaquePointer?>!,
    pzTail: UnsafeMutablePointer<UnsafePointer<CChar>?>!
) -> Int32 {
    return sqlite3_prepare_v3(db, zSql, nByte, prepFlags, ppStmt, pzTail)
}

private func sqlite3_prepare_v2(
    db: OpaquePointer!, zSql: UnsafePointer<CChar>!, nByte: Int32,
    ppStmt: UnsafeMutablePointer<OpaquePointer?>!,
    pzTail: UnsafeMutablePointer<UnsafePointer<CChar>?>!
) -> Int32 {
    return sqlite3_prepare_v2(db, zSql, nByte, ppStmt, pzTail)
}

private func sqlite3_exec(
    db: OpaquePointer!,
    sql: UnsafePointer<CChar>!,
    callback: sqlite3_callback!,
    callbackArgument: UnsafeMutableRawPointer!,
    errmsg: UnsafeMutablePointer<UnsafeMutablePointer<CChar>?>!
) -> Int32 {
    return sqlite3_exec(db, sql, callback, callbackArgument, errmsg)
}

private func sqlite3_bind_text64(
    statement: OpaquePointer!,
    position: Int32,
    text: UnsafePointer<CChar>!,
    byteSize: sqlite3_uint64,
    destructor: sqlite3_destructor_type!,
    encoding: UInt8
) -> Int32 {
    sqlite3_bind_text64(statement, position, text, byteSize, destructor, encoding)
}

private func sqlite3_bind_blob64(
    statement: OpaquePointer!,
    position: Int32,
    bytes: UnsafeRawPointer!,
    size: sqlite3_uint64,
    destructor: sqlite3_destructor_type!
) -> Int32 {
    sqlite3_bind_blob64(statement, position, bytes, size, destructor)
}

// Theese constants are not imported by swift from C headers, since they do unsafe function pointer casting
// DO NOT CALL THOSE! THEY ARE NOT VALID POINTERS
private let SQLITE_STATIC = unsafeBitCast(0, to: sqlite3_destructor_type.self)
private let SQLITE_TRANSIENT = unsafeBitCast(-1, to: sqlite3_destructor_type.self)

internal let log = Logger(label: "io.github.malien.raw-dawg")

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
public actor Database {
    fileprivate let db: OpaquePointer
    
    /// Read/write mode of the database connection to be opened by ``Database/init(filename:mode:)``.
    ///
    /// For convenience, the static property ``OpenMode/readWrite`` is provided, which is equivalent to
    /// `.readWrite(create:)`.
    ///
    /// ```swift
    /// let db = try Database(filename: "file.sqlite", mode: .readOnly)
    /// try await db.execute("create table table (column integer)") // This will fail
    /// ```
    public enum OpenMode: Sendable, Equatable, Hashable {
        case readOnly
        /// - Parameter create: Whether to create the database file if it doesn't already exist.
        case readWrite(create: Bool)
        /// An alias for `.readWrite(create: true)`.
        public static var readWrite = Self.readWrite(create: true)
    }

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
        // Apple's SQLite3 by the looks of things is either compiled without SQLITE_OMIT_AUTOINIT or
        // is doing initialization by itself (likely as a dynamically linked library constructor).
        // On Apple platforms we just link to /usr/lib/libsqlite3.dylib, otherwise we statically compile
        // sqlite3's amalgamation into the binary (via CSQLite swift package)
        // CSQLite is explicitly compiled with SQLITE_OMIT_AUTOINIT as per sqlite's recommendation
        // as such we need to call `sqlite3_initialize`, at least once. Any subsequent call to
        // `sqlite3_initialize` is a no-op
        #if !canImport(SQLite3)
            let initResult = sqlite3_initialize()
            if initResult != SQLITE_OK {
                throw SQLiteError(code: initResult, message: "Failed to initialize SQLite")
            }
        #endif
        var db: OpaquePointer? = nil
        var flags: Int32 = 0
        switch mode {
        case .readOnly:
            flags |= SQLITE_OPEN_READONLY
        case .readWrite(create: false):
            flags |= SQLITE_OPEN_READWRITE
        case .readWrite(create: true):
            flags |= SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE
        }
        if sqlite3_threadsafe() != 0 {
            flags |= SQLITE_OPEN_NOMUTEX
        }

        let res = sqlite3_open_v2(filename: filename, ppDb: &db, flags: flags, zVfs: nil)
        guard let db = db else {
            preconditionFailure(
                "Cannot open sqlite databse, since sqlite wasn't able to allocate memory (how exactly?)"
            )
        }

        if let error = Self.error(
            unsafelyDescribedBy: db, unlessOK: res,
            context: .openDatabase(filename: filename, mode: mode))
        {
            let res = sqlite3_close_v2(db)
            if let closeError = Self.error(
                unsafelyDescribedBy: db, unlessOK: res,
                context: .openDatabase(filename: filename, mode: mode))
            {
                log.error(
                    "Database open failed, during which closing the database also failed. \(closeError)"
                )
            }
            throw error
        }

        self.db = db
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
        var stmt: OpaquePointer? = nil
        try throwing(context: .prepareStatement(query: query)) {
            sqlite3_prepare_v3(
                db: self.db,
                zSql: query.query,
                nByte: Int32(query.query.utf8.count),
                prepFlags: 0,
                ppStmt: &stmt,
                pzTail: nil
            )
        }
        if stmt == nil {
            throw SQLiteError.emptyQuery(query: query)
        }
        let columnCount = sqlite3_column_count(stmt)
        let columnNames = (0..<columnCount).map {
            if let cStr = sqlite3_column_name(stmt, $0) {
                CString(ptr: cStr)
            } else {
                fatalError("Couldn't allocate column name")
            }
        }
        let bindingCount = sqlite3_bind_parameter_count(stmt)
        guard bindingCount == query.bindings.count else {
            throw SQLiteError.bindingMissmatch(
                query: query, expected: bindingCount, got: query.bindings.count)
        }
        for (position, binding) in zip(Int32(1)..., query.bindings) {
            try throwing {
                switch binding {
                case .null:
                    sqlite3_bind_null(stmt, position)
                case .integer(let int):
                    sqlite3_bind_int64(stmt, position, int)
                case .float(let float):
                    sqlite3_bind_double(stmt, position, float)
                case .text(let string):
                    sqlite3_bind_text64(
                        statement: stmt, position: position, text: string,
                        byteSize: sqlite3_uint64(string.utf8.count), destructor: SQLITE_TRANSIENT,
                        encoding: UInt8(SQLITE_UTF8))
                case .blob(.loaded(let data)):
                    data.withUnsafeBytes { buffer in
                        sqlite3_bind_blob64(
                            statement: stmt, position: position, bytes: buffer.baseAddress,
                            size: sqlite3_uint64(buffer.count), destructor: SQLITE_TRANSIENT)
                    }
                case .blob(.empty):
                    sqlite3_bind_blob64(
                        statement: stmt, position: position, bytes: nil, size: 0,
                        destructor: SQLITE_STATIC)
                }
            }
        }
        log.trace(
            "Prepared SQL statement",
            metadata: ["query": "\(query.query)", "bindings": "\(query.bindings)"])
        return PreparedStatement(
            db: self,
            stmt: PreparedStatementPtr(ptr: stmt!),
            columnNames: columnNames
        )
    }

    /// Executes a series of SQL statements separated by semicolons.
    ///
    /// NOTE: This method is **not safe to use with untrusted input**. It is recommended to use
    /// ``Database/prepare(_:)`` for executing queries with interpolated values.
    public func execute(_ query: String) throws {
        try throwing {
            sqlite3_exec(db: self.db, sql: query, callback: nil, callbackArgument: nil, errmsg: nil)
        }
    }

    private func lastError(withCode code: Int32, context: SQLiteError.Context = .unknown) -> SQLiteError {
        let message =
            if let cMsgStr = sqlite3_errmsg(self.db) {
                String(cString: cMsgStr)
            } else {
                "No error message available"
            }
        return SQLiteError(sqliteErrorCode: code, message: message, context: context)
    }

    private func error(unlessOK resultCode: Int32, context: SQLiteError.Context = .unknown) -> SQLiteError?
    {
        if resultCode != SQLITE_OK {
            return self.lastError(withCode: resultCode, context: context)
        } else {
            return nil
        }
    }

    // This constructor is safe to call only from the thread (actor) that manages the database connection
    private static func error(
        unsafelyDescribedBy db: OpaquePointer, code: Int32, context: SQLiteError.Context = .unknown
    ) -> SQLiteError {
        let message =
            if let cMsgStr = sqlite3_errmsg(db) {
                String(cString: cMsgStr)
            } else {
                "No error message available"
            }
        return SQLiteError(sqliteErrorCode: code, message: message, context: context)
    }

    // This constructor is safe to call only from the thread (actor) that manages the database connection
    private static func error(
        unsafelyDescribedBy db: OpaquePointer, unlessOK resultCode: Int32,
        context: SQLiteError.Context = .unknown
    )
        -> SQLiteError?
    {
        if resultCode != SQLITE_OK {
            return error(unsafelyDescribedBy: db, code: resultCode, context: context)
        } else {
            return nil
        }
    }


    private func throwing(context: SQLiteError.Context = .unknown, _ action: () -> Int32) throws {
        if let error = self.error(unlessOK: action(), context: context) {
            throw error
        }
    }

    internal func finalize(statement: PreparedStatementPtr) throws {
        try throwing {
            sqlite3_finalize(statement.ptr)
        }
    }

    internal func step(statement: PreparedStatementPtr, columnCount: Int) throws -> [SQLiteValue]? {
        let res = sqlite3_step(statement.ptr)
        switch res {
        case SQLITE_DONE:
            return nil
        case SQLITE_ROW:
            return try (0..<columnCount).map {
                try self.parseValue(in: statement, at: Int32($0))
            }
        case SQLITE_BUSY:
            fallthrough
        default:
            throw self.lastError(withCode: res)
        }
    }

    private func parseValue(in statement: PreparedStatementPtr, at columnIndex: Int32) throws
        -> SQLiteValue
    {
        switch sqlite3_column_type(statement.ptr, columnIndex) {
        case SQLITE_NULL:
            return .null
        case SQLITE_INTEGER:
            return .integer(sqlite3_column_int64(statement.ptr, columnIndex))
        case SQLITE_FLOAT:
            return .float(sqlite3_column_double(statement.ptr, columnIndex))
        case SQLITE3_TEXT:
            return .text(
                String(cString: sqlite3_column_text(statement.ptr, columnIndex))
            )
        case SQLITE_BLOB:
            let size = sqlite3_column_bytes(statement.ptr, columnIndex)
            let blobPtr = sqlite3_column_blob(statement.ptr, columnIndex)
            return if let blobPtr = blobPtr {
                .blob(SQLiteBlob.init(bytes: blobPtr, count: Int(size)))
            } else {
                .blob(.empty)
            }

        default:
            fatalError("Unreachable. SQLite doesn't support data type for column \(columnIndex)")
        }
    }

    internal func fetchAll(statement: PreparedStatementPtr, columnCount: Int) throws
        -> [SQLiteValue]
    {
        var result = [SQLiteValue]()
        while let row = try step(statement: statement, columnCount: columnCount) {
            result.append(contentsOf: row)
        }
        return result
    }

    internal func run(statement: PreparedStatementPtr) throws -> InsertionStats {
        let res = sqlite3_step(statement.ptr)
        switch res {
        case SQLITE_DONE, SQLITE_ROW:
            let rowsAffected =
                if #available(macOS 12.3, iOS 15.4, *) {
                    sqlite3_changes64(db)
                } else {
                    Int64(sqlite3_changes(db))
                }
            let totalRowsAffected =
                if #available(macOS 12.3, iOS 15.4, *) {
                    sqlite3_total_changes64(db)
                } else {
                    Int64(sqlite3_changes(db))
                }

            return InsertionStats(
                lastInsertedRowid: sqlite3_last_insert_rowid(db),
                rowsAffected: rowsAffected,
                totalRowsAffected: totalRowsAffected
            )
        case SQLITE_BUSY:
            fallthrough
        default:
            throw self.lastError(withCode: res)
        }
    }

    deinit {
        // This might be hella unsafe as the thread on which deinit is called synchronously 
        // is not neccessarily the one that opened and maanges the connection. Don't know. 
        // Solving this would require doing own DispatchQueue synchronisation. I don't want it. 
        // I'd like to stay in the realm of swift actors. Or this might be a non-issue, since 
        // the is no concurrent/parallel access to the actor/database state.
        if let error = Self.error(unsafelyDescribedBy: db, unlessOK: sqlite3_close(db)) {
            log.error("Database close failed. \(error)")
        }
    }
}
