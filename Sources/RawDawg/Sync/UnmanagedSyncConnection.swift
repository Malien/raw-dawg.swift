#if canImport(SQLite3)
import SQLite3
#else
import CSQLite
#endif

@available(*, unavailable)
extension UnmanagedSyncConnection: Sendable {}

internal struct UnmanagedSyncConnection {
    private let db: OpaquePointer

    init(filename: String, mode: OpenMode = .readWrite) throws {
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
                throw SQLiteError.openDatabase(code: initResult, message: "Failed to initialize SQLite", filename: filename, mode: mode)
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

    /// ## SAFETY
    ///  returned ``SyncPreparedStatement`` must not be alive past the call to ``close()``
    func prepare(_ query: BoundQuery) throws -> (ptr: PreparedStatementPtr, columns: [CString]) {
        var stmt: OpaquePointer? = nil
        try throwing(context: .prepareStatement(query: query)) {
            sqlite3_prepare_v3(
                db: self.db,
                zSql: query.queryString,
                nByte: Int32(query.queryString.utf8.count),
                prepFlags: 0,
                ppStmt: &stmt,
                pzTail: nil
            )
        }
        guard let stmt = stmt else {
            throw SQLiteError.emptyQuery(query: query)
        }
        let columnCount = sqlite3_column_count(stmt)
        let columnNames = (0..<columnCount).map {
            if let cStr = sqlite3_column_name(stmt, $0) {
                CString(ptr: cStr)
            } else {
                fatalError("Couldn't allocate memory for the column name")
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
            metadata: ["query": "\(query.queryString)", "bindings": "\(query.bindings)"])
        return (PreparedStatementPtr(ptr: stmt), columnNames)
    }

    func execute(_ query: String) throws {
        try throwing {
            sqlite3_exec(db: self.db, sql: query, callback: nil, callbackArgument: nil, errmsg: nil)
        }
    }
    
    consuming func close() throws {
        let ptr = self.db
        try throwing {
            sqlite3_close(ptr)
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

    @discardableResult
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
    
    internal func begin(kind: Transaction.Kind) throws {
        let query = "begin \(kind.rawValue)"
        var stmt: OpaquePointer? = nil
        try throwing(context: .begin(kind: kind)) {
            sqlite3_prepare_v3(
                db: self.db,
                zSql: query,
                nByte: Int32(query.utf8.count),
                prepFlags: 0,
                ppStmt: &stmt,
                pzTail: nil
            )
        }
    }
}
