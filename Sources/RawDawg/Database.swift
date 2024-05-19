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

public enum SQLiteError: Error, CustomStringConvertible, Sendable {
    case unknown(code: Int32, message: String)
    case openDatabase(code: Int32, message: String, filename: String, mode: OpenMode)
    case prepareStatement(code: Int32, message: String, query: BoundQuery)
    case emptyQuery(query: BoundQuery)
    case bindingMissmatch(query: BoundQuery, expected: Int32, got: Int)
    case noRowsFetched
    case notSingleValue(columnCount: Int)

    internal enum Context {
        case unknown
        case openDatabase(filename: String, mode: OpenMode)
        case prepareStatement(query: BoundQuery)
    }
    internal init(sqliteErrorCode code: Int32, message: String, context: Context = .unknown) {
        switch context {
        case .unknown:
            self = .unknown(code: code, message: message)
        case .openDatabase(let filename, let mode):
            self = .openDatabase(code: code, message: message, filename: filename, mode: mode)
        case .prepareStatement(let query):
            self = .prepareStatement(code: code, message: message, query: query)
        }
    }

    public var description: String {
        switch self {
        case .unknown(let code, let message): "SQLite Error \(code): \(message)"
        case .openDatabase(let code, let message, let filename, let mode):
            "SQLite Error \(code): \(message) when trying to open a database (filename=\(filename), mode=\(mode))"
        case .prepareStatement(let code, let message, let query):
            "SQLite Error \(code): \(message) in \(query.query) \(query.bindings)"
        case .emptyQuery(let query):
            "Cannot prepare an empty query. \(query.query) \(query.bindings)"
        case let .bindingMissmatch(query: query, expected: expected, got: got) where got < expected:
            "Insufficient number of bindings provided. Query has \(expected) placeholder(s), but \(got) binding(s) provided. In query \(query.query) \(query.bindings)"
        case let .bindingMissmatch(query: query, expected: expected, got: got):
            "Too many bindings provided. Query has \(expected) placeholder(s), but \(got) binding(s) provided. In query \(query.query) \(query.bindings)"
        case .noRowsFetched:
            "When calling .fetchOne() no rows were returned"
        case .notSingleValue(columnCount: 0):
            "Cannot to decode SQLPrimitiveDecodable from a statment that returns zero columns"
        case .notSingleValue(let columnCount):
            "Cannot to decode SQLPrimitiveDecodable from a statment that returns more than one column. (Expected 1, got \(columnCount))"
        }
    }

    public var sqliteErrorCode: Int32? {
        switch self {
        case .unknown(let code, message: _): code
        case .openDatabase(let code, message: _, filename: _, mode: _): code
        case .prepareStatement(let code, message: _, query: _): code
        default: nil
        }
    }
    public var sqliteMessage: String? {
        switch self {
        case .unknown(code: _, let message): message
        case .openDatabase(code: _, let message, filename: _, mode: _): message
        case .prepareStatement(code: _, let message, query: _): message
        default: nil
        }
    }

    public var code: Int32 { sqliteErrorCode ?? SQLITE_ERROR }
    public var message: String { sqliteMessage ?? description }
}

internal let log = Logger(label: "io.github.malien.raw-dawg")

public enum RowIDColumnSelector: Sendable {
    case none
    case column(named: String)
    case column(indexed: Int32)

    static let id: Self = .column(named: "id")
    static let rowid: Self = .column(named: "rowid")
}

public struct InsertionStats: Equatable, Hashable, Sendable {
    public var lastInsertedRowid: sqlite3_int64
    public var rowsAffected: sqlite3_int64
    public var totalRowsAffected: sqlite3_int64
}

public enum OpenMode: Sendable, Equatable, Hashable {
    case readOnly
    case readWrite(create: Bool)
    public static var readWrite = Self.readWrite(create: true)
}

public actor Database {
    fileprivate let db: OpaquePointer

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

    public func prepare(
        _ query: BoundQuery, persistent: Bool = false, rowid: RowIDColumnSelector = .none
    ) throws -> PreparedStatement {
        var stmt: OpaquePointer? = nil
        var flags: UInt32 = 0
        if persistent {
            flags |= UInt32(SQLITE_PREPARE_PERSISTENT)
        }
        try throwing(context: .prepareStatement(query: query)) {
            sqlite3_prepare_v3(
                db: self.db,
                zSql: query.query,
                nByte: Int32(query.query.utf8.count),
                prepFlags: flags,
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
            db: self, stmt: PreparedStatementPtr(ptr: stmt!), rowidColumn: rowid,
            columnNames: columnNames)
    }

    public func execute(_ query: String) throws {
        try throwing {
            sqlite3_exec(db: self.db, sql: query, callback: nil, callbackArgument: nil, errmsg: nil)
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

    func lastError() -> SQLiteError {
        return self.lastError(withCode: sqlite3_errcode(self.db))
    }

    func lastError(withCode code: Int32, context: SQLiteError.Context = .unknown) -> SQLiteError {
        let message =
            if let cMsgStr = sqlite3_errmsg(self.db) {
                String(cString: cMsgStr)
            } else {
                "No error message available"
            }
        return SQLiteError(sqliteErrorCode: code, message: message, context: context)
    }

    func error(unlessOK resultCode: Int32, context: SQLiteError.Context = .unknown) -> SQLiteError?
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
        // This might be hella unsafe. Don't know. Solving this would require doing own DispatchQueue
        // synchronisation. I don't want it. I'd like to stay in the realm of swift actors.
        if let error = Self.error(unsafelyDescribedBy: db, unlessOK: sqlite3_close(db)) {
            log.error("Database close failed. \(error)")
        }
    }
}
