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

public struct SQLiteError: Error, CustomStringConvertible {
    public var code: Int32
    public var message: String

    public var description: String {
        return "SQLite Error \(code): \(message)"
    }
}

public enum InvalidQuery: Error, CustomStringConvertible {
    case empty
    case bindingMissmatch(expected: Int32, got: Int)

    public var description: String {
        return switch self {
        case .empty:
            "Cannot prepare an empty query."
        case let .bindingMissmatch(expected: expected, got: got) where got < expected:
            "Insufficient number of bindings provided. Query has \(expected) placeholder(s), but \(got) binding(s) provided"
        case let .bindingMissmatch(expected: expected, got: got):
            "Too many bindings provided. Query has \(expected) placeholder(s), but \(got) binding(s) provided"
        }
    }
}

internal let log = Logger(label: "io.github.malien.raw-dawg")

public enum RowIDColumnSelector: Sendable {
    case none
    case column(named: String)
    case column(indexed: Int32)

    static let id: Self = .column(named: "id")
    static let rowid: Self = .column(named: "rowid")
}

public struct InsertionStats: Equatable, Hashable {
    public var lastInsertedRowid: sqlite3_int64
    public var rowsAffected: sqlite3_int64
    public var totalRowsAffected: sqlite3_int64
}

public enum OpenMode {
    case readOnly, readWrite
}

public actor Database {
    fileprivate let db: OpaquePointer

    public init(filename: String, mode: OpenMode = .readWrite, create: Bool = false) throws {
        var db: OpaquePointer? = nil
        var flags: Int32 = 0
        if mode == .readOnly {
            flags |= SQLITE_OPEN_READONLY
        } else {
            flags |= SQLITE_OPEN_READWRITE
        }
        if create {
            flags |= SQLITE_OPEN_CREATE
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

        if let error = Self.error(unsafelyDescribedBy: db, unlessOK: res) {
            let res = sqlite3_close_v2(db)
            if let closeError = Self.error(unsafelyDescribedBy: db, unlessOK: res) {
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
        try throwing {
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
            throw InvalidQuery.empty
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
            throw InvalidQuery.bindingMissmatch(expected: bindingCount, got: query.bindings.count)
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

    private func throwing(_ action: () -> Int32) throws {
        if let error = self.error(unlessOK: action()) {
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

    func lastError(withCode code: Int32) -> SQLiteError {
        let message =
            if let cMsgStr = sqlite3_errmsg(self.db) {
                String(cString: cMsgStr)
            } else {
                "No error message available"
            }
        return SQLiteError(code: code, message: message)
    }

    func error(unlessOK resultCode: Int32) -> SQLiteError? {
        if resultCode != SQLITE_OK {
            return self.lastError(withCode: resultCode)
        } else {
            return nil
        }
    }

    // This constructor is safe to call only from the thread (actor) that manages the database connection
    private static func error(unsafelyDescribedBy db: OpaquePointer, code: Int32) -> SQLiteError {
        let message =
            if let cMsgStr = sqlite3_errmsg(db) {
                String(cString: cMsgStr)
            } else {
                "No error message available"
            }
        return SQLiteError(code: code, message: message)
    }

    // This constructor is safe to call only from the thread (actor) that manages the database connection
    private static func error(unsafelyDescribedBy db: OpaquePointer, unlessOK resultCode: Int32)
        -> SQLiteError?
    {
        if resultCode != SQLITE_OK {
            return error(unsafelyDescribedBy: db, code: resultCode)
        } else {
            return nil
        }
    }

    internal func step(statement: PreparedStatementPtr, columnNames: [CString]) throws -> Row? {
        let res = sqlite3_step(statement.ptr)
        switch res {
        case SQLITE_DONE:
            return nil
        case SQLITE_ROW:
            let values = try columnNames.indices.map {
                try self.parseValue(in: statement, at: Int32($0))
            }
            return Row(columns: columnNames.map { $0.toOwned() }, values: values)
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

    internal func fetchAll(statement: PreparedStatementPtr, columnNames: [CString]) throws
        -> [Row]
    {
        var result = [Row]()
        while let row = try step(statement: statement, columnNames: columnNames) {
            result.append(row)
        }
        return result
    }
    
    internal func run(statement: PreparedStatementPtr) throws -> InsertionStats {
        let res = sqlite3_step(statement.ptr)
        switch res {
        case SQLITE_DONE: fallthrough
        case SQLITE_ROW:
            let rowsAffected = if #available(macOS 12.3, iOS 15.4, *) {
                sqlite3_changes64(db)
            } else {
                Int64(sqlite3_changes(db))
            }
            let totalRowsAffected = if #available(macOS 12.3, iOS 15.4, *) {
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

public struct Row: Equatable, Sequence {
    public let columns: [String]
    public var values: [SQLiteValue]

    public typealias Element = (columnName: String, value: SQLiteValue)

    public struct Iterator: IteratorProtocol {
        public typealias Element = Row.Element
        var inner: Zip2Sequence<[String], [SQLiteValue]>.Iterator

        public mutating func next() -> Self.Element? {
            return if let (name, value) = inner.next() {
                (name, value)
            } else {
                nil
            }
        }
    }

    public func makeIterator() -> Iterator {
        Iterator(inner: zip(columns, values).makeIterator())
    }

    public func decode<T: Decodable>() throws -> T {
        try T(from: SQLDecoder(row: self))
    }
}
