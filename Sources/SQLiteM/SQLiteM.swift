import Logging
// The Swift Programming Language
// https://docs.swift.org/swift-book
import SQLite3
import Dispatch

public enum OpenMode {
    case readOnly, readWrite
}

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

public struct SQLiteError: Error, CustomStringConvertible {
    public var code: Int32
    public var message: String

    public var description: String {
        return "SQLite Error \(code): \(message)"
    }

    fileprivate init(lastError db: isolated Database, code: Int32? = nil) {
        self.init(lastError: db.db, code: code)
    }
    fileprivate init(lastError db: OpaquePointer, code: Int32? = nil) {
        self.code = code ?? sqlite3_errcode(db)
        self.message = String(cString: sqlite3_errmsg(db))
    }
    fileprivate init?(unlessOK resultCode: Int32, db: isolated Database) {
        self.init(unlessOK: resultCode, db: db.db)
    }
    fileprivate init?(unlessOK resultCode: Int32, db: OpaquePointer) {
        if resultCode != SQLITE_OK {
            self.init(lastError: db, code: resultCode)
        } else {
            return nil
        }
    }
}

public struct SQLiteEmptyQuery: Error, CustomStringConvertible {
    public var description: String {
        return "Cannot prepare an empty query."
    }
}

private let log = Logger(label: "io.github.malien.SQLiteM")

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
        if let error = SQLiteError(unlessOK: res, db: db!) {
            if db != nil {
                let res = sqlite3_close_v2(db)
                if let closeError = SQLiteError(unlessOK: res, db: db!) {
                    log.error(
                        "Database open failed, during which closing the database also failed. \(closeError)"
                    )
                }
            }
            throw error
        }

        self.db = db!
    }

    @available(macOS 10.14, *)
    public func prepare(_ query: String, persistent: Bool = false) throws -> PreparedStatement {
        var stmt: OpaquePointer? = nil
        var flags: UInt32 = 0
        if persistent {
            flags |= UInt32(SQLITE_PREPARE_PERSISTENT)
        }
        try throwing {
            sqlite3_prepare_v3(
                db: self.db,
                zSql: query,
                nByte: Int32(query.utf8.count),
                prepFlags: flags,
                ppStmt: &stmt,
                pzTail: nil
            )
        }
        if stmt == nil {
            throw SQLiteEmptyQuery()
        }
        return PreparedStatement(db: self, stmt: PreparedStatementPtr(ptr: stmt!))
    }

    @available(macOS, obsoleted: 10.14)
    public func prepare(_ query: String) throws -> PreparedStatement {
        var stmt: OpaquePointer? = nil
        try throwing {
            sqlite3_prepare_v2(
                db: self.db,
                zSql: query,
                nByte: Int32(query.utf8.count),
                ppStmt: &stmt,
                pzTail: nil
            )
        }
        if stmt == nil {
            throw SQLiteEmptyQuery()
        }
        return PreparedStatement(db: self, stmt: PreparedStatementPtr(ptr: stmt!))
    }
    
    private func throwing(_ action: () -> Int32) throws {
        if let error = SQLiteError(unlessOK: action(), db: self.db) {
            throw error
        }
    }

    fileprivate func finalize(statement: PreparedStatementPtr) throws {
        try throwing {
            sqlite3_finalize(statement.ptr)
        }
    }
    
    deinit {
        if let error = SQLiteError(unlessOK: sqlite3_close(db), db: db) {
            log.error("Database close failed. \(error)")
        }
    }
}

private struct PreparedStatementPtr: @unchecked Sendable {
    var ptr: OpaquePointer
}

public struct PreparedStatement: ~Copyable {
    private let db: Database
    private let stmt: PreparedStatementPtr
    private var finalized = false
    
    fileprivate init(db: Database, stmt: PreparedStatementPtr) {
        self.db = db
        self.stmt = stmt
    }
    
    public consuming func finalize() async throws {
        self.finalized = true
        try await self.db.finalize(statement: self.stmt)
    }
    
    deinit {
        if self.finalized { return }
        
        let stmnt = self.stmt
        let db = self.db
        Task {
            do {
                try await db.finalize(statement: stmnt)
            } catch let error {
                log.error("Couldn't finalize prepared statement: \(error)")
            }
        }
    }
}
