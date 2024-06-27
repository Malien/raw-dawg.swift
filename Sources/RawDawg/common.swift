#if canImport(SQLite3)
import SQLite3
#else
import CSQLite
#endif
import Logging

internal let log = Logger(label: "com.github.malien.raw-dawg")

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
    public static let readWrite = Self.readWrite(create: true)
}

/// A wrapper around the `sqlite3_stmt` pointer.
internal struct PreparedStatementPtr: @unchecked Sendable {
    var ptr: OpaquePointer
}

/// Just a type wrapper around `*const c_char` to make it more Swift-y
internal struct CString: @unchecked Sendable {
    var ptr: UnsafePointer<CChar>

    func toOwned() -> String {
        String(cString: self.ptr)
    }
}

// C functions, but with swift's named parameters
internal func sqlite3_open_v2(
    filename: String, ppDb: UnsafeMutablePointer<OpaquePointer?>?, flags: Int32,
    zVfs: UnsafePointer<Int8>?
) -> Int32 {
    return sqlite3_open_v2(filename, ppDb, flags, zVfs)
}

@available(macOS 10.14, *)
internal func sqlite3_prepare_v3(
    db: OpaquePointer!, zSql: UnsafePointer<CChar>!, nByte: Int32, prepFlags: UInt32,
    ppStmt: UnsafeMutablePointer<OpaquePointer?>!,
    pzTail: UnsafeMutablePointer<UnsafePointer<CChar>?>!
) -> Int32 {
    return sqlite3_prepare_v3(db, zSql, nByte, prepFlags, ppStmt, pzTail)
}

internal func sqlite3_prepare_v2(
    db: OpaquePointer!, zSql: UnsafePointer<CChar>!, nByte: Int32,
    ppStmt: UnsafeMutablePointer<OpaquePointer?>!,
    pzTail: UnsafeMutablePointer<UnsafePointer<CChar>?>!
) -> Int32 {
    return sqlite3_prepare_v2(db, zSql, nByte, ppStmt, pzTail)
}

internal func sqlite3_exec(
    db: OpaquePointer!,
    sql: UnsafePointer<CChar>!,
    callback: sqlite3_callback!,
    callbackArgument: UnsafeMutableRawPointer!,
    errmsg: UnsafeMutablePointer<UnsafeMutablePointer<CChar>?>!
) -> Int32 {
    return sqlite3_exec(db, sql, callback, callbackArgument, errmsg)
}

internal func sqlite3_bind_text64(
    statement: OpaquePointer!,
    position: Int32,
    text: UnsafePointer<CChar>!,
    byteSize: sqlite3_uint64,
    destructor: sqlite3_destructor_type!,
    encoding: UInt8
) -> Int32 {
    sqlite3_bind_text64(statement, position, text, byteSize, destructor, encoding)
}

internal func sqlite3_bind_blob64(
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
internal let SQLITE_STATIC = unsafeBitCast(0, to: sqlite3_destructor_type.self)
internal let SQLITE_TRANSIENT = unsafeBitCast(-1, to: sqlite3_destructor_type.self)

// This is hacky af. But from what I've seen at compiler explorer, simple constant folding optimizes this pattern easily.
// This might get a lot simpler in swift 6, yet in the meantime, damn do I feel clever af.
internal func packLength<each T>(_ witness: repeat each T) -> Int {
    var length = 0
    _ = (repeat increment(each witness, &length))
    return length
}

private func increment<T>(_ dummy: T, _ value: inout Int) {
    value += 1
}
