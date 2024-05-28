import Algorithms
import Foundation

#if canImport(SQLite3)
    import SQLite3
#else
    import CSQLite
#endif

public struct SyncPreparedStatement: ~Copyable {
    private let conn: UnmanagedSyncConnection
    internal let ptr: PreparedStatementPtr
    /// I don't really understand swift's restriction on `discard self` being restricted to trivially destructible types.
    /// Since we can't use `discard self`, here's the flag, that `.finalize()` was called explicitly before.
    private var finalized = false
    /// Whether or not we've exhausted all of the rows via the `.step()` function.
    /// As per sqlite docs. After `SQLITE_DONE` is yielded from `sqlite3_step`, further calls to step are prohibited
    /// (given that `sqlite3_reset` isn't called before that)
    private var finished = false
    /// ~~C Strings are valid until the call to `sqlite3_finalize`~~
    /// These are Swift string now. Just to avoid conversions on every `.step()` call
    internal let columnNames: [String]

    internal init(conn: UnmanagedSyncConnection, stmt: PreparedStatementPtr, columnNames: [CString]) {
        self.conn = conn
        self.ptr = stmt
        // We might not need to copy names into swift strings in the future, for now, we do
        self.columnNames = columnNames.map { String(cString: $0.ptr) }
    }

    public consuming func finalize() throws {
        self.finalized = true
        try self.conn.finalize(statement: self.ptr)
    }
    
    @discardableResult
    internal consuming func run() throws -> InsertionStats {
        try finalizeAfter { statement in
            try statement.conn.run(statement: statement.ptr)
        }
    }

    // MARK: Step overloads

    public mutating func step() throws -> Row? {
        if finished {
            return nil
        }
        if let values = try self.conn.step(
            statement: self.ptr, columnCount: self.columnNames.count)
        {
            return Row(columns: columnNames, values: values[...])
        } else {
            finished = true
            return nil
        }
    }

    public mutating func step<Column1, Column2, each Column>() throws -> (
        Column1, Column2, repeat each Column
    )?
    where
        Column1: SQLPrimitiveDecodable, Column2: SQLPrimitiveDecodable,
        repeat each Column: SQLPrimitiveDecodable
    {
        try guardColumnCount(expected: 2 + packLength(repeat (each Column).self))
        let row: Row? = try self.step()
        return try row.map { try $0.decode() }
    }

    public mutating func step<T: Decodable>() throws -> T? {
        if let type = T.self as? SQLPrimitiveDecodable.Type {
            try guardColumnCount(expected: 1)
            return try step().map { try $0.decode(valueAt: 0, as: type) as! T }
        }
        return try step().map { try $0.decode() }
    }

    // MARK: Fetch All overloads
    
    internal consuming func fetchAll() throws -> [Row] {
        let columns = self.columnNames
        return try self.finalizeAfter { statement in
            try statement.conn.fetchAll(
                statement: statement.ptr, columnCount: statement.columnNames.count
            )
            .chunks(ofCount: columns.count).map {
                Row(columns: columns, values: $0)
            }
        }
    }
    
    internal consuming func fetchAll<Column1, Column2, each Column>() throws -> [(
        Column1, Column2, repeat each Column
    )]
    where
        Column1: SQLPrimitiveDecodable, Column2: SQLPrimitiveDecodable,
        repeat each Column: SQLPrimitiveDecodable
    {
        try guardColumnCount(expected: 2 + packLength(repeat (each Column).self))
        let row: [Row] = try self.fetchAll()
        return try row.map { try $0.decode() }
    }

    internal consuming func fetchAll<T: Decodable>() throws -> [T] {
        if let type = T.self as? SQLPrimitiveDecodable.Type {
            try guardColumnCount(expected: 1)
            return try fetchAll().map { try $0.decode(valueAt: 0, as: type) as! T }
        }
        return try fetchAll().map { try $0.decode() }
    }
    
    // MARK: Fetch One overloads

    internal consuming func fetchOne() throws -> Row {
        try finalizeAfter { statement in
            guard let row = try statement.step() else {
                throw SQLiteError.noRowsFetched
            }
            return row
        }
    }

    // Curiously there is no second "padding" generic argument for this one as it doesn't seem
    // to conflict with <T: Decodable> overload. Hella curious. Basically any applied wrapper
    // (aka. Optiona<(repeat each Column)> or [(repeat each Column)]) breaks resolution.
    // Still there is one to prevent forming let _: () = statement.fetchOne() from happenning
    internal consuming func fetchOne<Column1, each Column>() throws -> (
        Column1, repeat each Column
    )
    where Column1: SQLPrimitiveDecodable, repeat each Column: SQLPrimitiveDecodable {
        try guardColumnCount(expected: 1 + packLength(repeat (each Column).self))
        let row: Row = try self.fetchOne()
        return try row.decode()
    }

    internal consuming func fetchOne<T: Decodable>() throws -> T {
        if let type = T.self as? SQLPrimitiveDecodable.Type {
            try guardColumnCount(expected: 1)
            return try fetchOne().decode(valueAt: 0, as: type) as! T
        }
        return try fetchOne().decode()
    }
    
    // MARK: Fetch Optional overloads
    
    internal consuming func fetchOptional() throws -> Row? {
        try finalizeAfter { statement in
            try statement.step()
        }
    }

    internal consuming func fetchOptional<Column1, Column2, each Column>() throws -> (
        Column1, Column2, repeat each Column
    )?
    where
        Column1: SQLPrimitiveDecodable, Column2: SQLPrimitiveDecodable,
        repeat each Column: SQLPrimitiveDecodable
    {
        try guardColumnCount(expected: 2 + packLength(repeat (each Column).self))
        let row: Row? = try self.fetchOptional()
        return try row.map { try $0.decode() }
    }

    internal consuming func fetchOptional<T: Decodable>() throws -> T? {
        if let type = T.self as? SQLPrimitiveDecodable.Type {
            try guardColumnCount(expected: 1)
            return try fetchOptional().map { try $0.decode(valueAt: 0, as: type) as! T }
        }
        return try fetchOptional().map { try $0.decode() }
    }

    // MARK: Private helpers
    
    private func guardColumnCount(expected: Int) throws {
        guard columnNames.count == expected else {
            throw SQLiteError.columnCountMissmatch(expected: expected, got: columnNames.count)
        }
    }
    
    consuming func finalizeAfter<T>(action: (inout Self) throws -> T) throws
        -> T
    {
        // There is no async defer blocks, so this mess is emulating it (or the finally block)
        var result: T
        do {
            result = try action(&self)
        } catch let e {
            try finalize()
            throw e
        }
        try finalize()
        return result
    }

    deinit {
        if self.finalized { return }

        let stmnt = self.ptr
        let db = self.conn

        // Requirement on Task is the only thing preventing this from building on macOS < 10.15
        Task {
            do {
                try db.finalize(statement: stmnt)
            } catch let error {
                log.error("Couldn't finalize prepared statement: \(error)")
            }
        }
    }
}
