import Algorithms
import Foundation

#if canImport(SQLite3)
    import SQLite3
#else
    import CSQLite
#endif

internal struct PreparedStatementPtr: @unchecked Sendable {
    var ptr: OpaquePointer
}

internal struct CString: @unchecked Sendable {
    var ptr: UnsafePointer<CChar>

    func toOwned() -> String {
        String(cString: self.ptr)
    }
}

public struct PreparedStatement: ~Copyable, Sendable {
    private let db: Database
    private let stmt: PreparedStatementPtr
    /// I don't really understand swift's restriction on `discard self` being restricted to trivially destructible types.
    /// Since we can't use `discard self`, here's the flag, that `.finalize()` was called explicitly before.
    private var finalized = false
    /// Whether or not we've exhausted all of the rows via the `.step()` function.
    /// As per sqlite docs. After `SQLITE_DONE` is yielded from `sqlite3_step`, further calls to step are prohibited
    /// (given that `sqlite3_reset` isn't called before that)
    private var finished = false
    private let rowidColumn: RowIDColumnSelector
    /// ~~C Strings are valid until the call to `sqlite3_finalize`~~
    ///  These are Swift string now. Just to avoid conversions on every `.step()` call
    private let columnNames: [String]

    internal init(
        db: Database, stmt: PreparedStatementPtr, rowidColumn: RowIDColumnSelector,
        columnNames: [CString]
    ) {
        self.db = db
        self.stmt = stmt
        self.rowidColumn = rowidColumn
        // We might not need to copy names into swift strings in the future, for now, we do
        self.columnNames = columnNames.map { String(cString: $0.ptr) }
    }

    public consuming func finalize() async throws {
        self.finalized = true
        try await self.db.finalize(statement: self.stmt)
    }

    /// Poll-style of fetching results
    public mutating func step() async throws -> Row? {
        if finished {
            return nil
        }
        if let values = try await self.db.step(
            statement: self.stmt, columnCount: self.columnNames.count)
        {
            return Row(columns: columnNames, values: values[...])
        } else {
            finished = true
            return nil
        }
    }

    public mutating func step<T: Decodable>() async throws -> T? {
        try await step().map { try $0.decode() }
    }

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

    public consuming func fetchAll() async throws -> [Row] {
        let columns = self.columnNames
        return try await finalizeAfter { statement in
            try await statement.db.fetchAll(
                statement: statement.stmt, columnCount: statement.columnNames.count
            )
            .chunks(ofCount: columns.count).map {
                Row(columns: columns, values: $0)
            }
        }
    }

    public consuming func fetchAll<T: Decodable>() async throws -> [T] {
        if let type = T.self as? SQLPrimitiveDecodable.Type {
            try guardSingleColumn()
            return try await fetchAll().map { try $0.decode(valueAt: 0, as: type) as! T }
        }
        return try await fetchAll().map { try $0.decode() }
    }

    public consuming func fetchOne() async throws -> Row {
        try await finalizeAfter { statement in
            guard let row = try await statement.step() else {
                throw SQLiteError.noRowsFetched
            }
            return row
        }
    }

    public consuming func fetchOne<T: Decodable>() async throws -> T {
        if let type = T.self as? SQLPrimitiveDecodable.Type {
            try guardSingleColumn()
            return try await fetchOne().decode(valueAt: 0, as: type) as! T
        }
        return try await fetchOne().decode()
    }

    public consuming func fetchOptional() async throws -> Row? {
        try await finalizeAfter { statement in
            try await statement.step()
        }
    }

    public consuming func fetchOptional<T: Decodable>() async throws -> T? {
        if let type = T.self as? SQLPrimitiveDecodable.Type {
            try guardSingleColumn()
            return try await fetchOptional().map { try $0.decode(valueAt: 0, as: type) as! T }
        }
        return try await fetchOptional().map { try $0.decode() }
    }

    private func guardSingleColumn() throws {
        guard columnNames.count == 1 else {
            throw SQLiteError.notSingleValue(columnCount: columnNames.count)
        }
    }

    //     I'm not comfortable with this API
    //    /// Push-style of fetching results
    //    public consuming func stream() -> AsyncThrowingStream<Row, any Error>
    //    public consuming func stream<T: Decodable>() -> some AsyncSequence

    /// Returns last inserted rowid
    @discardableResult
    public consuming func run() async throws -> InsertionStats {
        try await finalizeAfter { statement in
            try await statement.db.run(statement: statement.stmt)
        }
    }

    deinit {
        if self.finalized { return }

        let stmnt = self.stmt
        let db = self.db

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
