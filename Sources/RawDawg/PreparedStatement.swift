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

public struct NoRowsFetched: Error, CustomStringConvertible {
    public var description: String {
        "When calling .fetchOne() no rows were returned"
    }
}

public struct PreparedStatement: ~Copyable, Sendable {
    private let db: Database
    private let stmt: PreparedStatementPtr
    private var finalized = false
    private let rowidColumn: RowIDColumnSelector
    // Strings are valid until the call to sqlite3_finalize
    private let columnNames: [CString]

    internal init(
        db: Database, stmt: PreparedStatementPtr, rowidColumn: RowIDColumnSelector,
        columnNames: [CString]
    ) {
        self.db = db
        self.stmt = stmt
        self.rowidColumn = rowidColumn
        self.columnNames = columnNames
    }

    public consuming func finalize() async throws {
        self.finalized = true
        try await self.db.finalize(statement: self.stmt)
    }

    /// Poll-style of fetching results
    public mutating func step() async throws -> Row? {
        try await self.db.step(statement: self.stmt, columnNames: self.columnNames)
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
        try await finalizeAfter { statement in
            try await statement.db.fetchAll(
                statement: statement.stmt, columnNames: statement.columnNames)
        }
    }

    public consuming func fetchAll<T: Decodable>() async throws -> [T] {
        try await fetchAll().map { try $0.decode() }
    }

    public consuming func fetchOne() async throws -> Row {
        try await finalizeAfter { statement in
            guard let row = try await statement.step() else {
                throw NoRowsFetched()
            }
            return row
        }
    }

    public consuming func fetchOne<T: Decodable>() async throws -> T {
        try await fetchOne().decode()
    }

    public consuming func fetchOptional() async throws -> Row? {
        try await finalizeAfter { statement in
            try await statement.step()
        }
    }

    public consuming func fetchOptional<T: Decodable>() async throws -> T? {
        try await fetchOptional().map { try $0.decode() }
    }

    /// Push-style of fetching results
    public consuming func stream() -> AsyncThrowingStream<Row, any Error> {
        // forget self
        self.finalized = true
        let stmt = self.stmt
        let columnNames = self.columnNames
        let db = self.db

        return AsyncThrowingStream { continuation in
            Task {
                do {
                    while let row = try await db.step(statement: stmt, columnNames: columnNames) {
                        continuation.yield(row)
                    }
                } catch let e {
                    try? await db.finalize(statement: stmt)
                    continuation.finish(throwing: e)
                    return
                }
                try await db.finalize(statement: stmt)
            }
        }
    }

    public consuming func stream<T: Decodable>() -> some AsyncSequence {
        self.stream().map { row async throws -> T in try row.decode() }
    }
    
    /// Returns last inserted rowid
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

