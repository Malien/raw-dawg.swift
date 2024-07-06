/// The transaction object given by ``SyncConnection/transaction(_:block:)``
///
/// Used in place of the ``SyncConnection``, while the database is under transaction.
///
/// The methods are equivalent to those of ``SyncConnection``
///
/// ### Thread-safety and Concurrency
/// Calling `Transaction` methods is not thread-safe, and should not be done concurrently.
/// The type itself is explicity NOT `Sendable`
public struct Transaction: ~Copyable {
    public enum Kind: String, Sendable {
        case deferred, immediate, exclusive
    }

    private let conn: UnmanagedSyncConnection

    internal init(conn: UnmanagedSyncConnection) {
        self.conn = conn
    }

    public func run(_ query: BoundQuery) throws -> InsertionStats {
        let (ptr, _) = try conn.prepare(query)
        return try conn.run(statement: ptr)
    }
    public func execute(_ query: String) throws {
        try conn.execute(query)
    }
    public func preparing<T>(_ query: BoundQuery, block: (inout SyncPreparedStatement) throws -> T)
        throws -> T
    {
        let (ptr, columns) = try conn.prepare(query)
        let stmt = SyncPreparedStatement(conn: conn, stmt: ptr, columnNames: columns)
        return try stmt.finalizeAfter { stmt in
            try block(&stmt)
        }
    }

    private func unsafePrepare(_ query: BoundQuery) throws -> SyncPreparedStatement {
        let (ptr, columns) = try conn.prepare(query)
        return SyncPreparedStatement(conn: conn, stmt: ptr, columnNames: columns)
    }

    public func fetchAll(_ query: BoundQuery) throws -> [Row] {
        try unsafePrepare(query).fetchAll()
    }

    public func fetchAll<Column1, Column2, each Column>(_ query: BoundQuery) throws -> [(
        Column1, Column2, repeat each Column
    )]
    where
        Column1: SQLPrimitiveDecodable, Column2: SQLPrimitiveDecodable,
        repeat each Column: SQLPrimitiveDecodable
    {
        try unsafePrepare(query).fetchAll()
    }

    public func fetchAll<T: Decodable>(_ query: BoundQuery) throws -> [T] {
        try unsafePrepare(query).fetchAll()
    }

    public func fetchOne(_ query: BoundQuery) throws -> Row {
        try unsafePrepare(query).fetchOne()
    }

    public func fetchOne<Column1, each Column>(_ query: BoundQuery) throws -> (
        Column1, repeat each Column
    )
    where Column1: SQLPrimitiveDecodable, repeat each Column: SQLPrimitiveDecodable {
        try unsafePrepare(query).fetchOne()
    }

    public func fetchOne<T: Decodable>(_ query: BoundQuery) throws -> T {
        try unsafePrepare(query).fetchOne()
    }

    public func fetchOptional(_ query: BoundQuery) throws -> Row? {
        try unsafePrepare(query).fetchOptional()
    }

    public func fetchOptional<Column1, Column2, each Column>(_ query: BoundQuery) throws -> (
        Column1, Column2, repeat each Column
    )?
    where
        Column1: SQLPrimitiveDecodable, Column2: SQLPrimitiveDecodable,
        repeat each Column: SQLPrimitiveDecodable
    {
        try unsafePrepare(query).fetchOptional()
    }

    public func fetchOptional<T: Decodable>(_ query: BoundQuery) throws -> T? {
        try unsafePrepare(query).fetchOptional()
    }

    internal consuming func commit() throws {
        let (statement, _) = try conn.prepare("commit")
        do {
            try conn.run(statement: statement)
        } catch {
            try conn.finalize(statement: statement)
            throw error
        }
        try conn.finalize(statement: statement)
    }

    internal consuming func rollback() throws {
        let (statement, _) = try conn.prepare("rollback")
        do {
            try conn.run(statement: statement)
        } catch {
            try conn.finalize(statement: statement)
            throw error
        }
        try conn.finalize(statement: statement)
    }
}
