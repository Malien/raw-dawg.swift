#if canImport(SQLite3)
    import SQLite3
#else
    import CSQLite
#endif

/// Unified error interface, to all the things wrong that could arise when using ``RawDawg``
///
/// ## Topics
/// ### SQLite3 errors
/// - ``unknown(code:message:)``
/// - ``openDatabase(code:message:filename:mode:)``
/// - ``prepareStatement(code:message:query:)``
/// - ``sqliteErrorCode``
/// - ``sqliteMessage``
/// - ``code``
/// - ``message``
public enum SQLiteError: Error, CustomStringConvertible, Sendable {
    /// An SQLite3 error without any additional context
    case unknown(code: Int32, message: String)
    /// SQLite3 error which occurred while opening a database connection via ``Database/init(filename:mode:)``
    case openDatabase(code: Int32, message: String, filename: String, mode: OpenMode)
    /// SQLite3 error which occurred while preparing a statement via ``Database/prepare(_:)``
    case prepareStatement(code: Int32, message: String, query: BoundQuery)
    /// When the supplied statement doesn't contain statment at all (aka. empty/blank string, or just SQL comments)
    ///
    /// Thrown by ``Database/prepare(_:)``
    ///
    /// ```swift
    /// try await db.prepare("-- haha empty query") // <- will error
    /// ```
    case emptyQuery(query: BoundQuery)
    /// If the number of supplied binding placeholders and actual binding values differ
    ///
    /// Thrown by ``Database/prepare(_:)``
    ///
    /// ```swift
    /// try await db.prepare("select ?, \(42)") // <- will error
    /// try await db.prepare(BoundQuery(
    ///     raw: "select 69",
    ///     bindings: [.integer(420)]
    /// )) // <- will also error error
    /// ```
    case bindingMissmatch(query: BoundQuery, expected: Int32, got: Int)
    /// When calls to ``PreparedStatement/fetchOne()-o1ui`` (and friends) yield 0 rows
    ///
    /// Thrown by: ``PreparedStatement/fetchOne()-8yva9``, ``PreparedStatement/fetchOne()-o1ui``, ``PreparedStatement/fetchOne()-4grfr``
    ///
    /// ```swift
    /// try await db.prepare("""
    ///     with users(id, name) as 
    ///         (values (1, 'Alice'), (2, 'Bob'))
    ///     select * from users where name = 'John'
    ///     """")
    ///     .fetchOne() // <- will return 0 rows, and as a result error out
    /// ```
    case noRowsFetched
    /// When decode to tuple isn't possible, due to column/tuple element count missmatch
    ///
    /// Thrown by: ``PreparedStatement/step()-92san``, ``PreparedStatement/fetchOne()-8yva9``,
    /// ``PreparedStatement/fetchAll()-6jov4``, ``PreparedStatement/fetchOptional()-92nz3``,
    /// ``Row/decode()-5gu78``
    ///
    /// ```swift
    /// let (id, name): (Int, String) = db
    ///     .prepare("select id, first_name, last_name from users")
    ///     .fetchOne() // <- will fail
    ///
    /// let user: (Int, String, String)? = db
    ///     .prepare("select id from users")
    ///     .fetchOptional() // <- will fail
    /// ```
    case columnCountMissmatch(expected: Int, got: Int)

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
            "SQLite Error \(code): \(message) in \(query.queryString) \(query.bindings)"
        case .emptyQuery(let query):
            "Cannot prepare an empty query. \(query.queryString) \(query.bindings)"
        case let .bindingMissmatch(query: query, expected: expected, got: got) where got < expected:
            "Insufficient number of bindings provided. Query has \(expected) placeholder(s), but \(got) binding(s) provided. In query \(query.queryString) \(query.bindings)"
        case let .bindingMissmatch(query: query, expected: expected, got: got):
            "Too many bindings provided. Query has \(expected) placeholder(s), but \(got) binding(s) provided. In query \(query.queryString) \(query.bindings)"
        case .noRowsFetched:
            "When calling .fetchOne() no rows were returned"
        case .columnCountMissmatch(let expected, let got):
            "Expected to decode \(expected) values from a statment that yields \(got) columns"
        }
    }

    /// Error code if the error was originiated directly from SQLite3, `nil` otherwise
    public var sqliteErrorCode: Int32? {
        switch self {
        case .unknown(let code, message: _): code
        case .openDatabase(let code, message: _, filename: _, mode: _): code
        case .prepareStatement(let code, message: _, query: _): code
        default: nil
        }
    }
    /// Error message if the error was originiated directly from SQLite3, `nil` otherwise
    public var sqliteMessage: String? {
        switch self {
        case .unknown(code: _, let message): message
        case .openDatabase(code: _, let message, filename: _, mode: _): message
        case .prepareStatement(code: _, let message, query: _): message
        default: nil
        }
    }

    /// Error code if the error was originiated directly from SQLite3, `SQLITE_ERROR` otherwise
    public var code: Int32 { sqliteErrorCode ?? SQLITE_ERROR }
    /// Error message if the error was originiated directly from SQLite3, ``description`` otherwise
    public var message: String { sqliteMessage ?? description }
}
