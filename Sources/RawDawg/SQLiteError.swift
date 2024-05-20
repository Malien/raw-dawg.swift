#if canImport(SQLite3)
    import SQLite3
#else
    import CSQLite
#endif

public enum SQLiteError: Error, CustomStringConvertible, Sendable {
    case unknown(code: Int32, message: String)
    case openDatabase(code: Int32, message: String, filename: String, mode: OpenMode)
    case prepareStatement(code: Int32, message: String, query: BoundQuery)
    case emptyQuery(query: BoundQuery)
    case bindingMissmatch(query: BoundQuery, expected: Int32, got: Int)
    case noRowsFetched
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
            "SQLite Error \(code): \(message) in \(query.query) \(query.bindings)"
        case .emptyQuery(let query):
            "Cannot prepare an empty query. \(query.query) \(query.bindings)"
        case let .bindingMissmatch(query: query, expected: expected, got: got) where got < expected:
            "Insufficient number of bindings provided. Query has \(expected) placeholder(s), but \(got) binding(s) provided. In query \(query.query) \(query.bindings)"
        case let .bindingMissmatch(query: query, expected: expected, got: got):
            "Too many bindings provided. Query has \(expected) placeholder(s), but \(got) binding(s) provided. In query \(query.query) \(query.bindings)"
        case .noRowsFetched:
            "When calling .fetchOne() no rows were returned"
        case .columnCountMissmatch(let expected, let got):
            "Expected to decode \(expected) values from a statment that yields \(got) columns"
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
