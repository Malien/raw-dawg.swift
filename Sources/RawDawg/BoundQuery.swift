/// The means by which RawDawg safely escapes and binds interpolated values to a query.
///
/// ```swift
/// let query: BoundQuery = "select * from users where id = \(1)"
/// ```
///
/// It supports three kinds of interpolated values:
/// - any `SQLPrimitiveEncodable` type, which is inserted as a placeholder into the underlying
///   query string, and is bound to the query at execution time.
///   ```swift
///   let name = "Alice"
///   let age = 30
///   let email: String? = nil
///   let quotient: Double = 1.5
///   let query: BoundQuery = """
///       insert into users (name, age, email, quotient) 
///       values (\(name), \(age), \(email), \(quotient))
///       """
///   ```
/// - another `BoundQuery`, which is inserted as a fragment into the underlying query string.
///   ```swift
///   let whereClause: BoundQuery = "where id = \(1)"
///   let query: BoundQuery = "select * from users \(fragment: whereClause)"
///   ```
/// - a raw string, which is inserted as-is into the underlying query string. This is 
///   not safe and should be used with caution.
///   ```swift
///   let query: BoundQuery = "select * from users where id = \(1) \(raw: "and name = 'Alice'")"
///   ```
public struct BoundQuery: ExpressibleByStringLiteral, ExpressibleByStringInterpolation, Sendable {
    var query: String
    var bindings: [SQLiteValue]

    public typealias StringLiteralType = StaticString

    init(raw query: String, bindings: [SQLiteValue]) {
        self.query = query
        self.bindings = bindings
    }

    public init(stringLiteral value: Self.StringLiteralType) {
        query = value.description
        bindings = []
    }

    public struct StringInterpolation: StringInterpolationProtocol {
        var query: String
        var bindings: [SQLiteValue] = []

        public typealias StringLiteralType = StaticString

        public init(literalCapacity: Int, interpolationCount: Int) {
            query = ""
            query.reserveCapacity(literalCapacity + interpolationCount)
            bindings = []
            bindings.reserveCapacity(interpolationCount)
        }

        public mutating func appendLiteral(_ literal: StaticString) {
            query.append(literal.description)
        }

        public mutating func appendInterpolation<T: SQLPrimitiveEncodable>(_ value: T) {
            query += "?"
            bindings.append(value.encode())
        }

        public mutating func appendInterpolation(fragment: BoundQuery?) {
            guard let fragment = fragment else { return }
            query += fragment.query
            bindings += fragment.bindings
        }

        public mutating func appendInterpolation(raw: String) {
            query += raw
        }
    }

    public init(stringInterpolation: Self.StringInterpolation) {
        query = stringInterpolation.query
        bindings = stringInterpolation.bindings
    }
}
