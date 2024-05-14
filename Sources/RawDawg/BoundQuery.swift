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
