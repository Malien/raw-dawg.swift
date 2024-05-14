import XCTest

@testable import RawDawg

final class SQLiteM_swiftTests: XCTestCase {
    func testCanSuccessfullyOpenInMemoryDB() throws {
        _ = try Database(filename: ":memory:")
    }

    func testCanPrepareStatement() async throws {
        let db = try Database(filename: ":memory:", mode: .readOnly)
        let stmt = try await db.prepare("SELECT 1")
        try await stmt.finalize()
    }

    func testCanSelectInteger() async throws {
        let db = try Database(filename: ":memory:", mode: .readOnly)
        var stmt = try await db.prepare("SELECT 1")
        let row = try await stmt.step()!
        XCTAssertEqual(
            row,
            Row(columns: ["1"], values: [.integer(1)])
        )
        try await stmt.finalize()
    }

    func testCanSelectTypedInteger() async throws {
        let db = try Database(filename: ":memory:", mode: .readOnly)
        let res: Int = try await db.prepare("SELECT 1").fetchOne()
        XCTAssertEqual(res, 1)
    }

    func testCanSelectIntegerEnum() async throws {
        enum State: Int, Codable {
            case pending = 0
            case completed = 1
            case failed = 2
        }
        let db = try Database(filename: ":memory:", mode: .readOnly)
        let states: [State] = try await db.prepare(
            """
            with cte(state) as (values (0), (1), (2))
                select state from cte
            """
        ).fetchAll()
        XCTAssertEqual(states, [.pending, .completed, .failed])
    }

    func testCanSelectStringEnum() async throws {
        enum State: String, Codable {
            case pending, completed, failed
        }
        let db = try Database(filename: ":memory:", mode: .readOnly)
        let states: [State] = try await db.prepare(
            """
            with cte(state) as (values ('pending'), ('completed'), ('failed'))
                select state from cte
            """
        ).fetchAll()
        XCTAssertEqual(states, [.pending, .completed, .failed])
    }

    func testCantDeserializePrimitiveWhenMoreThanOneColumnIsSelected() async throws {
        let db = try Database(filename: ":memory:", mode: .readOnly)
        let statement = try await db.prepare("select 1, 2")
        try await assertThrows(statement: statement) {
            (statement: consuming PreparedStatement) async throws in
            _ = try await statement.fetchOne() as Int
        }
    }

    func testCanSelectAllKindsOfThings() async throws {
        let db = try Database(filename: ":memory:")
        var stmt = try await db.prepare(
            "SELECT 1 as i64, 2.0 as f64, 'text' as string, unhex('42069f') as bytes, null as nil")
        let row = try await stmt.step()!
        XCTAssertEqual(
            row,
            specialRow(i64: 1, f64: 2.0, string: "text", bytes: [0x42, 0x06, 0x9f])
        )
        try await stmt.finalize()
    }

    private func prepareSampleDB() async throws -> Database {
        let db = try Database(filename: ":memory:")
        try await db.execute(
            """
            create table test(
                i64 integer,
                f64 float,
                string text,
                bytes blob,
                nil integer
            );
            insert into test values
                (1, 1.0, 'first', unhex('01'), null),
                (2, 2.0, 'second', unhex('02'), null),
                (3, 3.0, 'third', unhex('03'), null);
            """)
        return db
    }

    private func assertThrows(block: () async throws -> Void) async throws {
        var error: (any Error)? = nil
        do {
            try await block()
        } catch let e {
            error = e
        }
        XCTAssertNotNil(error)
    }
    private func assertThrows(
        statement: consuming PreparedStatement,
        block: (consuming PreparedStatement) async throws -> Void
    ) async throws {
        var error: (any Error)? = nil
        do {
            try await block(statement)
        } catch let e {
            error = e
        }
        XCTAssertNotNil(error)
    }

    func testCanSelectManyRows() async throws {
        let db = try await prepareSampleDB()
        let rows = try await db.prepare("select * from test").fetchAll()
        XCTAssertEqual(
            rows,
            [
                specialRow(i64: 1, f64: 1.0, string: "first", bytes: [0x01]),
                specialRow(i64: 2, f64: 2.0, string: "second", bytes: [0x02]),
                specialRow(i64: 3, f64: 3.0, string: "third", bytes: [0x03]),
            ])
    }

    func specialRow(i64: Int64, f64: Double, string: String, bytes: [UInt8]) -> Row {
        Row(
            columns: ["i64", "f64", "string", "bytes", "nil"],
            values: [.integer(i64), .float(f64), .text(string), .blob(.loaded(Data(bytes))), .null])
    }

    struct SpecialRow: Decodable, Equatable {
        var i64: Int64
        var f64: Double
        var string: String
        var bytes: SQLiteBlob
        var null: SQLNull = SQLNull()

        init(i64: Int64, f64: Double, string: String, bytes: [UInt8]) {
            self.i64 = i64
            self.f64 = f64
            self.string = string
            self.bytes = .loaded(Data(bytes))
        }

        enum CodingKeys: String, CodingKey {
            case i64, f64, string, bytes
            case null = "nil"
        }
    }

    func testCanSelectDecodable() async throws {
        let db = try Database(filename: ":memory:")
        var stmt = try await db.prepare(
            "SELECT 1 as i64, 2.0 as f64, 'text' as string, unhex('42069f') as bytes, null as nil")
        let row: SpecialRow? = try await stmt.step()
        XCTAssertEqual(
            row,
            SpecialRow(i64: 1, f64: 2.0, string: "text", bytes: [0x42, 0x06, 0x9f])
        )
        try await stmt.finalize()
    }

    func testCanSelectManyDecodables() async throws {
        let db = try await prepareSampleDB()
        let rows: [SpecialRow] = try await db.prepare("select * from test").fetchAll()
        XCTAssertEqual(
            rows,
            [
                SpecialRow(i64: 1, f64: 1.0, string: "first", bytes: [0x01]),
                SpecialRow(i64: 2, f64: 2.0, string: "second", bytes: [0x02]),
                SpecialRow(i64: 3, f64: 3.0, string: "third", bytes: [0x03]),
            ])
    }

    func testCanFetchExistingOptional() async throws {
        let db = try await prepareSampleDB()
        let row = try await db.prepare("select * from test").fetchOptional()
        XCTAssertEqual(row, specialRow(i64: 1, f64: 1.0, string: "first", bytes: [0x01]))
    }

    func testCanFetchMissingOptional() async throws {
        let db = try await prepareSampleDB()
        let row = try await db.prepare("select * from test limit 0").fetchOptional()
        XCTAssertEqual(row, nil)
    }

    func testCanFetchExistingDecodableOptional() async throws {
        let db = try await prepareSampleDB()
        let row: SpecialRow? = try await db.prepare("select * from test").fetchOptional()
        XCTAssertEqual(row, SpecialRow(i64: 1, f64: 1.0, string: "first", bytes: [0x01]))
    }

    func testCanFetchMissingDecodableOptional() async throws {
        let db = try await prepareSampleDB()
        let row: SpecialRow? = try await db.prepare("select * from test limit 0").fetchOptional()
        XCTAssertEqual(row, nil)
    }

    func testCanFetchExistingOne() async throws {
        let db = try await prepareSampleDB()
        let row = try await db.prepare("select * from test").fetchOne()
        XCTAssertEqual(row, specialRow(i64: 1, f64: 1.0, string: "first", bytes: [0x01]))
    }

    func testCanFetchMissingOne() async throws {
        let db = try await prepareSampleDB()
        let statement = try await db.prepare("select * from test limit 0")
        try await assertThrows(statement: statement) {
            (statement: consuming PreparedStatement) async throws in
            _ = try await statement.fetchOne()
        }
    }

    func testCanFetchExistingDecodableOne() async throws {
        let db = try await prepareSampleDB()
        let row: SpecialRow = try await db.prepare("select * from test").fetchOne()
        XCTAssertEqual(row, SpecialRow(i64: 1, f64: 1.0, string: "first", bytes: [0x01]))
    }

    func testCanFetchMissingDecodableOne() async throws {
        let db = try await prepareSampleDB()
        let statement = try await db.prepare("select * from test limit 0")
        try await assertThrows(statement: statement) {
            (statement: consuming PreparedStatement) async throws in
            _ = try await statement.fetchOne() as SpecialRow
        }
    }

    func testInsuffiecientlyBoundQueryDoesntExecute() async throws {
        let db = try Database(filename: ":memory:", mode: .readOnly)
        try await assertThrows {
            _ = try await db.prepare("select ?")
        }
    }

    func testOverboundQueryDoesntExecute() async throws {
        let db = try Database(filename: ":memory:", mode: .readOnly)
        let query = BoundQuery(raw: "select 1", bindings: [.integer(5)])
        try await assertThrows {
            _ = try await db.prepare(query)
        }
    }

    func testBoundQueryProperlyRuns() async throws {
        let db = try Database(filename: ":memory:", mode: .readOnly)
        let res: SpecialRow = try await db.prepare(
            """
            select
                \(42) as i64,
                \(2.0) as f64,
                \("text") as string,
                \(SQLiteBlob([0x42, 0x69])) as bytes,
                \(SQLNull()) as nil
            """
        ).fetchOne()
        XCTAssertEqual(res, SpecialRow(i64: 42, f64: 2.0, string: "text", bytes: [0x42, 0x69]))
    }
    
    func testCanInterpolateFragment() async throws {
        let db = try await prepareSampleDB()
        let whereClause: BoundQuery = "where f64 > \(1.0)"
        let res: [Int] = try await db.prepare("select i64 from test \(fragment: whereClause) limit \(1)").fetchAll()
        XCTAssertEqual(res, [2])
    }
    
    func runReturnsCorrectRowid() async throws {
        let db = try await prepareSampleDB()
        let insertionStats = try await db.prepare("insert into test values (42, 6.9, 'text', unhex('0x4269'), null)").run()
        let lastId: Int64 = try await db.prepare("select max(rowid) from test").fetchOne()
        XCTAssertEqual(insertionStats, InsertionStats(
            lastInsertedRowid: lastId, rowsAffected: 1, totalRowsAffected: 1
        ))
    }
}
