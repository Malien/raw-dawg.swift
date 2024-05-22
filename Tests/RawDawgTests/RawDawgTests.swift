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
        let res: [Int] = try await db.prepare(
            "select i64 from test \(fragment: whereClause) limit \(1)"
        ).fetchAll()
        XCTAssertEqual(res, [2])
    }

    func runReturnsCorrectRowid() async throws {
        let db = try await prepareSampleDB()
        let insertionStats = try await db.prepare(
            "insert into test values (42, 6.9, 'text', unhex('0x4269'), null)"
        ).run()
        let lastId: Int64 = try await db.prepare("select max(rowid) from test").fetchOne()
        XCTAssertEqual(
            insertionStats,
            InsertionStats(
                lastInsertedRowid: lastId, rowsAffected: 1, totalRowsAffected: 1
            ))
    }

    func testRealWorldUse1() async throws {
        let input = [
            "Aaran", "Aaren", "Aarez", "Aarman", "Aaron", "Aaron-James", "Aarron", "Aaryan",
            "Aaryn", "Aayan", "Aazaan", "Abaan", "Abbas", "Abdallah", "Abdalroof", "Abdihakim",
            "Abdirahman", "Abdisalam", "Abdul", "Abdul-Aziz", "Abdulbasir", "Abdulkadir",
            "Abdulkarem", "Abdulkhader", "Abdullah", "Abdul-Majeed", "Abdulmalik", "Abdul-Rehman",
            "Abdur", "Abdurraheem", "Abdur-Rahman", "Abdur-Rehmaan", "Abel", "Abhinav",
            "Abhisumant", "Abid", "Abir", "Abraham", "Abu", "Abubakar", "Ace", "Adain", "Adam",
            "Adam-James", "Addison", "Addisson", "Adegbola", "Adegbolahan", "Aden", "Adenn", "Adie",
            "Adil", "Aditya", "Adnan", "foo",
        ]

        let valuesClause = "(" + input.map { "'\($0)'" }.joined(separator: "), (") + ")"

        let db = try Database(filename: ":memory:")
        try await db.execute(
            """
            create table if not exists migrations (
                idx integer not null,
                applied_at text not null
            );
            begin;
            create table bazinga (
                id integer primary key autoincrement,
                name text not null
            );
            create table users(first_name text);
            insert into migrations (idx, applied_at) values (1, datetime());
            commit;
            insert into users(first_name) values \(valuesClause)
            """)

        struct MyResponse: Codable, Equatable {
            var usernames: String
        }

        let res: [MyResponse] = try await db.prepare("select first_name as usernames from users")
            .fetchAll()
        XCTAssertEqual(res, input.map { MyResponse(usernames: $0) })
    }

    func testWillDecodeDates() async throws {
        let db = try Database(filename: ":memory:", mode: .readOnly)

        struct DateRow: Equatable, Codable {
            var epochSeconds: Date
            var epochPreciseSeconds: Date
            var iso8601: Date
        }

        let row: DateRow = try await db.prepare(
            """
            with cte(epochSeconds, epochPreciseSeconds, iso8601) as
                (values (1716041456, 1716041456.069, '2024-05-18T14:11:35.069Z'))
            select * from cte
            """
        ).fetchOne()

        let formatter = ISO8601DateFormatter()
        formatter.formatOptions.insert(.withFractionalSeconds)

        XCTAssertEqual(
            row,
            DateRow(
                epochSeconds: Date(timeIntervalSince1970: 1_716_041_456),
                epochPreciseSeconds: Date(timeIntervalSince1970: 1716041456.069),
                iso8601: formatter.date(from: "2024-05-18T14:11:35.069Z")!
            ))
    }

    // SQLite's `datetime` functions will return iso8601 strings without "Z" time zone.
    // Swift's Date parsing, understandably, doesn't like dates that don't end on "Z" or "±HH:MM".
    // My driver has to handle these cases
    func testWillSQLiteDates() async throws {
        let db = try Database(filename: ":memory:", mode: .readOnly)
        let date: Date = try await db.prepare(
            "select datetime('2024-02-03 08:12:23.032Z', 'subsec')"
        ).fetchOne()

        let formatter = ISO8601DateFormatter()
        formatter.formatOptions.insert(.withFractionalSeconds)
        formatter.timeZone = TimeZone(identifier: "UTC")!

        XCTAssertEqual(date, formatter.date(from: "2024-02-03T08:12:23.032Z"))
    }

    func testFetchOneParameterPackDecode() async throws {
        let db = try Database(filename: ":memory:", mode: .readOnly)
        let (i64, f64, string, blob, _): (Int, Double, String, SQLiteBlob, SQLNull) =
            try await db.prepare("SELECT 1, 2.0, 'text', unhex('42069f'), null").fetchOne()
        XCTAssertEqual(i64, 1)
        XCTAssertEqual(f64, 2.0)
        XCTAssertEqual(string, "text")
        XCTAssertEqual(blob, SQLiteBlob.loaded(Data([0x42, 0x06, 0x9f])))
    }

    func testFetchAllParameterPackDecode() async throws {
        let db = try Database(filename: ":memory:", mode: .readOnly)
        let rows: [(Int, String?)] = try await db.prepare(
            """
            with cte(number, string) as (values (1, 'hello'), (2, null))
            select number, string from cte
            """
        ).fetchAll()
        XCTAssertEqual(rows[0].0, 1)
        XCTAssertEqual(rows[0].1, "hello")
        XCTAssertEqual(rows[1].0, 2)
        XCTAssertEqual(rows[1].1, nil)
    }

    func testFetchOptionalParameterPackDecode() async throws {
        let db = try Database(filename: ":memory:", mode: .readOnly)
        let value: (Int, String?)? = try await db.prepare(
            """
            with cte(number, string) as (values (1, 'hello'))
            select number, string from cte
            """
        ).fetchOptional()
        XCTAssertNotNil(value)
        XCTAssertEqual(value?.0, 1)
        XCTAssertEqual(value?.1, "hello")
    }

    func testStepParameterPackDecode() async throws {
        let db = try Database(filename: ":memory:", mode: .readOnly)
        var statement = try await db.prepare(
            """
            with cte(number, string) as (values (1, 'hello'))
            select number, string from cte
            """)
        let value: (Int, String?)? = try await statement.step()
        try await statement.finalize()
        XCTAssertNotNil(value)
        XCTAssertEqual(value?.0, 1)
        XCTAssertEqual(value?.1, "hello")
    }

    func compileTestFetchOneIsNotAmbigious() async throws {
        let db = try Database(filename: ":memory:", mode: .readOnly)
        let _: Int = try await db.prepare("select 1").fetchOne()
    }

    func compileTestFetchOptionalIsNotAmbigious() async throws {
        let db = try Database(filename: ":memory:", mode: .readOnly)
        let _: Int? = try await db.prepare("select 1").fetchOptional()
    }

    func compileTestStepIsNotAmbigious() async throws {
        let db = try Database(filename: ":memory:", mode: .readOnly)
        var statement = try await db.prepare("select 1")
        let _: Int? = try await statement.step()
        try await statement.finalize()
    }
    
    func testBoolsAreDecodedAsTheyShould() async throws {
        let db = try Database(filename: ":memory:", mode: .readOnly)
        let oneTrue: Bool = try await db.prepare("select 1").fetchOne()
        XCTAssertTrue(oneTrue)
        let oneFalse: Bool = try await db.prepare("select 0").fetchOne()
        XCTAssertFalse(oneFalse)
        let inTuple: (Bool, Bool, Bool) = try await db.prepare("select 0, 1, 69").fetchOne()
        XCTAssertFalse(inTuple.0)
        XCTAssertTrue(inTuple.1)
        XCTAssertTrue(inTuple.2)
    }
}
