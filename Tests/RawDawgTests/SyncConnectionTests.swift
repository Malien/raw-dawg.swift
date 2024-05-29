import XCTest

@testable import RawDawg

final class SyncConnection_swiftTests: XCTestCase {
    func testCanSuccessfullyOpenInMemoryDB() throws {
        _ = try SyncConnection(filename: ":memory:")
    }

    func testCanPrepareStatement() throws {
        var db = try SyncConnection(filename: ":memory:", mode: .readOnly)
        try db.preparing("select 1") { _ in
            
        }
    }

    func testCanSelectInteger() throws {
        var db = try SyncConnection(filename: ":memory:", mode: .readOnly)
        let row = try db.preparing("SELECT 1") { stmt in
            try stmt.step()!
        }
        XCTAssertEqual(
            row,
            Row(columns: ["1"], values: [.integer(1)])
        )
    }

    func testCanSelectTypedInteger() throws {
        var db = try SyncConnection(filename: ":memory:", mode: .readOnly)
        let res: Int = try db.fetchOne("SELECT 1")
        XCTAssertEqual(res, 1)
    }

    func testCanSelectIntegerEnum() throws {
        enum State: Int, Codable {
            case pending = 0
            case completed = 1
            case failed = 2
        }
        var db = try SyncConnection(filename: ":memory:", mode: .readOnly)
        let states: [State] = try db.fetchAll(
            """
            with cte(state) as (values (0), (1), (2))
                select state from cte
            """
        )
        XCTAssertEqual(states, [.pending, .completed, .failed])
    }

    func testCanSelectStringEnum() throws {
        enum State: String, Codable {
            case pending, completed, failed
        }
        var db = try SyncConnection(filename: ":memory:", mode: .readOnly)
        let states: [State] = try db.fetchAll(
            """
            with cte(state) as (values ('pending'), ('completed'), ('failed'))
                select state from cte
            """
        )
        XCTAssertEqual(states, [.pending, .completed, .failed])
    }

    func testCantDeserializePrimitiveWhenMoreThanOneColumnIsSelected() throws {
        var db = try SyncConnection(filename: ":memory:", mode: .readOnly)
        XCTAssertThrowsError(
            try db.fetchOne("select 1, 2") as Int
        )
    }

    func testCanSelectAllKindsOfThings() throws {
        var db = try SyncConnection(filename: ":memory:")
        let row = try db.preparing(
            "SELECT 1 as i64, 2.0 as f64, 'text' as string, unhex('42069f') as bytes, null as nil"
        ) { stmt in
            try stmt.step()!
        }
        XCTAssertEqual(
            row,
            specialRow(i64: 1, f64: 2.0, string: "text", bytes: [0x42, 0x06, 0x9f])
        )
    }

    private func prepareSampleDB() throws -> SyncConnection {
        var db = try SyncConnection(filename: ":memory:")
        try db.execute(
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

    func testCanSelectManyRows() throws {
        var db = try prepareSampleDB()
        let rows = try db.fetchAll("select * from test")
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

    func testCanSelectDecodable() throws {
        var db = try SyncConnection(filename: ":memory:")
        let row: SpecialRow? = try db.preparing(
            "SELECT 1 as i64, 2.0 as f64, 'text' as string, unhex('42069f') as bytes, null as nil"
        ) { stmt in
            try stmt.step()
        }
        
        XCTAssertEqual(
            row,
            SpecialRow(i64: 1, f64: 2.0, string: "text", bytes: [0x42, 0x06, 0x9f])
        )
    }

    func testCanSelectManyDecodables() throws {
        var db = try prepareSampleDB()
        let rows: [SpecialRow] = try db.fetchAll("select * from test")
        XCTAssertEqual(
            rows,
            [
                SpecialRow(i64: 1, f64: 1.0, string: "first", bytes: [0x01]),
                SpecialRow(i64: 2, f64: 2.0, string: "second", bytes: [0x02]),
                SpecialRow(i64: 3, f64: 3.0, string: "third", bytes: [0x03]),
            ])
    }

    func testCanFetchExistingOptional() throws {
        var db = try prepareSampleDB()
        let row = try db.fetchOptional("select * from test")
        XCTAssertEqual(row, specialRow(i64: 1, f64: 1.0, string: "first", bytes: [0x01]))
    }

    func testCanFetchMissingOptional() throws {
        var db = try prepareSampleDB()
        let row = try db.fetchOptional("select * from test limit 0")
        XCTAssertEqual(row, nil)
    }

    func testCanFetchExistingDecodableOptional() throws {
        var db = try prepareSampleDB()
        let row: SpecialRow? = try db.fetchOptional("select * from test")
        XCTAssertEqual(row, SpecialRow(i64: 1, f64: 1.0, string: "first", bytes: [0x01]))
    }

    func testCanFetchMissingDecodableOptional() throws {
        var db = try prepareSampleDB()
        let row: SpecialRow? = try db.fetchOptional("select * from test limit 0")
        XCTAssertEqual(row, nil)
    }

    func testCanFetchExistingOne() throws {
        var db = try prepareSampleDB()
        let row = try db.fetchOne("select * from test")
        XCTAssertEqual(row, specialRow(i64: 1, f64: 1.0, string: "first", bytes: [0x01]))
    }

    func testCanFetchMissingOne() throws {
        var db = try prepareSampleDB()
        XCTAssertThrowsError(try db.fetchOne("select * from test limit 0"))
    }

    func testCanFetchExistingDecodableOne() throws {
        var db = try prepareSampleDB()
        let row: SpecialRow = try db.fetchOne("select * from test")
        XCTAssertEqual(row, SpecialRow(i64: 1, f64: 1.0, string: "first", bytes: [0x01]))
    }

    func testCanFetchMissingDecodableOne() throws {
        var db = try prepareSampleDB()
        XCTAssertThrowsError(try db.fetchOne("select * from test limit 0") as SpecialRow)
    }

    func testInsuffiecientlyBoundQueryDoesntExecute() throws {
        var db = try SyncConnection(filename: ":memory:", mode: .readOnly)
        XCTAssertThrowsError(try db.preparing("select ?") { _ in })
    }

    func testOverboundQueryDoesntExecute() throws {
        var db = try SyncConnection(filename: ":memory:", mode: .readOnly)
        let query = BoundQuery(raw: "select 1", bindings: [.integer(5)])
        XCTAssertThrowsError(try db.preparing(query) { _ in })
    }

    func testBoundQueryProperlyRuns() throws {
        var db = try SyncConnection(filename: ":memory:", mode: .readOnly)
        let res: SpecialRow = try db.fetchOne(
            """
            select
                \(42) as i64,
                \(2.0) as f64,
                \("text") as string,
                \(SQLiteBlob([0x42, 0x69])) as bytes,
                \(SQLNull()) as nil
            """
        )
        XCTAssertEqual(res, SpecialRow(i64: 42, f64: 2.0, string: "text", bytes: [0x42, 0x69]))
    }

    func testCanInterpolateFragment() throws {
        var db = try prepareSampleDB()
        let whereClause: BoundQuery = "where f64 > \(1.0)"
        let res: [Int] = try db.fetchAll(
            "select i64 from test \(fragment: whereClause) limit \(1)"
        )
        XCTAssertEqual(res, [2])
    }

    func runReturnsCorrectRowid() throws {
        var db = try prepareSampleDB()
        let insertionStats = try db.run(
            "insert into test values (42, 6.9, 'text', unhex('0x4269'), null)"
        )
        let lastId: Int64 = try db.fetchOne("select max(rowid) from test")
        XCTAssertEqual(
            insertionStats,
            InsertionStats(
                lastInsertedRowid: lastId, rowsAffected: 1, totalRowsAffected: 1
            ))
    }

    func testRealWorldUse1() throws {
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

        var db = try SyncConnection(filename: ":memory:")
        try db.execute(
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

        let res: [MyResponse] = try db.fetchAll("select first_name as usernames from users")
        XCTAssertEqual(res, input.map { MyResponse(usernames: $0) })
    }

    func testWillDecodeDates() throws {
        var db = try SyncConnection(filename: ":memory:", mode: .readOnly)

        struct DateRow: Equatable, Codable {
            var epochSeconds: Date
            var epochPreciseSeconds: Date
            var iso8601: Date
        }

        let row: DateRow = try db.fetchOne(
            """
            with cte(epochSeconds, epochPreciseSeconds, iso8601) as
                (values (1716041456, 1716041456.069, '2024-05-18T14:11:35.069Z'))
            select * from cte
            """
        )

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
    func testWillSQLiteDates() throws {
        var db = try SyncConnection(filename: ":memory:", mode: .readOnly)
        let date: Date = try db.fetchOne(
            "select datetime('2024-02-03 08:12:23.032Z', 'subsec')"
        )

        let formatter = ISO8601DateFormatter()
        formatter.formatOptions.insert(.withFractionalSeconds)
        formatter.timeZone = TimeZone(identifier: "UTC")!

        XCTAssertEqual(date, formatter.date(from: "2024-02-03T08:12:23.032Z"))
    }

    func testFetchOneParameterPackDecode() throws {
        var db = try SyncConnection(filename: ":memory:", mode: .readOnly)
        let (i64, f64, string, blob, _): (Int, Double, String, SQLiteBlob, SQLNull) =
            try db.fetchOne("SELECT 1, 2.0, 'text', unhex('42069f'), null")
        XCTAssertEqual(i64, 1)
        XCTAssertEqual(f64, 2.0)
        XCTAssertEqual(string, "text")
        XCTAssertEqual(blob, SQLiteBlob.loaded(Data([0x42, 0x06, 0x9f])))
    }

    func testFetchAllParameterPackDecode() throws {
        var db = try SyncConnection(filename: ":memory:", mode: .readOnly)
        let rows: [(Int, String?)] = try db.fetchAll(
            """
            with cte(number, string) as (values (1, 'hello'), (2, null))
            select number, string from cte
            """
        )
        XCTAssertEqual(rows[0].0, 1)
        XCTAssertEqual(rows[0].1, "hello")
        XCTAssertEqual(rows[1].0, 2)
        XCTAssertEqual(rows[1].1, nil)
    }

    func testFetchOptionalParameterPackDecode() throws {
        var db = try SyncConnection(filename: ":memory:", mode: .readOnly)
        let value: (Int, String?)? = try db.fetchOptional(
            """
            with cte(number, string) as (values (1, 'hello'))
            select number, string from cte
            """
        )
        XCTAssertNotNil(value)
        XCTAssertEqual(value?.0, 1)
        XCTAssertEqual(value?.1, "hello")
    }

    func testStepParameterPackDecode() throws {
        var db = try SyncConnection(filename: ":memory:", mode: .readOnly)
        let value: (Int, String?)? = try db.preparing(
            """
            with cte(number, string) as (values (1, 'hello'))
            select number, string from cte
            """
        ) { statement in
            try statement.step()
        }
        XCTAssertNotNil(value)
        XCTAssertEqual(value?.0, 1)
        XCTAssertEqual(value?.1, "hello")
    }

    func compileTestFetchOneIsNotAmbigious() throws {
        var db = try SyncConnection(filename: ":memory:", mode: .readOnly)
        let _: Int = try db.fetchOne("select 1")
    }

    func compileTestFetchOptionalIsNotAmbigious() throws {
        var db = try SyncConnection(filename: ":memory:", mode: .readOnly)
        let _: Int? = try db.fetchOptional("select 1")
    }

    func compileTestStepIsNotAmbigious() throws {
        var db = try SyncConnection(filename: ":memory:", mode: .readOnly)
        let _: Int? = try db.preparing("select 1") { statement in
            try statement.step()
        }
    }
    
    func testBoolsAreDecodedAsTheyShould() throws {
        var db = try SyncConnection(filename: ":memory:", mode: .readOnly)
        let oneTrue: Bool = try db.fetchOne("select 1")
        XCTAssertTrue(oneTrue)
        let oneFalse: Bool = try db.fetchOne("select 0")
        XCTAssertFalse(oneFalse)
        let inTuple: (Bool, Bool, Bool) = try db.fetchOne("select 0, 1, 69")
        XCTAssertFalse(inTuple.0)
        XCTAssertTrue(inTuple.1)
        XCTAssertTrue(inTuple.2)
    }
}
