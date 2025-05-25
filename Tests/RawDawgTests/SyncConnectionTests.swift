import Testing
import Foundation

@testable import RawDawg

@Suite struct SyncConnectionTests {
    @Test func canSuccessfullyOpenInMemoryDB() throws {
        _ = try SyncConnection(filename: ":memory:")
    }

    @Test func canPrepareStatement() throws {
        var db = try SyncConnection(filename: ":memory:", mode: .readOnly)
        try db.preparing("select 1") { _ in

        }
    }

    @Test func canSelectInteger() throws {
        var db = try SyncConnection(filename: ":memory:", mode: .readOnly)
        let row = try db.preparing("SELECT 1") { stmt in
            try stmt.step()!
        }
        #expect(row == Row(columns: ["1"], values: [.integer(1)]))
    }

    @Test func canSelectTypedInteger() throws {
        var db = try SyncConnection(filename: ":memory:", mode: .readOnly)
        let res: Int = try db.fetchOne("SELECT 1")
        #expect(res == 1)
    }

    @Test func canSelectIntegerEnum() throws {
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
        #expect(states == [.pending, .completed, .failed])
    }

    @Test func canSelectStringEnum() throws {
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
        #expect(states == [.pending, .completed, .failed])
    }

    @Test func cantDeserializePrimitiveWhenMoreThanOneColumnIsSelected() throws {
        var db = try SyncConnection(filename: ":memory:", mode: .readOnly)
        #expect(throws: SQLiteError.self) {
            try db.fetchOne("select 1, 2") as Int
        }
    }

    @Test func canSelectAllKindsOfThings() throws {
        var db = try SyncConnection(filename: ":memory:")
        let row = try db.preparing(
            "SELECT 1 as i64, 2.0 as f64, 'text' as string, unhex('42069f') as bytes, null as nil"
        ) { stmt in
            try stmt.step()!
        }
        #expect(row == specialRow(i64: 1, f64: 2.0, string: "text", bytes: [0x42, 0x06, 0x9f]))
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

    @Test func canSelectManyRows() throws {
        var db = try prepareSampleDB()
        let rows = try db.fetchAll("select * from test")
        #expect(
            rows == [
                specialRow(i64: 1, f64: 1.0, string: "first", bytes: [0x01]),
                specialRow(i64: 2, f64: 2.0, string: "second", bytes: [0x02]),
                specialRow(i64: 3, f64: 3.0, string: "third", bytes: [0x03]),
            ]
        )
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

    @Test func canSelectDecodable() throws {
        var db = try SyncConnection(filename: ":memory:")
        let row: SpecialRow? = try db.preparing(
            "SELECT 1 as i64, 2.0 as f64, 'text' as string, unhex('42069f') as bytes, null as nil"
        ) { stmt in
            try stmt.step()
        }

        #expect(row == SpecialRow(i64: 1, f64: 2.0, string: "text", bytes: [0x42, 0x06, 0x9f]))
    }

    @Test func canSelectManyDecodables() throws {
        var db = try prepareSampleDB()
        let rows: [SpecialRow] = try db.fetchAll("select * from test")
        #expect(
            rows == [
                SpecialRow(i64: 1, f64: 1.0, string: "first", bytes: [0x01]),
                SpecialRow(i64: 2, f64: 2.0, string: "second", bytes: [0x02]),
                SpecialRow(i64: 3, f64: 3.0, string: "third", bytes: [0x03]),
            ]
        )
    }

    @Test func canFetchExistingOptional() throws {
        var db = try prepareSampleDB()
        let row = try db.fetchOptional("select * from test")
        #expect(row == specialRow(i64: 1, f64: 1.0, string: "first", bytes: [0x01]))
    }

    @Test func canFetchMissingOptional() throws {
        var db = try prepareSampleDB()
        let row = try db.fetchOptional("select * from test limit 0")
        #expect(row == nil)
    }

    @Test func canFetchExistingDecodableOptional() throws {
        var db = try prepareSampleDB()
        let row: SpecialRow? = try db.fetchOptional("select * from test")
        #expect(row == SpecialRow(i64: 1, f64: 1.0, string: "first", bytes: [0x01]))
    }

    @Test func canFetchMissingDecodableOptional() throws {
        var db = try prepareSampleDB()
        let row: SpecialRow? = try db.fetchOptional("select * from test limit 0")
        #expect(row == nil)
    }

    @Test func canFetchExistingOne() throws {
        var db = try prepareSampleDB()
        let row = try db.fetchOne("select * from test")
        #expect(row == specialRow(i64: 1, f64: 1.0, string: "first", bytes: [0x01]))
    }

    @Test func canFetchMissingOne() throws {
        var db = try prepareSampleDB()
        #expect(throws: SQLiteError.self) {
            try db.fetchOne("select * from test limit 0")
        }
    }

    @Test func canFetchExistingDecodableOne() throws {
        var db = try prepareSampleDB()
        let row: SpecialRow = try db.fetchOne("select * from test")
        #expect(row == SpecialRow(i64: 1, f64: 1.0, string: "first", bytes: [0x01]))
    }

    @Test func canFetchMissingDecodableOne() throws {
        var db = try prepareSampleDB()
        #expect(throws: SQLiteError.self) {
            try db.fetchOne("select * from test limit 0") as SpecialRow
        }
    }

    @Test func insuffiecientlyBoundQueryDoesntExecute() throws {
        var db = try SyncConnection(filename: ":memory:", mode: .readOnly)
        #expect(throws: SQLiteError.self) {
            try db.preparing("select ?") { _ in }
        }
    }

    @Test func overboundQueryDoesntExecute() throws {
        var db = try SyncConnection(filename: ":memory:", mode: .readOnly)
        let query = BoundQuery(raw: "select 1", bindings: [.integer(5)])
        #expect(throws: SQLiteError.self) {
            try db.preparing(query) { _ in }
        }
    }

    @Test func boundQueryProperlyRuns() throws {
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
        #expect(res == SpecialRow(i64: 42, f64: 2.0, string: "text", bytes: [0x42, 0x69]))
    }

    @Test func canInterpolateFragment() throws {
        var db = try prepareSampleDB()
        let whereClause: BoundQuery = "where f64 > \(1.0)"
        let res: [Int] = try db.fetchAll(
            "select i64 from test \(fragment: whereClause) limit \(1)"
        )
        #expect(res == [2])
    }

    func runReturnsCorrectRowid() throws {
        var db = try prepareSampleDB()
        let insertionStats = try db.run(
            "insert into test values (42, 6.9, 'text', unhex('0x4269'), null)"
        )
        let lastId: Int64 = try db.fetchOne("select max(rowid) from test")
        #expect(lastId == insertionStats.lastInsertedRowid)
        #expect(insertionStats.rowsAffected == 1)
    }

    @Test func realWorldUse1() throws {
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
        #expect(res == input.map { MyResponse(usernames: $0) })
    }

    @Test func willDecodeDates() throws {
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

        #expect(
            row ==
            DateRow(
                epochSeconds: Date(timeIntervalSince1970: 1_716_041_456),
                epochPreciseSeconds: Date(timeIntervalSince1970: 1716041456.069),
                iso8601: formatter.date(from: "2024-05-18T14:11:35.069Z")!
            ))
    }

    // SQLite's `datetime` functions will return iso8601 strings without "Z" time zone.
    // Swift's Date parsing, understandably, doesn't like dates that don't end on "Z" or "±HH:MM".
    // My driver has to handle these cases
    @Test func willSQLiteDates() throws {
        var db = try SyncConnection(filename: ":memory:", mode: .readOnly)
        let date: Date = try db.fetchOne(
            "select datetime('2024-02-03 08:12:23.032Z', 'subsec')"
        )

        let formatter = ISO8601DateFormatter()
        formatter.formatOptions.insert(.withFractionalSeconds)
        formatter.timeZone = TimeZone(identifier: "UTC")!

        #expect(date == formatter.date(from: "2024-02-03T08:12:23.032Z"))
    }

    @Test func fetchOneParameterPackDecode() throws {
        var db = try SyncConnection(filename: ":memory:", mode: .readOnly)
        let (i64, f64, string, blob, _): (Int, Double, String, SQLiteBlob, SQLNull) =
            try db.fetchOne("SELECT 1, 2.0, 'text', unhex('42069f'), null")
        #expect(i64 == 1)
        #expect(f64 == 2.0)
        #expect(string == "text")
        #expect(blob == SQLiteBlob.loaded(Data([0x42, 0x06, 0x9f])))
    }

    @Test func fetchAllParameterPackDecode() throws {
        var db = try SyncConnection(filename: ":memory:", mode: .readOnly)
        let rows: [(Int, String?)] = try db.fetchAll(
            """
            with cte(number, string) as (values (1, 'hello'), (2, null))
            select number, string from cte
            """
        )
        #expect(rows.count == 2)
        #expect(rows[0] == (1, "hello"))
        #expect(rows[1] == (2, nil))
    }

    @Test func fetchOptionalParameterPackDecode() throws {
        var db = try SyncConnection(filename: ":memory:", mode: .readOnly)
        let value: (Int, String?)? = try db.fetchOptional(
            """
            with cte(number, string) as (values (1, 'hello'))
            select number, string from cte
            """
        )
        #expect(try #require(value) == (1, "hello"))
    }

    @Test func stepParameterPackDecode() throws {
        var db = try SyncConnection(filename: ":memory:", mode: .readOnly)
        let value: (Int, String?)? = try db.preparing(
            """
            with cte(number, string) as (values (1, 'hello'))
            select number, string from cte
            """
        ) { statement in
            try statement.step()
        }
        #expect(try #require(value) == (1, "hello"))
    }

    @Test func compileTestFetchOneIsNotAmbigious() throws {
        var db = try SyncConnection(filename: ":memory:", mode: .readOnly)
        let _: Int = try db.fetchOne("select 1")
    }

    @Test func compileTestFetchOptionalIsNotAmbigious() throws {
        var db = try SyncConnection(filename: ":memory:", mode: .readOnly)
        let _: Int? = try db.fetchOptional("select 1")
    }

    @Test func compileTestStepIsNotAmbigious() throws {
        var db = try SyncConnection(filename: ":memory:", mode: .readOnly)
        let _: Int? = try db.preparing("select 1") { statement in
            try statement.step()
        }
    }

    @Test func boolsAreDecodedAsTheyShould() throws {
        var db = try SyncConnection(filename: ":memory:", mode: .readOnly)
        let oneTrue: Bool = try db.fetchOne("select 1")
        #expect(oneTrue)
        let oneFalse: Bool = try db.fetchOne("select 0")
        #expect(!oneFalse)
        let inTuple: (Bool, Bool, Bool) = try db.fetchOne("select 0, 1, 69")
        #expect(inTuple == (false, true, true))
    }
}
