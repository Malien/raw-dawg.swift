import Foundation
import Testing

@testable import RawDawg

@Suite() struct AsyncConnections {
    @Test func canSuccessfullyOpenInMemoryDB() throws {
        _ = try SharedConnection(filename: ":memory:")
    }

    @Test func canPrepareStatement() async throws {
        let db = try SharedConnection(filename: ":memory:", mode: .readOnly)
        let stmt = try await db.prepare("SELECT 1")
        try await stmt.finalize()
    }

    @Test func canSelectInteger() async throws {
        let db = try SharedConnection(filename: ":memory:", mode: .readOnly)
        var stmt = try await db.prepare("SELECT 1")
        let row = try await stmt.step()!
        #expect(row == Row(columns: ["1"], values: [.integer(1)]))
        try await stmt.finalize()
    }

    @Test func canSelectTypedInteger() async throws {
        let db = try SharedConnection(filename: ":memory:", mode: .readOnly)
        let res: Int = try await db.prepare("SELECT 1").fetchOne()
        #expect(res == 1)
    }

    @Test func canSelectIntegerEnum() async throws {
        enum State: Int, Codable {
            case pending = 0
            case completed = 1
            case failed = 2
        }
        let db = try SharedConnection(filename: ":memory:", mode: .readOnly)
        let states: [State] = try await db.prepare(
            """
            with cte(state) as (values (0), (1), (2))
                select state from cte
            """
        ).fetchAll()
        #expect(states == [.pending, .completed, .failed])
    }

    @Test func canSelectStringEnum() async throws {
        enum State: String, Codable {
            case pending, completed, failed
        }
        let db = try SharedConnection(filename: ":memory:", mode: .readOnly)
        let states: [State] = try await db.prepare(
            """
            with cte(state) as (values ('pending'), ('completed'), ('failed'))
                select state from cte
            """
        ).fetchAll()
        #expect(states == [.pending, .completed, .failed])
    }

    @Test func cantDeserializePrimitiveWhenMoreThanOneColumnIsSelected() async throws {
        let db = try SharedConnection(filename: ":memory:", mode: .readOnly)
        let statement = try await db.prepare("select 1, 2")

        await expectThrows(statement: statement) {
            _ = try await $0.fetchOne() as Int
        }
    }

    @Test func canSelectAllKindsOfThings() async throws {
        let db = try SharedConnection(filename: ":memory:")
        var stmt = try await db.prepare(
            "SELECT 1 as i64, 2.0 as f64, 'text' as string, unhex('42069f') as bytes, null as nil")
        let row = try await stmt.step()!
        #expect(row == specialRow(i64: 1, f64: 2.0, string: "text", bytes: [0x42, 0x06, 0x9f]))
        try await stmt.finalize()
    }

    private func prepareSampleDB() async throws -> SharedConnection {
        let db = try SharedConnection(filename: ":memory:")
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

    @Test func canSelectManyRows() async throws {
        let db = try await prepareSampleDB()
        let rows = try await db.prepare("select * from test").fetchAll()
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

    @Test func canSelectDecodable() async throws {
        let db = try SharedConnection(filename: ":memory:")
        var stmt = try await db.prepare(
            "SELECT 1 as i64, 2.0 as f64, 'text' as string, unhex('42069f') as bytes, null as nil")
        let row: SpecialRow? = try await stmt.step()
        #expect(row == SpecialRow(i64: 1, f64: 2.0, string: "text", bytes: [0x42, 0x06, 0x9f]))
        try await stmt.finalize()
    }

    @Test func canSelectManyDecodables() async throws {
        let db = try await prepareSampleDB()
        let rows: [SpecialRow] = try await db.prepare("select * from test").fetchAll()
        #expect(
            rows == [
                SpecialRow(i64: 1, f64: 1.0, string: "first", bytes: [0x01]),
                SpecialRow(i64: 2, f64: 2.0, string: "second", bytes: [0x02]),
                SpecialRow(i64: 3, f64: 3.0, string: "third", bytes: [0x03]),
            ]
        )
    }

    @Test func canFetchExistingOptional() async throws {
        let db = try await prepareSampleDB()
        let row = try await db.prepare("select * from test").fetchOptional()
        #expect(row == specialRow(i64: 1, f64: 1.0, string: "first", bytes: [0x01]))
    }

    @Test func canFetchMissingOptional() async throws {
        let db = try await prepareSampleDB()
        let row = try await db.prepare("select * from test limit 0").fetchOptional()
        #expect(row == nil)
    }

    @Test func canFetchExistingDecodableOptional() async throws {
        let db = try await prepareSampleDB()
        let row: SpecialRow? = try await db.prepare("select * from test").fetchOptional()
        #expect(row == SpecialRow(i64: 1, f64: 1.0, string: "first", bytes: [0x01]))
    }

    @Test func canFetchMissingDecodableOptional() async throws {
        let db = try await prepareSampleDB()
        let row: SpecialRow? = try await db.prepare("select * from test limit 0").fetchOptional()
        #expect(row == nil)
    }

    @Test func canFetchExistingOne() async throws {
        let db = try await prepareSampleDB()
        let row = try await db.prepare("select * from test").fetchOne()
        #expect(row == specialRow(i64: 1, f64: 1.0, string: "first", bytes: [0x01]))
    }

    @Test func testCanFetchMissingOne() async throws {
        let db = try await prepareSampleDB()
        let statement = try await db.prepare("select * from test limit 0")

        await expectThrows(statement: statement) {
            _ = try await $0.fetchOne()
        }
    }

    @Test func canFetchExistingDecodableOne() async throws {
        let db = try await prepareSampleDB()
        let row: SpecialRow = try await db.prepare("select * from test").fetchOne()
        #expect(row == SpecialRow(i64: 1, f64: 1.0, string: "first", bytes: [0x01]))
    }

    @Test func canFetchMissingDecodableOne() async throws {
        let db = try await prepareSampleDB()
        let statement = try await db.prepare("select * from test limit 0")
        await expectThrows(statement: statement, "Shouldn't be able to fetchOne on zero rows") {
            _ = try await $0.fetchOne() as SpecialRow
        }
    }

    @Test func insuffiecientlyBoundQueryDoesntExecute() async throws {
        let db = try SharedConnection(filename: ":memory:", mode: .readOnly)
        await #expect(
            throws: SQLiteError.self,
            "Shouldn't be able to prepare query with insufficient bindings"
        ) {
            _ = try await db.prepare("select ?")
        }
    }

    @Test func overboundQueryDoesntExecute() async throws {
        let db = try SharedConnection(filename: ":memory:", mode: .readOnly)
        let query = BoundQuery(raw: "select 1", bindings: [.integer(5)])
        await #expect(
            throws: SQLiteError.self, "Shouldn't be able to prepare query with too many bindings"
        ) {
            _ = try await db.prepare(query)
        }
    }

    @Test func boundQueryProperlyRuns() async throws {
        let db = try SharedConnection(filename: ":memory:", mode: .readOnly)
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
        #expect(res == SpecialRow(i64: 42, f64: 2.0, string: "text", bytes: [0x42, 0x69]))
    }

    @Test func canInterpolateFragment() async throws {
        let db = try await prepareSampleDB()
        let whereClause: BoundQuery = "where f64 > \(1.0)"
        let res: [Int] = try await db.prepare(
            "select i64 from test \(fragment: whereClause) limit \(1)"
        ).fetchAll()
        #expect(res == [2])
    }

    @Test func runReturnsCorrectRowid() async throws {
        let db = try await prepareSampleDB()
        let insertionStats = try await db.prepare(
            "insert into test values (42, 6.9, 'text', unhex('0x4269'), null)"
        ).run()
        let lastId: Int64 = try await db.prepare("select max(rowid) from test").fetchOne()
        #expect(insertionStats.lastInsertedRowid == lastId)
        #expect(insertionStats.rowsAffected == 1)
    }

    @Test func realWorldUse1() async throws {
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

        let db = try SharedConnection(filename: ":memory:")
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
        #expect(res == input.map { MyResponse(usernames: $0) })
    }

    @Test func willDecodeDates() async throws {
        let db = try SharedConnection(filename: ":memory:", mode: .readOnly)

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

        #expect(
            row
                == DateRow(
                    epochSeconds: Date(timeIntervalSince1970: 1_716_041_456),
                    epochPreciseSeconds: Date(timeIntervalSince1970: 1716041456.069),
                    iso8601: formatter.date(from: "2024-05-18T14:11:35.069Z")!
                ))
    }

    // SQLite's `datetime` functions will return iso8601 strings without "Z" time zone.
    // Swift's Date parsing, understandably, doesn't like dates that don't end on "Z" or "±HH:MM".
    // My driver has to handle these cases
    @Test func willSQLiteDates() async throws {
        let db = try SharedConnection(filename: ":memory:", mode: .readOnly)
        let date: Date = try await db.prepare(
            "select datetime('2024-02-03 08:12:23.032Z', 'subsec')"
        ).fetchOne()

        let formatter = ISO8601DateFormatter()
        formatter.formatOptions.insert(.withFractionalSeconds)
        formatter.timeZone = TimeZone(identifier: "UTC")!

        #expect(date == formatter.date(from: "2024-02-03T08:12:23.032Z")!)
    }

    @Test func fetchOneParameterPackDecode() async throws {
        let db = try SharedConnection(filename: ":memory:", mode: .readOnly)
        let (i64, f64, string, blob, _): (Int, Double, String, SQLiteBlob, SQLNull) =
            try await db.prepare("SELECT 1, 2.0, 'text', unhex('42069f'), null").fetchOne()
        #expect(i64 == 1)
        #expect(f64 == 2.0)
        #expect(string == "text")
        #expect(blob == SQLiteBlob.loaded(Data([0x42, 0x06, 0x9f])))
    }

    @Test func fetchAllParameterPackDecode() async throws {
        let db = try SharedConnection(filename: ":memory:", mode: .readOnly)
        let rows: [(Int, String?)] = try await db.prepare(
            """
            with cte(number, string) as (values (1, 'hello'), (2, null))
            select number, string from cte
            """
        ).fetchAll()
        #expect(rows.count == 2)
        #expect(rows[0] == (1, "hello"))
        #expect(rows[1] == (2, nil))
    }

    @Test func fetchOptionalParameterPackDecode() async throws {
        let db = try SharedConnection(filename: ":memory:", mode: .readOnly)
        let value: (Int, String?)? = try await db.prepare(
            """
            with cte(number, string) as (values (1, 'hello'))
            select number, string from cte
            """
        ).fetchOptional()
        #expect(try #require(value) == (1, "hello"))
    }

    @Test func stepParameterPackDecode() async throws {
        let db = try SharedConnection(filename: ":memory:", mode: .readOnly)
        var statement = try await db.prepare(
            """
            with cte(number, string) as (values (1, 'hello'))
            select number, string from cte
            """)
        let value: (Int, String?)? = try await statement.step()
        try await statement.finalize()
        #expect(try #require(value) == (1, "hello"))
    }

    @Test func compileTestFetchOneIsNotAmbigious() async throws {
        let db = try SharedConnection(filename: ":memory:", mode: .readOnly)
        let _: Int = try await db.prepare("select 1").fetchOne()
    }

    @Test func compileTestFetchOptionalIsNotAmbigious() async throws {
        let db = try SharedConnection(filename: ":memory:", mode: .readOnly)
        let _: Int? = try await db.prepare("select 1").fetchOptional()
    }

    @Test func compileTestStepIsNotAmbigious() async throws {
        let db = try SharedConnection(filename: ":memory:", mode: .readOnly)
        var statement = try await db.prepare("select 1")
        let _: Int? = try await statement.step()
        try await statement.finalize()
    }

    @Test func boolsAreDecodedAsTheyShould() async throws {
        let db = try SharedConnection(filename: ":memory:", mode: .readOnly)
        let oneTrue: Bool = try await db.prepare("select 1").fetchOne()
        #expect(oneTrue)
        let oneFalse: Bool = try await db.prepare("select 0").fetchOne()
        #expect(!oneFalse)
        let inTuple: (Bool, Bool, Bool) = try await db.prepare("select 0, 1, 69").fetchOne()
        #expect(!inTuple.0)
        #expect(inTuple.1)
        #expect(inTuple.2)
    }

    func expectThrows(
        statement: consuming PreparedStatement,
        _ comment: Comment? = nil,
        _ block: (consuming PreparedStatement) async throws -> Void
    ) async {
        do {
            try await block(statement)
            #expect(Bool(false), "Should throw on invalid statement")
        } catch {
            // Hurray, it threw as expected!
        }
    }
}
