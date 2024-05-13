import XCTest
@testable import RawDawg

final class SQLiteM_swiftTests: XCTestCase {
    func testCanSuccessfullyOpenInMemoryDB() throws {
        _ = try Database(filename: ":memory:")
    }

    func testCanPrepareStatement() async throws {
        let db = try Database(filename: ":memory:")
        let stmt = try await db.prepare("SELECT 1")
        try await stmt.finalize()
    }
    
    func testCanSelectInteger() async throws {
        let db = try Database(filename: ":memory:")
        var stmt = try await db.prepare("SELECT 1")
        let row = try await stmt.step()!
        XCTAssertEqual(
            row,
            Row(columns: ["1"], values: [.integer(1)])
        )
        try await stmt.finalize()
    }
    
    func testCanSelectAllKindsOfThings() async throws {
        let db = try Database(filename: ":memory:")
        var stmt = try await db.prepare("SELECT 1 as i64, 2.0 as f64, 'text' as string, unhex('42069f') as bytes, null as nil")
        let row = try await stmt.step()!
        XCTAssertEqual(
            row,
            specialRow(i64: 1, f64: 2.0, string: "text", bytes: [0x42, 0x06, 0x9f])
        )
        try await stmt.finalize()
    }
    
    func testCanSelectManyRows() async throws {
        let db = try Database(filename: ":memory:")
        try await db.execute("""
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
        let rows = try await db.prepare("select * from test").collect()
        XCTAssertEqual(rows, [
            specialRow(i64: 1, f64: 1.0, string: "first", bytes: [0x01]),
            specialRow(i64: 2, f64: 2.0, string: "second", bytes: [0x02]),
            specialRow(i64: 3, f64: 3.0, string: "third", bytes: [0x03]),
        ])
    }
    
    func specialRow(i64: Int64, f64: Double, string: String, bytes: [UInt8]) -> Row {
        Row(columns: ["i64", "f64", "string", "bytes", "nil"],
            values: [.integer(i64), .float(f64), .text(string), .blob(.loaded(Data(bytes))), .null])
    }
}
