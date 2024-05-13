import XCTest
@testable import RawDawg

final class SQLiteM_swiftTests: XCTestCase {
    func testCanSuccessfullyOpenInMemoryDB() throws {
        _ = try Database(filename: ":memory:")
    }

    func canPrepareStatement() async throws {
        let db = try Database(filename: ":memory:")
        let stmt = try await db.prepare("SELECT 1")
        try await stmt.finalize()
    }
}
