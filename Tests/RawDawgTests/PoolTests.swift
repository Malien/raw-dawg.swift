import XCTest

@testable import RawDawg

final class Pool_swiftTests: XCTestSuite {

    func testAquire() async throws {
        let pool = Pool(filename: ":memory:")
        let result: Int = try await pool.acquire { conn in 
            try conn.fetchOne("select 1")
        }
        XCTAssertEqual(1, result)
    }
}
