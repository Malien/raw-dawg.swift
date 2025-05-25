import Testing

@testable import RawDawg

@Test("Pool.acquire(_:)") func poolAquire() async throws {
    let pool = Pool(filename: ":memory:")
    let result: Int = try await pool.acquire { conn in 
        try conn.fetchOne("select 1")
    }
    #expect(result == 1)
}
