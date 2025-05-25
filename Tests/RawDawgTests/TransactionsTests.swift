import Testing
import Foundation
@testable import RawDawg

@Suite struct Transactions {

    @Test func transactionCommitsAndChangesAreVisible() throws {
        var db = try SyncConnection(filename: ":memory:", mode: .readWrite)
        try db.execute("create table t(x)")
        try db.transaction { tx in
            try tx.execute("insert into t values (42), (69)")
            let res: [Int] = try tx.fetchAll("select x from t")
            #expect(res == [42, 69])
        }
        let res: [Int] = try db.fetchAll("select x from t")
        #expect(res == [42, 69])
    }
    
    struct DummyError: Error {}
    
    @Test func rollbackedTransactionWritesArentVisible() throws {
        var db = try SyncConnection(filename: ":memory:", mode: .readWrite)
        try db.execute("create table t(x)")
        do {
            try db.transaction { tx in
                try tx.execute("insert into t values (42), (69)")
                let res: [Int] = try tx.fetchAll("select x from t")
                #expect(res == [42, 69])
                throw DummyError()
            }
        } catch is DummyError {
            // ignore
        }
        let res: [Int] = try db.fetchAll("select x from t")
        #expect(res == [])
    }
    
    @Test func twoInMemoryDBsAreDifferent() throws {
        let dbname = UUID().uuidString + ".rawdawgtest.sqlite"
        let dburl = FileManager.default.temporaryDirectory.appendingPathComponent(dbname)
        var db1 = try SyncConnection(filename: dburl.absoluteString, mode: .readWrite(create: true))
        var db2 = try SyncConnection(filename: dburl.absoluteString, mode: .readWrite(create: true))
        defer { try? FileManager.default.removeItem(at: dburl) }
        
        try db1.execute("create table t(x); insert into t values (42)")
        
        try db1.transaction { tx in
            try tx.execute("insert into t values (69)")
            let res1: [Int] = try tx.fetchAll("select x from t")
            #expect(res1 == [42, 69])
            let res2: [Int] = try db2.fetchAll("select x from t")
            #expect(res2 == [42])
        }
        
        let res1: [Int] = try db2.fetchAll("select x from t")
        #expect(res1 == [42, 69])
        let res2: [Int] = try db2.fetchAll("select x from t")
        #expect(res2 == [42, 69])
    }
}

