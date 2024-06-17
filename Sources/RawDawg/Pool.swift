
public actor Pool {
    var freeConnections: [UnmanagedSyncConnection] = []
    var parkedContinuations: [UnmanagedContinuation] = []

    let connectionString: String
    let mode: OpenMode
    let maxPoolSize: Int

    public static let DEFAULT_MAX_POOL_SIZE = 20

    init(filename: String, mode: OpenMode, maxPoolSize: Int) {
        self.connectionString = filename
        self.mode = mode
        self.maxPoolSize = maxPoolSize
    }

    
}

public struct SyncHandle: ~Copyable {
    
}

@available(*, unavailable)
extension SyncHandle: Sendable {}
