import Collections

/// PLEASE BE CAREFUL WITH THIS GUY
private struct UnsafeSendableUnmanagedSyncConnection: @unchecked Sendable {
    let conn: UnmanagedSyncConnection

    init(_ conn: UnmanagedSyncConnection) {
        self.conn = conn
    }
}

public actor Pool {
    // This is just a stack of connections as it doesn't matter the order in which they are returned
    private var freeConnections: [UnmanagedSyncConnection] = []
    // This should be a FIFO queue probably. Just to be fair to all continuations.
    // Also this could be a unchecked continuation, but let's just be safe.
    private var parkedContinuations:
        Deque<CheckedContinuation<UnsafeSendableUnmanagedSyncConnection, Never>> = []
    private var currentPoolSize: Int = 0

    public let connectionString: String
    public let mode: OpenMode
    public let maxPoolSize: Int

    public static let DEFAULT_MAX_POOL_SIZE = 20

    /// - Precondition: `maxPoolSize` must be greater than 0.
    public init(
        filename: String, mode: OpenMode = .readWrite, maxPoolSize: Int = DEFAULT_MAX_POOL_SIZE
    ) {
        assert(maxPoolSize > 0)
        self.connectionString = filename
        self.mode = mode
        self.maxPoolSize = maxPoolSize
    }

    public nonisolated func acquire<T>(block: (inout SyncConnection) async throws -> T) async throws
        -> T
    {
        // SAFETY: We don't share the underlying connection. It is "moved" to the callee.
        let unmanagedConn = try await acquireUnmanaged()
        var conn = SyncConnection(unsafeFromUnmanaged: unmanagedConn.conn)
        // async defer when??
        do {
            let result = try await block(&conn)
            // SAFETY: We don't share the underlying connection. It is "moved" to the caller.
            await release(
                connection: UnsafeSendableUnmanagedSyncConnection(conn.unsafeReleaseUnmanaged()))
            return result
        } catch {
            // SAFETY: We don't share the underlying connection. It is "moved" to the caller.
            await release(
                connection: UnsafeSendableUnmanagedSyncConnection(conn.unsafeReleaseUnmanaged()))
            throw error
        }
    }

    private func acquireUnmanaged() async throws -> UnsafeSendableUnmanagedSyncConnection {
        assert(currentPoolSize <= maxPoolSize)
        assert(freeConnections.count <= currentPoolSize)

        if let unmanagedConn = freeConnections.popLast() {
            // SAFETY: We don't share the underlying connection. It is "moved" to the callee.
            return UnsafeSendableUnmanagedSyncConnection(unmanagedConn)
        } else if currentPoolSize < maxPoolSize {
            log.trace(
                "Creating a new connection to \(connectionString)",
                metadata: [
                    "mode": "\(mode)", "currentPoolSize": "\(currentPoolSize)",
                    "maxPoolSize": "\(maxPoolSize)",
                ]
            )
            currentPoolSize += 1
            // SAFETY: We don't share the underlying connection. It is "moved" to the callee.
            return try UnsafeSendableUnmanagedSyncConnection(
                UnmanagedSyncConnection(filename: connectionString, mode: mode))
        } else {
            return await withCheckedContinuation {
                (continuation: CheckedContinuation<UnsafeSendableUnmanagedSyncConnection, Never>) in
                parkedContinuations.append(continuation)
            }
        }
    }

    private func release(connection: /* sending */ UnsafeSendableUnmanagedSyncConnection) {
        assert(currentPoolSize <= maxPoolSize)
        assert(freeConnections.count < currentPoolSize)

        if let continuation = parkedContinuations.popFirst() {
            // SAFETY: We don't share the underlying connection. It is "moved" into the continuation.
            continuation.resume(returning: connection)
        } else {
            freeConnections.append(connection.conn)
        }
    }

    deinit {
        var errors = [any Error]()
        for conn in freeConnections {
            do {
                try conn.close()
            } catch {
                errors.append(error)
            }
        }
        if !errors.isEmpty {
            log.error("Failed to close some connections", metadata: ["errors": "\(errors)"])
        }
    }
}
