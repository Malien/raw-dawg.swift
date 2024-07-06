import Collections

/// PLEASE BE CAREFUL WITH THIS GUY
private struct UnsafeSendableUnmanagedSyncConnection: @unchecked Sendable {
    let conn: UnmanagedSyncConnection

    init(_ conn: UnmanagedSyncConnection) {
        self.conn = conn
    }
}

/// The pool of ``SyncConnection``s. 
///
/// Use this type when the concurrenct access to the database is desired.
///
/// Use ``acquire(block:)`` to borrow the connection for the duration of the call.
/// New connections are created on-demand, up until ``maxPoolSize``. After that, access is queued.
///
/// ```swift
/// let pool = Pool(filename: "db.sqlite")
/// try await pool.acquire { conn in
///     try conn.fetchOne("select * from users where id = 5")
/// }
/// ```
///
/// ### Thread-safety and Concurrency
/// ``Pool`` methods are threadsafe. The ``SyncConnection``s yielded by the ``acquire(block:)`` are not though.
/// Operations issued to different connections are run concurrently, no additional serialization is done, apart from the SQLite's own ones.
public actor Pool {
    // This is just a stack of connections as it doesn't matter the order in which they are returned
    // nonisolated(unsafe) is needed to circumvent the swift 6 concurrency checking constraints: see deinit
    // Every other use should be guarded via freeConnections, hence unguarded uses are screaming UNSAFE
    nonisolated(unsafe) private var UNSAFE_freeConnections: [UnmanagedSyncConnection] = []
    private var freeConnections: [UnmanagedSyncConnection] {
        get { UNSAFE_freeConnections }
        set { UNSAFE_freeConnections = newValue }
    }
    // This should be a FIFO queue probably. Just to be fair to all continuations.
    // Also this could be a unchecked continuation, but let's just be safe.
    private var parkedContinuations:
        Deque<CheckedContinuation<UnsafeSendableUnmanagedSyncConnection, Never>> = []
    private var currentPoolSize: Int = 0

    public let connectionString: String
    public let mode: OpenMode
    public let maxPoolSize: Int

    public static let DEFAULT_MAX_POOL_SIZE = 20

    /// Provide the options with which to open new connections
    ///
    /// - Precondition: `maxPoolSize` must be greater than 0.
    ///
    /// ## See also
    /// - ``SyncConnection/init(filename:mode:)``
    public init(
        filename: String, mode: OpenMode = .readWrite, maxPoolSize: Int = DEFAULT_MAX_POOL_SIZE
    ) {
        assert(maxPoolSize > 0)
        self.connectionString = filename
        self.mode = mode
        self.maxPoolSize = maxPoolSize
    }

    /// Borrow one of the ``SyncConnection``s for the duration of the call.
    ///
    /// New connections are created on-demand, up until ``maxPoolSize``. After that, access is queued.
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
    
    // It is safe to call `conn.close()` from any thread, since the connection is not being shared / used concurently
    deinit {
        var errors = [any Error]()
        for conn in UNSAFE_freeConnections {
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
