# Choosing the right connection type

Having trouble choosing between ``SyncConnection``, ``SharedConnection``, or ``Pool``?

## Overview

Depending on your usecase one of thees may fit your needs better than other:

|     |concurrent| single connection  |
|-----|----------|--------------------|
|async|``Pool``  |``SharedConnection``|
|sync |``Pool``  |``SyncConnection``  |

### ``SyncConnection``
This is the simplest, thinnest connection type. It can be used only by a single thread at a time, hence it is not `Sendable`. 
The resource management is determininstic af. Meaning the type itself is `~Copyable`.
```swift
let db = SyncConnection(filename: "db.sqlite")
let user: User = try db.fetchOne("select * from users")
// This consumed db, and any further use will result in compilation failure
try db.close()
```

This is the only type that permits use of transactions, since the connection object is known not to be shared.
```swift
let db = SyncConnection(filename: "db.sqlite")
try db.transaction { tx in 
    try tx.run("insert into users(name) values ('Alice')")
}
```

The use of this type is best for simple single-threaded (or connection-per-thread) applications and scripts.

### ``SharedConnection``
This type is similar to the ``SyncConnection``, except it synchronized the access to it using swift's actor system. 
Hence, every access to it is guarded with an `await`.

```swift
let db = SharedConnection(filename: "db.sqlite")
let one: Int = try await db.prepare("select 1").fetchOne()
```

Since the connection is shared between multiple executors, there are no transaction support, since executing `begin;` 
on one executor, will put every executor that fires SQL statements into the same transaction.

### ``Pool``
Most concurrent application should utilize this type. This allowes for maximum concurrency. 
Calls to ``Pool/acquire(block:)`` will borrow ``SyncConnection`` for the duration of the block call.

```swift
let pool = Pool(filename: "db.sqlite")

try await pool.acquire { conn in // This is the same SyncConnection as before
    try conn.transaction { tx in 
        try tx.run("insert into users(name) values ('Alice'))
        try tx.fetchOne("select * from users where name = 'Alice'")
    }
}
```

Access to pool is synchronized via the same `async/await` via the same actor primitive as ``SharedConnection``.

Pool will create new connections on-demand, up until ``Pool/maxPoolSize``. It can be set on creation in
``Pool/init(filename:mode:maxPoolSize:)``. If there are more demand than available connections, the access will be queued.
