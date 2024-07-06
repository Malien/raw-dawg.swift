# ``RawDawg``

Yet another SQLite3 Swift driver

## Overview

_raw dawg dat squeel_.

Cause there's nothing wrong with writing raw SQL!

```swift
let db = try SyncConnection(filename: "mydb.sqlite")
try db.execute("""
  create table users (id integer primary key autoincrement, name text not null, age integer);
  insert into users (name, age) values ('Alice', 24), ('Bob', null);
  """)

struct User: Codable {
  var id: Int
  var name: String
  var age: Int?
}

let username = "Alice"
let alice: User = try db.prepare("select id, name, age from users where name = \(username)").fetchOne()

let adults: [User] = try db.prepare("select * from users where age is not null and age > 18").fetchAll()
```

### Checklist:
#### ✅ As close to raw SQL as possible.
Without building leaky ORM abstractions on top of relational model. Plain and simple. No need to learn an additional query mechanism.
```swift
db.fetchAll("""
  select
    products.id as product_id,
    products.name as product_name,
    sum(receipt_items.amount * products.price) as total_price
  from receipt_items
  join products
    on receipt_items.product_id = products.id
  having receipt_items.is_promotional = 0
  group by (products.id, products.name)
  where receipt_items.receipt_id = \(id)
  """)
```
#### ✅ Convenient swift API on top of sqlite3
sqlite3 C's API is quite nice... when it comes to C APIs. 

Swift users deserve better! Using all of the modern Swift tooling to build a delightful experience
#### ✅ Both async, and synchronous APIs available depending on the desired use-case
```swift
let asyncDB = try SharedConection(filename: "db.sqlite")
let syncDB = try SyncConnection(filename: "db.sqlite")
```
[Choosing the right connection type](<doc:Choosing-the-right-connection-type>)
#### ✅ Connection pooling built-in
```swift
let pool = Pool(filename: "db.sqlite")

Task {
    try await pool.acquire { conn in
        processProducts(try conn.fetchAll("select * from products limit 5"))
    }
}

Task {
    try await pool.acquire { conn in
        procssProducts(try conn.fetchAll("select * from products offset 5 limit 5")
    }
}
```
#### ✅ Codable support for easy and convenient row unmarshaling.
Using the built-in familiar way to deserialize values from sqlite into structs. Couldn't be easier.
```swift
struct Post: Codable {
  var title: String
  var contents: String
  var createdAtEpoch: Int
  var starred: Bool
  var cover: String?
  var category: Category

  enum CodingKeys: String, CodingKey {
    case title, contents, createdAtEpoch = "created_at", starred, cover
  }

  enum Category: String, Codable {
    case lifestyle, health, cooking, sqlite
  }
}

let posts: [Post] = try db.fetchAll("""
    select title, contents, created_at, 0 as starred, cover, category
    from posts
    """)
```
This also means you get `RawRepresentable` enum serialization for free. Bot `Int` and `String` ones.

#### ✅ Quick and easy tuple deserialization
Want to quickly extract a couple of values from the database in ad-hoc manner? No worries, there is no longer a need to create a struct just to hold the type-safe result of a query
```swift
let userCount: Int = db.fetchOne("select count(*) from users")

let (id, createdAt): (Int, Date) = db.fetchOne("""
    insert into users (fist_name, last_name)
    values ('John', 'Appleseed')
    returning id, created_at
    """)

let username: (String, String)? = db.fetchOptional(
    "select first_name, last_name from users where id = \(userID)"
)

let produceSoldToday: [(Int, String, Int)] = db.fetchAll("""
    select products.id, products.name, sum(sales.amount * sales.price)
    from sales
    join products on sales.product_id = products.id
    group by products.id, products.name
    having sales.created_at > datetime('now', 'start of day')
    """)
```

#### ✅ No SQL injections.
`"where name = \(username)"` is built on top of Swift's `ExpressibleByStringInterpolation` and safely escapes (binds) arguments instead of interpolating a string.
```swift
func getUser(byID id: Int) throws -> User? {
  try db.fetchOptional("select * from users where id = \(id)")
}

func createUser(withName name: String) throws -> User {
  try db.fetchOne("insert into users (name) values (\(name)) returning *")
}

try createUser(withName: "mark'); drop table users;") // Phew 😮‍💨. Nothing to worry about
```

#### ✅ Convenient APIs for whatever life throws your way
- [X] "Always-at-least-one" fetching via ``PreparedStatement/fetchOne()-4grfr``
- [X] Optionalities built-in via ``PreparedStatement/fetchOptional()-1sp53``
- [X] Fetch everything via ``PreparedStatement/fetchAll()-3h0eg``
- [X] Incremental fetching via ``PreparedStatement/step()-3wy2j``

#### ✅ Transaction support
```swift
let db = SyncConnection(filename: "db.sqlite")

func createProduct(name: String, price: Int, initialQuantity: Int) throws {
    try db.transaction { tx in
        let id: Int = try tx.fetchOne("""
            insert into products (name, price) 
            values (\(name), \(price)) 
            returning id
            """)
        try tx.run("""
            insert into product_stock (product_id, quantity) 
            values (\(id), \(initialQuantity))
            """)
    }
}
```
_Note that transaction are not supported for ``SharedConnection``, since it shares one underlying connection_

#### ✅ Dynamic safe query building
```swift
func findProducts(filter: ProducFilter) throws -> [Product] {
  try db.fetchAll("select * from products \(fragment: filter.whereClause)")
}

enum ProductFilter {
  case withPrice(below: Int)
  case withPrice(above: Int)
  case all

  var whereClause: BoundQuery {
    switch self {
    case .withPrice(below: let threshold): "where price < \(threshold)"
    case .withPrice(above: let threshold): "where price > \(threshold)"
    case .all: ""
    }
  }
}
```
_I might want to remove the `fragment:` label going forwards 🤔_
#### ✅ Dynamic unsafe query building 🚧
```swift
try await db.prepare("select * from \(raw: sqlInjectionGalore)")
```
#### ✅ Deterministic resource management
`struct PreparedStatement: ~Copyable`. This means there is no way to misuse statement with something like
```swift
var statement = try await db.prepare("select 1")
let row = try await statement.step()
try await statement.finalize()
let nextRow = try await statement.step() // Nope!
```

## Topics

### Opening a Database Connection
- <doc:Choosing-the-right-connection-type>
- ``Pool``
- ``SyncConnection``
- ``SharedConnection``
- ``OpenMode``

### Making Queries
- ``BoundQuery``
- ``SQLPrimitiveEncodable``
- ``PreparedStatement``

### Decoding Values
- ``SQLiteValue``
- ``SQLNull``
- ``SQLiteBlob``
- ``Row``
- ``SQLPrimitiveDecodable``
