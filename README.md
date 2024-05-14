# raw-dawg.swift
_raw dawg dat squeel_.

Cause there's nothing wrong with writing raw SQL

### Package isn't quite ready yet
- [X] Parametrized updates/inserts via `db.prepare`
- [X] SPM 0.0.1 tag
- [ ] Transaction support
- [ ] Blob streaming
- [ ] Custom `AsyncRowDecodable` protocol based deserialization
- [ ] `@AsyncRowDecodable` macro to conform automagically
- [ ] Additional dynamic query building capabilities(?)
- [ ] Pooling
- [ ] Date-time support
- [ ] URL support
- [ ] SharedStatement
- [ ] Stabilize API / Usability testing
- [ ] SPM 1.0.0 tag

## Usage
Add dependancy to the `Package.swift`
```swift
.package(url: "https://github.com/malien/raw-dawg.swift.git", from: "0.0.1")
```
And don't forget to add 
```swift
.product(name: "RawDawg", package: "raw-dawg.swift")
```
to the target dependancies as well

```swift
let db = try Database(filename: "mydb.sqlite")
try await db.execute("""
  create table users (id integer primary key autoincrement, name text not null, age integer);
  insert into users (name, age) values ('Alice', 24), ('Bob', null);
  """)

struct User: Codable {
  var id: Int
  var name: String
  var age: Int?
}

let username = "Alice"
let alice: User = try await db.prepare("select id, name, age from users where name = \(username)").fetchOne()

let adults: [User] = try await db.prepare("select * from users where age is not null and age > 18").fetchAll()
```

### Checklist:
#### ✅ As close to raw SQL as possible.
Without building leaky ORM abstractions on top of relational model. Plain and simple. No need to learn an additional query mechanism.
```swift
db.prepare("""
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
  .fetchAll()
```
#### ✅ Convenient swift API on top of sqlite3
sqlite3 C's API is quite nice... when it comes to C APIs. 

Swift users deserve better! Using all of the modern Swift tooling to build a delightful experience
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

let posts: [Post] = try await db.prepare("select title, contents, created_at, 0 as starred, cover, category from posts").fetchAll()
```
This also means you get `RawRepresentable` enum serialization for free. Bot `Int` and `String` ones.
#### ✅ No SQL injections.
`"where name = \(username)"` is built on top of Swift's `ExpressibleByStringInterpolation` and safely escapes (binds) arguments instead of interpolating a string.
```swift
func getUser(byID id: Int) async throws -> User? {
  try await db.prepare("select * from users where id = \(id)").fetchOptional()
}

func createUser(withName name: String) async throws -> User {
  try await db.prepare("insert into users (name) values (\(name)) returning *").fetchOne()
}

try await createUser(withName: "mark'); drop table users;") // Phew 😮‍💨. Nothing to worry about
```
#### ✅ Database is an actor. 
SQLite access is single threaded anyway. Actors provide convenient data access serialization with familiar async-await syntax.
#### ✅ Convenient APIs for whatever life throws your way
- [X] "Always-at-least-one" fetching via `statement.fetchOne()`
- [X] Optionalities built-in via `statement.fetchOptional()`
- [X] Fetch everything via `statement.fetchAll()`
- [X] Incremental fetching via `statement.step()`
- [X] Push-based streaming via `statement.stream()`
#### ✅ Dynamic safe query building
```swift
func findProducts(filter: ProducFilter) async throws -> [Product] {
  try await db.prepare("select * from products \(fragment: filter.whereClause)").fetchAll()
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
