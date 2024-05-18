# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project **DOES NOT** adhere to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). (_for now_)

## [Unreleased]

## [0.0.5] - 2024-05-15

### Added
- Better error descriptions, which now contain the context of where they've occurred

### Fixed
- On non-Apple platforms, statically linked sqlite3 library was initialized before opening a connection (Apple platforms link to `libsqlite3.dylib` dynamically, which doesn't require prior initialization)
- The combination of `mode: .readOnly, create: true` caused `sqlite3_open_v2` to fail. Now this combination is not expressible in the swift type system.

### Removed
- `NoRowsFetched` error struct
- `InvalidQuery` error struct
- `Database.init(filename:mode:created:)` replaced in favour of `Database.init(filename:mode:)`

### Changed
- Unified all error clauses under `SQLiteError`, which isn't anymore an error type that comes directly from `sqlite3`.
- `SQLiteError.code` may be set to generic `SQLITE_ERROR`, if error didn't originate directly from `sqlite3`
- `SQLiteError.message` may be set to library-specific error description, if error didn't originate directly from `sqlite3`
- `create` flag is now defaults to true, meaning readWrite databases would
- `OpenMode` now also describes whether or not to create the database file in read-write mode. `.readWrite(craete: Bool)`

## [0.0.4] - 2024-05-15

### Added

- Row conformance to `Collection` and `RandomAccessCollection`
- Publically exposed `SQLPrimitiveDecodable` protocol and its standard library type conformances.
- Conveniance `Row.decode(valueAt:)` methods that utilize extension points of `SQLPrimitiveDecodable`
- Conveniance `Row[valueAt: Int]`, `Row[String]` subscript operators
- Dependance on `swift-agorithms`
- This CHANGELOG.md file

### Fixed

- Invlid/Inconsistent Package.swift on Apple vs non-Apple platforms

### Changed

- `Row.values` and `Row.columns` are now computed properties that return `some RandomAccessCollection` instead of being mutable versions of themselves
- Internal method signatures hopefully resulting in les allocations and better performance.
- `PreparedStatement` now keeps a `finished` flag, to prevent calls to `sqlite3_step` after `SQLITE3_DONE` is returned.

### Removed

- `PreparedStatement.stream` methods as I don't feel comfortable shipping this API yet.

## [0.0.3] - 2024-05-14

### Changed
- Made `PreparedStatement.run` `@discardableResult

## [0.0.2] - 2024-05-14

### Added
- Installation instructions to README.md

### Changed
- Added `Sendable` constraints to all type-safe types (`PreparedStatement`, `BoundQuery`, `Row`, `SQLNull`, `SQLiteBlob`, `SQLiteValue`, `OpenMode`, `InsertionStats`, error types)

[unreleased]: https://github.com/malien/raw-dawg.swift/compare/0.0.3...HEAD
[0.0.5]: https://github.com/malien/raw-dawg.swift/compare/0.0.4...0.0.5
[0.0.4]: https://github.com/malien/raw-dawg.swift/compare/0.0.3...0.0.4
[0.0.3]: https://github.com/malien/raw-dawg.swift/compare/0.0.2...0.0.3
[0.0.2]: https://github.com/malien/raw-dawg.swift/compare/0.0.1...0.0.2
[0.0.1]: https://github.com/malien/raw-dawg.swift/releases/tag/0.0.1