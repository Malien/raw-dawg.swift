// swift-tools-version: 5.10
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

var packageDeps: [Package.Dependency] = [
    .package(url: "https://github.com/apple/swift-log.git", from: "1.0.0")
]
#if !canImport(SQLite3)
    packageDeps.append(.package(url: "https://github.com/sbooth/CSQLite.git", from: "3.45.3"))
#endif

var targetDeps: [Target.Dependency] = [
    .product(name: "Logging", package: "swift-log")
]
#if !canImport(SQLite3)
    packageDeps.append("CSQLite")
#endif

let package = Package(
    name: "raw-dawg",
    platforms: [
        .macOS(.v10_15),
        .iOS(.v13),
    ],
    products: [
        // Products define the executables and libraries a package produces, making them visible to other packages.
        .library(
            name: "RawDawg",
            targets: ["RawDawg"]
        )
    ],
    dependencies: packageDeps,
    targets: [
        // Targets are the basic building blocks of a package, defining a module or a test suite.
        // Targets can depend on other targets in this package and products from dependencies.
        .target(
            name: "RawDawg",
            dependencies: targetDeps),
        .testTarget(
            name: "RawDawgTests",
            dependencies: ["RawDawg"]),
    ]
)
