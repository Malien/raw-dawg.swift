// swift-tools-version: 5.10
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

// Aka. the platforms that don't ship SQLite3 swift package by default
let nonApplePlatforms: [Platform] = [.linux, .windows, .android, .openbsd, .wasi]

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
    dependencies: [
        .package(url: "https://github.com/apple/swift-log.git", from: "1.0.0"),
        .package(
            url: "https://github.com/apple/swift-algorithms.git", .upToNextMajor(from: "1.2.0")),
        .package(url: "https://github.com/sbooth/CSQLite.git", from: "3.45.3"),
        .package(url: "https://github.com/apple/swift-docc-plugin", from: "1.0.0")
    ],
    targets: [
        // Targets are the basic building blocks of a package, defining a module or a test suite.
        // Targets can depend on other targets in this package and products from dependencies.
        .target(
            name: "RawDawg",
            dependencies: [
                .product(name: "Logging", package: "swift-log"),
                .product(name: "Algorithms", package: "swift-algorithms"),
                .product(
                    name: "CSQLite", package: "CSQLite",
                    condition: .when(platforms: nonApplePlatforms)),
            ]),
        .testTarget(
            name: "RawDawgTests",
            dependencies: ["RawDawg"]),
    ]
)
