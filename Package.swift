// swift-tools-version: 6.4
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

/// Opt into every upcoming feature that is not yet on by default in Swift 6 mode,
/// plus strict memory safety, lifetime annotations, and warnings-as-errors.
let swiftSettings: [SwiftSetting] = [
    // Upcoming features scheduled for Swift 7
    .enableUpcomingFeature("ExistentialAny"),
    .enableUpcomingFeature("InternalImportsByDefault"),
    .enableUpcomingFeature("MemberImportVisibility"),
    .enableUpcomingFeature("InferIsolatedConformances"),
    .enableUpcomingFeature("NonisolatedNonsendingByDefault"),
    .enableUpcomingFeature("ImmutableWeakCaptures"),
    .strictMemorySafety(),
    .treatAllWarnings(as: .error),
]

let package = Package(
    name: "SIMDate",
    products: [
        // Products define the executables and libraries a package produces, making them visible to other packages.
        .library(
            name: "SIMDate",
            targets: ["SIMDate"]
        ),
    ],
    targets: [
        // Targets are the basic building blocks of a package, defining a module or a test suite.
        // Targets can depend on other targets in this package and products from dependencies.
        .target(
            name: "SIMDate",
            swiftSettings: swiftSettings,
        ),
        .testTarget(
            name: "SIMDateTests",
            dependencies: ["SIMDate"],
            swiftSettings: swiftSettings,
        ),
    ],
    swiftLanguageModes: [.v6]
)
