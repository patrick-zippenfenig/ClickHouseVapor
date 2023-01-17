// swift-tools-version:5.2

import PackageDescription

let package = Package(
    name: "ClickHouseVapor",
    platforms: [.macOS(.v10_15)],
    products: [
        .library(
            name: "ClickHouseVapor",
            targets: ["ClickHouseVapor"]),
    ],
    dependencies: [
        .package(url: "https://github.com/vapor/vapor.git", from: "4.0.0"),
        .package(url: "https://github.com/patrick-zippenfenig/ClickHouseNIO.git", from: "1.4.1")
    ],
    targets: [
        .target(
            name: "ClickHouseVapor",
            dependencies: ["ClickHouseNIO", .product(name: "Vapor", package: "vapor")]),
        .testTarget(
            name: "ClickHouseVaporTests",
            dependencies: ["ClickHouseVapor"]),
    ]
)
