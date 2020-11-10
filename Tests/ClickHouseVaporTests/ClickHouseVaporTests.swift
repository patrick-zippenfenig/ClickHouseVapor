import XCTest
@testable import ClickHouseVapor
import Vapor

extension Application {
    func configureClickHouseDatabases() throws {
        let ip = ProcessInfo.processInfo.environment["CLICKHOUSE_SERVER"] ?? "172.25.101.30"
        let user = ProcessInfo.processInfo.environment["CLICKHOUSE_USER"] ?? "default"
        let password = ProcessInfo.processInfo.environment["CLICKHOUSE_PASSWORD"] ?? "admin"
        clickHouse.configuration = try ClickHouseConfiguration(hostname: ip, port: 9000, user: user, password: password, database: "default")
    }
}



final class ClickHouseVaporTests: XCTestCase {
    func testPing() {
        let app = Application(.testing)
        defer { app.shutdown() }
        try! app.configureClickHouseDatabases()
        
        let _ = XCTAssertNoThrow(try app.clickHouse.ping().wait())
    }

    static var allTests = [
        ("testPing", testPing),
    ]
}
