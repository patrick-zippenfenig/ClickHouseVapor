import XCTest
@testable import ClickHouseVapor
import Vapor

extension Application {
    func configureClickHouseDatabases() throws {
        let ip = ProcessInfo.processInfo.environment["CLICKHOUSE_SERVER"] ?? "172.25.101.30"
        let user = ProcessInfo.processInfo.environment["CLICKHOUSE_USER"] ?? "default"
        let password = ProcessInfo.processInfo.environment["CLICKHOUSE_PASSWORD"] ?? "admin"
        clickHouse.configuration = try ClickHousePoolConfiguration(hostname: ip, port: 9000, user: user, password: password, database: "default")
    }
}

public class TestModel : ClickHouseModel {
    @Field(key: "timestamp", isPrimary: true, isOrderBy: true)
    var timestamp: [Int64]
    
    @Field(key: "stationID", isPrimary: true, isOrderBy: true)
    var id: [String]
    
    @Field(key: "fixed", fixedStringLen: 10)
    var fixed: [ String ]
    
    @Field(key: "temperature_hourly_something")
    var temperature: [Float]
    
    required public init() {
        
    }
    
    public static var engine: ClickHouseEngine {
        return ClickHouseEngineReplacingMergeTree(
            table: "test",
            database: nil,
            cluster: nil,
            partitionBy: "toYYYYMM(toDateTime(timestamp))"
        )
    }
}


final class ClickHouseVaporTests: XCTestCase {
    func testPing() {
        let app = Application(.testing)
        defer { app.shutdown() }
        try! app.configureClickHouseDatabases()
        
        XCTAssertNoThrow(try app.clickHouse.ping().wait())
    }
    
    public func testModel() {
        let app = Application(.testing)
        defer { app.shutdown() }
        try! app.configureClickHouseDatabases()
        app.logger.logLevel = .trace
        
        let model = TestModel()
        
        // drop table to ensure unit test
        try! TestModel.deleteTable(on: app.clickHouse).wait()

        
        model.id = [ "x010", "ax51", "cd22" ]
        model.fixed = [ "", "123456", "12345678901234" ]
        model.timestamp = [ 100, 200, 300 ]
        model.temperature = [ 11.1, 10.4, 8.9 ]

        try! TestModel.createTable(on: app.clickHouse).wait()
        try! model.insert(on: app.clickHouse).wait()
        
        let model2 = try! TestModel.select(on: app.clickHouse).wait()
        
        XCTAssertEqual(model.temperature, model2.temperature)
        XCTAssertEqual(model.id, model2.id)
        XCTAssertEqual(["", "123456", "1234567890"], model2.fixed)
        XCTAssertEqual(model.timestamp, model2.timestamp)
        
        /// Raw select query, that gets applied to
        let model3 = try! TestModel.select(on: app.clickHouse, sql: "SELECT timestamp, stationID FROM default.test").wait()
        XCTAssertTrue(model3.temperature.isEmpty, "temperature array was not selected, is supposed to be empty")
        XCTAssertEqual(model3.id, model2.id)
        XCTAssertEqual(model3.timestamp, model2.timestamp)
    }

    static var allTests = [
        ("testPing", testPing),
    ]
}
