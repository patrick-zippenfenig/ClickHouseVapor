import XCTest
@testable import ClickHouseVapor
import Vapor

extension Application {
    func configureClickHouseDatabases() throws {
        let ip = ProcessInfo.processInfo.environment["CLICKHOUSE_SERVER"] ?? "172.25.101.30"
        let user = ProcessInfo.processInfo.environment["CLICKHOUSE_USER"] ?? "default"
        let password = ProcessInfo.processInfo.environment["CLICKHOUSE_PASSWORD"] ?? "admin"
        clickHouse.configuration = try ClickHousePoolConfiguration(
            hostname: ip,
            port: 9000,
            user: user,
            password: password,
            database: "default",
            maxConnectionsPerEventLoop: 2,
            requestTimeout: .seconds(10)
        )
    }
}

public class TestModel: ClickHouseModel {
    @Field(key: "timestamp", isPrimary: true, isOrderBy: true)
    var timestamp: [Int64]

    @Field(key: "stationID", isPrimary: true, isOrderBy: true, isLowCardinality: true)
    var id: [String]

    @Field(key: "fixed", isLowCardinality: true, fixedStringLen: 10)
    var fixed: [ String ]

    @Field(key: "temperature")
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

    static var allTests = [
        ("testPing", testPing),
        ("testModel", testModel),
        ("testDifferingRowsInsert", testDifferingRowsInsert),
        ("testEmptyModelInsert", testEmptyModelInsert)
    ]

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

        let filtered = try! TestModel.select(
            on: app.clickHouse,
            fields: ["timestamp", "stationID"],
            where: "temperature > 10",
            order: "timestamp DESC",
            limit: 10,
            offset: 0
        ).wait()

        XCTAssertTrue(filtered.temperature.isEmpty, "temperature array was not selected, is supposed to be empty")
        XCTAssertEqual(filtered.id, ["ax51", "x010"])
        XCTAssertEqual(filtered.timestamp, [200, 100])

        /// Raw select query, that gets applied to
        let model3 = try! TestModel.select(
            on: app.clickHouse,
            sql: "SELECT timestamp, stationID FROM default.test"
        ).wait()
        XCTAssertTrue(model3.temperature.isEmpty, "temperature array was not selected, is supposed to be empty")
        XCTAssertEqual(model3.id, model2.id)
        XCTAssertEqual(model3.timestamp, model2.timestamp)
    }

    /// insert should fail if some columns are not set, but others are
    public func testDifferingRowsInsert() {
        let app = Application(.testing)
        defer { app.shutdown() }
        try! app.configureClickHouseDatabases()
        app.logger.logLevel = .trace

        let model = TestModel()

        // drop table to ensure unit test
        try! TestModel.deleteTable(on: app.clickHouse).wait()

        model.id = [ "x010", "ax51", "cd22" ]
        model.fixed = [ "", "12345678901234" ]
        model.timestamp = [ 100, 200, 300 ]
        model.temperature = [ 11.1, 10.4, 8.9 ]

        try! TestModel.createTable(on: app.clickHouse).wait()

        var thrownError: Error?
        // insert should fail if one tries to insert columns with different amount of rows
        XCTAssertThrowsError(try model.insert(on: app.clickHouse).wait()) {
            thrownError = $0
        }
        // check that we get correct error message
        XCTAssertEqual(
            thrownError as! ClickHouseVaporError,
            ClickHouseVaporError.mismatchingRowCount(count: 2, expected: 3)
        )
    }

    /// insert should not fail if the complete model is empty
    public func testEmptyModelInsert() {
        let app = Application(.testing)
        defer { app.shutdown() }
        try! app.configureClickHouseDatabases()
        app.logger.logLevel = .trace

        let model = TestModel()

        // drop table to ensure unit test
        try! TestModel.deleteTable(on: app.clickHouse).wait()

        try! TestModel.createTable(on: app.clickHouse).wait()

        // insert should not fail if all columns are empty
        XCTAssertNoThrow(try model.insert(on: app.clickHouse).wait())
    }
}
