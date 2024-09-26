import Foundation
import Vapor
import XCTest

@testable import ClickHouseVapor

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

    @Field(key: "arr")
    var arr: [ [Int64] ]

    /// Not implemented on test-server
    // @Field(key: "bol")
    // var bol: [ Bool ]

    @Field(key: "dat")
    var dat: [ClickHouseDate]

    /// Not implemented on test-server
    // @Field(key: "dat32")
    // var dat32: [ClickHouseDate32]

    @Field(key: "datt")
    var datt: [ ClickHouseDateTime ]

    @Field(key: "dattz", timeZone: "'GMT'")
    var dattz: [ ClickHouseDateTime ]

    @Field(key: "datt64", precision: 3)
    var datt64: [ ClickHouseDateTime64 ]

    @Field(key: "datt64z", precision: 3, timeZone: "'GMT'")
    var datt64z: [ ClickHouseDateTime64 ]

    @Field(key: "en8", mapping: ["a": 0, "b": 1])
    var en8: [ ClickHouseEnum8 ]

    @Field(key: "en16", mapping: ["a": 12, "b": 1, "c": 600])
    var en16: [ ClickHouseEnum16 ]

    @Field(key: "temperature")
    var temperature: [Float]

    public required init() {}

    public static var engine: ClickHouseEngine {
        return ClickHouseEngineReplacingMergeTree(
            table: "test",
            database: nil,
            cluster: nil,
            partitionBy: "toYYYYMM(toDateTime(timestamp))"
        )
    }
}

open class TestParentClass {
    @Field(key: "timestamp", isPrimary: true, isOrderBy: true)
    var timestamp: [Int64]

    @Field(key: "stationID", isPrimary: true, isOrderBy: true, isLowCardinality: true)
    var id: [String]
}

public final class InheritedTestModel: TestParentClass, ClickHouseModel {
    @Field(key: "temperature")
    var temperature: [Float]

    override public init() {}

    public static var engine: ClickHouseEngine {
        return ClickHouseEngineReplacingMergeTree(
            table: "testInherited",
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
        model.arr = [[1], [], [76, 56, 2]]
        model.dat = [.clickhouseDefault, .clickhouseDefault, .clickhouseDefault]
        model.datt = [.clickhouseDefault, .clickhouseDefault, .clickhouseDefault]
        model.datt64 = [.clickhouseDefault, .clickhouseDefault, .clickhouseDefault]
        model.datt64z = [.clickhouseDefault, .clickhouseDefault, .clickhouseDefault]
        model.dattz = [.clickhouseDefault, .clickhouseDefault, .clickhouseDefault]
        model.en8 = [.init(word: "a"), .init(word: "b"), .init(word: "a")]
        model.en16 = [.init(word: "a"), .init(word: "b"), .init(word: "c")]
        model.timestamp = [ 100, 200, 300 ]
        model.temperature = [ 11.1, 10.4, 8.9 ]

        let createQuery = TestModel.engine.createTableQuery(columns: model.properties)
        XCTAssertEqual(createQuery
            .replacingOccurrences(of: "Enum8('b'=1,'a'=0)", with: "Enum8('a'=0,'b'=1)")
            .replacingOccurrences(of: "Enum16('b'=1,'a'=12,'c'=600)", with: "Enum16('a'=12,'b'=1,'c'=600)")
            .replacingOccurrences(of: "Enum16('b'=1,'c'=600,'a'=12)", with: "Enum16('a'=12,'b'=1,'c'=600)")
            .replacingOccurrences(of: "Enum16('a'=12,'c'=600,'b'=1)", with: "Enum16('a'=12,'b'=1,'c'=600)")
            .replacingOccurrences(of: "Enum16('c'=600,'b'=1,'a'=12)", with: "Enum16('a'=12,'b'=1,'c'=600)")
            .replacingOccurrences(of: "Enum16('c'=600,'a'=12,'b'=1)", with: "Enum16('a'=12,'b'=1,'c'=600)"),
            """
            CREATE TABLE IF NOT EXISTS `test`  (timestamp Int64,stationID LowCardinality(String),fixed LowCardinality(FixedString(10)),arr Array(Int64),dat Date,datt DateTime,dattz DateTime('GMT'),datt64 DateTime64(3),datt64z DateTime64(3, 'GMT'),en8 Enum8('a'=0,'b'=1),en16 Enum16('a'=12,'b'=1,'c'=600),temperature Float32)
            ENGINE = ReplacingMergeTree()
            PRIMARY KEY (timestamp,stationID) PARTITION BY (toYYYYMM(toDateTime(timestamp))) ORDER BY (timestamp,stationID)
            """)
        try! TestModel.createTable(on: app.clickHouse).wait()
        try! model.insert(on: app.clickHouse).wait()
        let model2 = try! TestModel.select(on: app.clickHouse).wait()

        XCTAssertEqual(model.temperature, model2.temperature)
        XCTAssertEqual(model.id, model2.id)
        XCTAssertEqual(["", "123456", "1234567890"], model2.fixed)
        XCTAssertEqual(model.timestamp, model2.timestamp)
        XCTAssertEqual(model.dat.map { $0.date }, [Date(timeIntervalSince1970: 0.0), Date(timeIntervalSince1970: 0.0), Date(timeIntervalSince1970: 0.0)])
        XCTAssertEqual(model2.dat.map { $0.date }, [Date(timeIntervalSince1970: 0.0), Date(timeIntervalSince1970: 0.0), Date(timeIntervalSince1970: 0.0)])
        XCTAssertEqual(model.datt.map { $0.date }, [Date(timeIntervalSince1970: 0.0), Date(timeIntervalSince1970: 0.0), Date(timeIntervalSince1970: 0.0)])
        XCTAssertEqual(model2.datt.map { $0.date }, [Date(timeIntervalSince1970: 0.0), Date(timeIntervalSince1970: 0.0), Date(timeIntervalSince1970: 0.0)])
        XCTAssertEqual(model.dattz.map { $0.date }, [Date(timeIntervalSince1970: 0.0), Date(timeIntervalSince1970: 0.0), Date(timeIntervalSince1970: 0.0)])
        XCTAssertEqual(model2.dattz.map { $0.date }, [Date(timeIntervalSince1970: 0.0), Date(timeIntervalSince1970: 0.0), Date(timeIntervalSince1970: 0.0)])
        XCTAssertEqual(model.en8.map { $0.word }, ["a", "b", "a"])
        XCTAssertEqual(model2.en8.map { $0.word }, ["a", "b", "a"])
        XCTAssertEqual(model.en16.map { $0.word }, ["a", "b", "c"])
        XCTAssertEqual(model2.en16.map { $0.word }, ["a", "b", "c"])
        XCTAssertEqual(model.arr, [[1], [], [76, 56, 2]])
        XCTAssertEqual(model2.arr, [[1], [], [76, 56, 2]])

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

        // Raw select query, that gets applied to
        let model3 = try! TestModel.select(
            on: app.clickHouse,
            sql: "SELECT timestamp, stationID FROM default.test"
        ).wait()
        XCTAssertTrue(model3.temperature.isEmpty, "temperature array was not selected, is supposed to be empty")
        XCTAssertEqual(model3.id, model2.id)
        XCTAssertEqual(model3.timestamp, model2.timestamp)
    }

    public func testInheritedModel() throws {
        let app = Application(.testing)
        defer { app.shutdown() }
        try! app.configureClickHouseDatabases()
        app.logger.logLevel = .trace

        let model = InheritedTestModel()
        let createQuery = InheritedTestModel.engine.createTableQuery(columns: model.properties)
        XCTAssertEqual(
            createQuery,
            """
            CREATE TABLE IF NOT EXISTS `testInherited`  (timestamp Int64,stationID LowCardinality(String),temperature Float32)
            ENGINE = ReplacingMergeTree()
            PRIMARY KEY (timestamp,stationID) PARTITION BY (toYYYYMM(toDateTime(timestamp))) ORDER BY (timestamp,stationID)
            """
        )

        // drop table to ensure unit test
        try! InheritedTestModel.deleteTable(on: app.clickHouse).wait()
        // create table
        try! InheritedTestModel.createTable(on: app.clickHouse).wait()

        // fill model with data and insert it
        model.id = [ "x010", "ax51", "cd22" ]
        model.timestamp = [ 100, 200, 300 ]
        model.temperature = [ 11.1, 10.4, 8.9 ]
        try! model.insert(on: app.clickHouse).wait()

        // select the data again
        let model2 = try! InheritedTestModel.select(on: app.clickHouse).wait()

        XCTAssertEqual(model2.id, model.id)
        XCTAssertEqual(model2.timestamp, model.timestamp)
        XCTAssertEqual(model2.temperature, model.temperature)
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
