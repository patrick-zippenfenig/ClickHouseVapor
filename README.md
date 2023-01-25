# ClickHouseVapor

![Swift 5](https://img.shields.io/badge/Swift-5-orange.svg) ![SPM](https://img.shields.io/badge/SPM-compatible-green.svg) ![Platforms](https://img.shields.io/badge/Platforms-macOS%20Linux-green.svg) [![codebeat badge](https://codebeat.co/badges/d4500aa3-30f6-471a-89ae-924cd14aed17)](https://codebeat.co/projects/github-com-patrick-zippenfenig-clickhousevapor-main) [![CircleCI](https://circleci.com/gh/patrick-zippenfenig/ClickHouseVapor/tree/main.svg?style=svg)](https://circleci.com/gh/patrick-zippenfenig/ClickHouseVapor/tree/main)

A simple column-oriented ORM for the [ClickHouse](https://clickhouse.tech) database in Swift.

Features:

- Fixed datatype ORM
- Connection pool
- Fully asynchronous based on the adapter [ClickHouseNio](https://github.com/patrick-zippenfenig/ClickHouseNIO)
- Integrated with [Vapor 4](https://github.com/vapor/vapor)

## Installation

1. Add `ClickHouseVapor` as a dependency to your `Package.swift`

```swift
  dependencies: [
    .package(url: "https://github.com/patrick-zippenfenig/ClickHouseVapor.git", from: "1.0.0")
  ],
  targets: [
    .target(name: "MyApp", dependencies: ["ClickHouseVapor"])
  ]
```

2. Build your project:

```bash
swift build
```

## Usage

1. Configure the connection credentials with a Vapor 4 application. Usually this is done in `config.swift`.

    Note: `maxConnectionsPerEventLoop` controls the number of connections per thread. If you have 4 CPU cores and Vapor is using 4 eventLoops, 8 connections will be used. `requestTimeout` is the timeout to establish a connection. It does not limit query runtime.

    ```swift
    import ClickHouseVapor
    
    let app = Application(.testing)
    defer { app.shutdown() }
    
    app.clickHouse.configuration = try ClickHousePoolConfiguration(
        hostname: "localhost",
        port: 9000,
        user: "default",
        password: "admin",
        database: "default",
        maxConnectionsPerEventLoop: 2,
        requestTimeout: .seconds(10)
    )
    ```

2. Define a table with fields and an engine.

    ```swift
    public class TestModel : ClickHouseModel {
        @Field(key: "timestamp", isPrimary: true, isOrderBy: true)
        var timestamp: [Int64]
        
        @Field(key: "stationID", isPrimary: true, isOrderBy: true)
        var id: [String]
        
        @Field(key: "fixed", fixedStringLen: 10)
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
    ```

3. Create a table. For simplicity this example is calling `wait()`. It is discouraged to use `wait()` in production.

    ```swift
    try TestModel.createTable(on: app.clickHouse).wait()
    ```

4. Insert data

    ```swift
    let model = TestModel()
    model.id = [ "x010", "ax51", "cd22" ]
    model.fixed = [ "", "123456", "12345678901234" ]
    model.timestamp = [ 100, 200, 300 ]
    model.temperature = [ 11.1, 10.4, 8.9 ]
    
    try model.insert(on: app.clickHouse).wait()
    ````

5. Query all data again

    ```swift
    let result = try TestModel.select(on: app.clickHouse).wait()
    print(result.temperature) // [ 11.1, 10.4, 8.9 ]
    
    // Filter data in more detail
    let result2 = try! TestModel.select(
        on: app.clickHouse,
        fields: ["timestamp", "stationID"],
        where: "temperature > 10",
        order: "timestamp DESC",
        limit: 10,
        offset: 0
    ).wait()
    
    print(result2.id) // ["ax51", "x010"]
    print(result2.timestamp) //  [200, 100]
    
    // Perform raw queries, but assign the result to TestModel
    let sql = "SELECT timestamp, stationID FROM default.test"
    let result2 = try! TestModel.select(on: app.clickHouse, sql: sql).wait()
    ```

6. If you have several models that follow a common base scheme, you can also use inheritance to keep your code tidy:

    ```swift
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
    ```

## ToDo List

- Query timeouts
- Implement more engines

## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License

[MIT](https://choosealicense.com/licenses/mit/)
