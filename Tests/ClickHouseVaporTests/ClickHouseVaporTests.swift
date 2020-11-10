import XCTest
@testable import ClickHouseVapor

final class ClickHouseVaporTests: XCTestCase {
    func testExample() {
        // This is an example of a functional test case.
        // Use XCTAssert and related functions to verify your tests produce the correct
        // results.
        XCTAssertEqual(ClickHouseVapor().text, "Hello, World!")
    }

    static var allTests = [
        ("testExample", testExample),
    ]
}
