import XCTest

import ClickHouseVaporTests

var tests = [XCTestCaseEntry]()
tests += ClickHouseVaporTests.allTests()
XCTMain(tests)
