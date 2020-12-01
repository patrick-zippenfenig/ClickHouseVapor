//
//  ClickHouseVaporError.swift
//  ClickHouseVapor
//
//  Created by Patrick Zippenfenig on 2020-11-11.
//

public enum ClickHouseVaporError: Error, Equatable {
    case mismatchingDataType(columnName: String)
    case mismatchingRowCount(count: Int, expected: Int)
}
