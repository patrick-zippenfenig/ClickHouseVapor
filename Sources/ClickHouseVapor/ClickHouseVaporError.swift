//
//  ClickHouseVaporError.swift
//  ClickHouseVapor
//
//  Created by Patrick Zippenfenig on 2020-11-11.
//

public enum ClickHouseVaporError: Error {
    case missmatchingDataType(columnName: String)
}
