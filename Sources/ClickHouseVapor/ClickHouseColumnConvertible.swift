//
//  ClickHouseColumnConvertible.swift
//  ClickHouseVapor
//
//  Created by Patrick Zippenfenig on 2020-11-10.
//

import ClickHouseNIO

/// Define how a colun can be converted into a clickhose datatype
public protocol ClickHouseColumnConvertible : Codable, AnyObject {
    var key: String         { get }
    var isPrimary: Bool     { get }
    var isOrderBy: Bool     { get }
    var partitionBy: Bool   { get }
    /// number of elements inside the data column array
    var count: Int { get }
    
    func setClickHouseArray(data: [ClickHouseDataType]) throws
    //func reserveCapacity(_ capacity: Int)
    //func append(_ other: [ClickHouseDataType])
    func getClickHouseArray() -> [ClickHouseDataType]
    func clickHouseTypeName() -> ClickHouseTypeName
    /// Only include column rows where the isIncluded array is true. This is equivalent to the IDL `where` function.
    //func filter(_ isIncluded: [Bool])
}

public protocol ClickHouseColumnConvertibleTyped : ClickHouseColumnConvertible {
    associatedtype Value: ClickHouseDataType
    var wrappedValue: [Value] { get set }
    var fixedStringLen: Int? { get }
    
}

extension ClickHouseColumnConvertibleTyped {
    public func toClickHouseArray() -> ClickHouseColumn {
        return ClickHouseColumn(key, wrappedValue)
    }
    
    public func setClickHouseArray(data: [ClickHouseDataType]) throws {
        guard let array = data as? [Value] else {
            throw ClickHouseModelError.missmatchingDataType(columnName: key)
        }
        self.wrappedValue = array
    }
    
    /// Only include column rows where the isIncluded array is true. This is equivalent to the IDL `where` function.
    /*public func filter(_ isIncluded: [Bool]) {
        wrappedValue = wrappedValue.filtered(isIncluded)
    }*/
    
    public var count: Int {
        return wrappedValue.count
    }
    
    /*ublic func reserveCapacity(_ capacity: Int) {
        wrappedValue.reserveCapacity(capacity)
    }
    public func append(_ other: [ClickHouseDataType]) {
        guard let array = other as? [Value] else {
            fatalError("Cannot append arrays of different datatypes in column \(key)")
        }
        wrappedValue += array
    }*/
    public func getClickHouseArray() -> [ClickHouseDataType] {
        return wrappedValue
    }
    public func clickHouseTypeName() -> ClickHouseTypeName {
        return Value.getClickHouseTypeName(fixedLength: fixedStringLen)
    }
}

    
@propertyWrapper
public final class Field<Value: ClickHouseDataType>: ClickHouseColumnConvertibleTyped where Value: Codable {
    public let key: String
    public var wrappedValue: [Value]
    public let isPrimary: Bool
    public let isOrderBy: Bool
    public let partitionBy: Bool
    public let fixedStringLen: Int?
    
    public var projectedValue: Field<Value> {
        self
    }

    public init(key: String, isPrimary: Bool = false, isOrderBy: Bool = false, partitionBy: Bool = false, fixedStringLen: Int? = nil) {
        self.key = key
        self.isPrimary = isPrimary
        self.isOrderBy = isOrderBy
        self.partitionBy = partitionBy
        self.fixedStringLen = fixedStringLen
        self.wrappedValue = []
    }

    enum CodingKeys: String, CodingKey {
        case key
        case wrappedValue = "data"
        case isPrimary
        case isOrderBy
        case partitionBy
        case fixedStringLen
    }
    
    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(wrappedValue, forKey: .wrappedValue)
    }
}
