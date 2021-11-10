//
//  ClickHouseColumnConvertible.swift
//  ClickHouseVapor
//
//  Created by Patrick Zippenfenig on 2020-11-10.
//

import ClickHouseNIO

/// Define how a colun can be converted into a clickhose datatype
public protocol ClickHouseColumnConvertible: AnyObject {
    var key: String { get }
    var isPrimary: Bool { get }
    var isOrderBy: Bool { get }
    var isLowCardinality: Bool { get }
    /// number of elements inside the data column array
    var count: Int { get }

    /// Set the query result to this column
    func setClickHouseArray(_ data: ClickHouseDataTypeArray) throws
    func reserveCapacity(_ capacity: Int)
    func append(_ other: ClickHouseDataTypeArray)
    func getClickHouseArray() -> ClickHouseDataTypeArray
    func clickHouseTypeName() -> ClickHouseTypeName
    /// Only include column rows where the isIncluded array is true.
    func filter(_ isIncluded: [Bool])
}

/// Intermediate protocol that is aware of the associated type
public protocol ClickHouseColumnConvertibleTyped: ClickHouseColumnConvertible {
    associatedtype Value: ClickHouseDataType
    var wrappedValue: [Value] { get set }
    var fixedStringLen: Int? { get }

}

extension ClickHouseColumnConvertibleTyped {
    public func toClickHouseArray() -> ClickHouseColumn {
        return ClickHouseColumn(key, wrappedValue)
    }

    public func setClickHouseArray(_ data: ClickHouseDataTypeArray) throws {
        guard let array = data as? [Value] else {
            throw ClickHouseVaporError.mismatchingDataType(columnName: key)
        }
        self.wrappedValue = array
    }

    public func filter(_ isIncluded: [Bool]) {
        wrappedValue = wrappedValue.filtered(isIncluded)
    }

    public var count: Int {
        return wrappedValue.count
    }

    public func reserveCapacity(_ capacity: Int) {
        wrappedValue.reserveCapacity(capacity)
    }

    public func append(_ other: ClickHouseDataTypeArray) {
        guard let array = other as? [Value] else {
            fatalError("Cannot append arrays of different datatypes in column \(key)")
        }
        wrappedValue += array
    }

    public func getClickHouseArray() -> ClickHouseDataTypeArray {
        return wrappedValue
    }

    public func clickHouseTypeName() -> ClickHouseTypeName {
        return Value.getClickHouseTypeName(fixedLength: fixedStringLen)
    }
}

@propertyWrapper
public final class Field<Value: ClickHouseDataType>: ClickHouseColumnConvertibleTyped {
    public let key: String
    public var wrappedValue: [Value]
    public let isPrimary: Bool
    public let isOrderBy: Bool
    public let isLowCardinality: Bool
    public let fixedStringLen: Int?

    public var projectedValue: Field<Value> {
        self
    }

    public init(
        key: String,
        isPrimary: Bool = false,
        isOrderBy: Bool = false,
        isLowCardinality: Bool = false,
        fixedStringLen: Int? = nil
    ) {
        self.key = key
        self.isPrimary = isPrimary
        self.isOrderBy = isOrderBy
        self.isLowCardinality = isLowCardinality
        self.fixedStringLen = fixedStringLen
        self.wrappedValue = []
    }
}

extension Array {
    /// Only include column rows where the isIncluded array is true
    func filtered(_ isIncluded: [Bool]) -> Self {
        precondition(count == isIncluded.count)
        var arr = Self.init()
        let count = isIncluded.reduce(0, { $0 + ($1 ? 1 : 0) })
        arr.reserveCapacity(count)
        for (i, include) in isIncluded.enumerated() where include {
            arr.append(self[i])
        }
        return arr
    }
}
