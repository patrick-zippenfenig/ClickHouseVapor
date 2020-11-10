//
//  ClickHouseModel.swift
//  ClickHouseVapor
//
//  Created by Patrick Zippenfenig on 2020-11-10.
//

import Foundation
import Vapor
import ClickHouseNIO

public struct TableModelMeta {
    var database: CustomStringConvertible
    var table: CustomStringConvertible
    var cluster: CustomStringConvertible?
}

public protocol ClickHouseModel {
    static var tableMeta: TableModelMeta { get }
    init()
}

extension ClickHouseModel {
    public var count: Int {
        return properties.first?.count ?? 0
    }
    
    /// Only include column rows where the isIncluded array is true. This is equivalent to the IDL `where` function.
    /*public func filter(_ isIncluded: [Bool]) {
        precondition(isIncluded.count == count)
        properties.forEach {
            $0.filter(isIncluded)
        }
    }
    
    public func append(_ other: Self) {
        zip(properties, other.properties).forEach {
            $0.0.append($0.1.getRawDataArray())
        }
    }
    
    /// Reserve space in all data coolumns. Also allocates arrays internally if required.
    public func reserveCapacity(_ capacity: Int) {
        properties.forEach {
            $0.reserveCapacity(capacity)
        }
    }*/
    
    public var properties: [ClickHouseColumnConvertible] {
        return Mirror(reflecting: self).children.compactMap {
            $0.value as? ClickHouseColumnConvertible
        }
    }
    
    public static func createTable(on connection: ClickHouseConnectionProtocol, table: String? = nil) throws -> EventLoopFuture<Void> {
        let fields = Self.init().properties
        let ids = fields.compactMap { $0.isPrimary ? $0.key : nil }
        assert(ids.count >= 1)
        let order = fields.compactMap { $0.isOrderBy ? $0.key : nil }
        assert(order.count >= 1)
        let partitionBy = fields.compactMap { $0.partitionBy ? $0.key : nil }
        assert(partitionBy.count <= 1)
      
        let columnDescriptions = fields.map { field -> String in
            return "\(field.key) \(field.clickHouseTypeName().string)"
        }.joined(separator: ",")
        
        let database = tableMeta.database
        let tableName = table ?? tableMeta.table
        
        let onCluster = tableMeta.cluster.map { "ON CLUSTER \($0)" } ?? ""
        
        let engineReplicated = "ReplicatedReplacingMergeTree('/clickhouse/{cluster}/tables/{database}.{table}/{shard}', '{replica}')"
        let engineNormal = "ReplacingMergeTree()"
        
        let engine = tableMeta.cluster == nil ? engineNormal : engineReplicated
        
        var query =
        """
        CREATE TABLE IF NOT EXISTS \(database).\(tableName) \(onCluster) (\(columnDescriptions))
        ENGINE = \(engine)
        PRIMARY KEY (\(ids.joined(separator: ",")))
        """
        if let partitionBy = partitionBy.first {
            query += "PARTITION BY toYYYYMM(toDateTime(\(partitionBy)))"
        }
        query += "ORDER BY (\(order.joined(separator: ",")))"
        
        connection.logger.debug("\(query)")
        
        if tableMeta.cluster != nil {
            // cluster operation, return some information
            return connection.query(sql: query).transform(to: ())
        } else {
            return connection.command(sql: query)
        }
    }
    
    public func insert(on connection: ClickHouseConnectionProtocol, table: String? = nil) throws  -> EventLoopFuture<Void> {
        let fields = properties
        let data = fields.map { return ClickHouseColumn($0.key, $0.getRawDataArray()) }
        let tableName = table ?? Self.tableMeta.table
        
        guard (data.first?.values.count ?? 0) > 0 else {
            // no values -> nothing to do
            return connection.eventLoop.makeSucceededFuture(())
        }
        
        let dbAndTable = "\(Self.tableMeta.database).\(tableName)"
        
        return connection.insert(into: dbAndTable, data: data)
    }
    
    /**
     WARNING: This function will delete a table on the whole cluster. THIS DELETE CAN NOT BE UNDONE. Use with care.
     */
    public static func deleteTable(on connection: ClickHouseConnectionProtocol, table: String? = nil) throws -> EventLoopFuture<Void> {
        let database = tableMeta.database
        let tableName = table ?? tableMeta.table
        let onCluster = tableMeta.cluster.map { "ON CLUSTER \($0)" } ?? ""
        
        let query = "DROP TABLE IF EXISTS \(database).\(tableName) \(onCluster)"
        
        // deserves a warning
        connection.logger.warning("\(query)")
        
        if tableMeta.cluster != nil {
            // deletes on a cluster, return some information
            return connection.query(sql: query).transform(to: ())
        } else {
            return connection.command(sql: query)
        }
    }
    
    // select: [KeyPath<Self, ClickHouseColumnConvertible>]? = nil,
    /// If final ist set to true, all duplicate merges are ensured, but perfmrance suffers
    public static func select(on connection: ClickHouseConnectionProtocol, final: Bool = false, where whereClause: String? = nil, order: String? = nil, limit: Int? = nil, table: String? = nil) throws -> EventLoopFuture<Self> {
        let this = Self.init()
        let properties = this.properties
        let selectFields = /*select?.map({this[keyPath: $0]}) ??*/ properties
        
        let tableName = table ?? Self.tableMeta.table
        
        var sql = "SELECT "
        sql += selectFields.map { "`\($0.key)`" }.joined(separator: ",")
        sql += "FROM " + Self.tableMeta.database.description + "." + tableName.description
        if final {
            sql += " FINAL"
        }
        if let whereClause = whereClause {
            sql += " WHERE \(whereClause)"
        }
        if let order = order {
            sql += " ORDER BY \(order)"
        }
        if let limit = limit {
            sql += " LIMIT \(limit)"
        }
        
        connection.logger.debug("\(sql)")
        
        return connection.query(sql: sql).map { res -> Self in
            res.columns.forEach { column in
                guard let prop = properties.first(where: {$0.key == column.name}) else {
                    return
                }
                prop.setClickHouseArray(data: column.values)
            }
            return this
        }
    }
}


public protocol ClickHouseColumnConvertible : Codable, AnyObject {
    var key: String         { get }
    var isPrimary: Bool     { get }
    var isOrderBy: Bool     { get }
    var partitionBy: Bool   { get }
    /// number of elements inside the data column array
    var count: Int { get }
    
    func setClickHouseArray(data: [ClickHouseDataType])
    //func reserveCapacity(_ capacity: Int)
    //func append(_ other: [ClickHouseDataType])
    func getRawDataArray() -> [ClickHouseDataType]
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
    
    public func setClickHouseArray(data: [ClickHouseDataType]) {
        guard let array = data as? [Value] else {
            fatalError("Received missmatching data types for column \(key)")
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
    
    public func reserveCapacity(_ capacity: Int) {
        wrappedValue.reserveCapacity(capacity)
    }
    public func append(_ other: [ClickHouseDataType]) {
        guard let array = other as? [Value] else {
            fatalError("Cannot append arrays of different datatypes in column \(key)")
        }
        wrappedValue += array
    }
    public func getRawDataArray() -> [ClickHouseDataType] {
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

