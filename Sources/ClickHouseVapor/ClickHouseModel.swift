//
//  ClickHouseModel.swift
//  ClickHouseVapor
//
//  Created by Patrick Zippenfenig on 2020-11-10.
//

import Foundation
import Vapor
import ClickHouseNIO

public enum ClickHouseModelError: Error {
    case missmatchingDataType(columnName: String)
}

public struct TableModelMeta {
    var database: CustomStringConvertible
    var table: CustomStringConvertible
    var cluster: CustomStringConvertible?
    
    var isCluster: Bool {
        return cluster != nil
    }
    
    func createTableQuery(fields: [ClickHouseColumnConvertible]) -> String {
        let ids = fields.compactMap { $0.isPrimary ? $0.key : nil }
        assert(ids.count >= 1)
        let order = fields.compactMap { $0.isOrderBy ? $0.key : nil }
        assert(order.count >= 1)
        let partitionBy = fields.compactMap { $0.partitionBy ? $0.key : nil }
        assert(partitionBy.count <= 1)

        let columnDescriptions = fields.map { field -> String in
          return "\(field.key) \(field.clickHouseTypeName().string)"
        }.joined(separator: ",")

        let tableName = table

        let onCluster = cluster.map { "ON CLUSTER \($0)" } ?? ""

        let engineReplicated = "ReplicatedReplacingMergeTree('/clickhouse/{cluster}/tables/{database}.{table}/{shard}', '{replica}')"
        let engineNormal = "ReplacingMergeTree()"

        let engine = isCluster ? engineReplicated : engineNormal

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
        return query
    }
    
    func dropTableQuery() -> String {
        if let cluster = cluster {
            return "DROP TABLE IF EXISTS \(database).\(table) ON CLUSTER \(cluster)"
        }
        return "DROP TABLE IF EXISTS \(database).\(table)"
    }
}

public protocol ClickHouseModel: AnyObject {
    static var tableMeta: TableModelMeta { get }
    init()
}

extension ClickHouseModel {
    public var count: Int {
        return properties.first(where: {$0.count > 0})?.count ?? 0
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
    
    var properties: [ClickHouseColumnConvertible] {
        return Mirror(reflecting: self).children.compactMap {
            $0.value as? ClickHouseColumnConvertible
        }
    }
    
    public static func createTable(on connection: ClickHouseConnectionProtocol, table: TableModelMeta? = nil) throws -> EventLoopFuture<Void> {
        let fields = Self.init().properties
        let meta = table ?? tableMeta
        let query = meta.createTableQuery(fields: fields)
        connection.logger.debug("\(query)")
        
        if meta.isCluster {
            // cluster operation, return some information
            return connection.query(sql: query).transform(to: ())
        } else {
            return connection.command(sql: query)
        }
    }
    
    public func insert(on connection: ClickHouseConnectionProtocol, table: TableModelMeta? = nil) throws  -> EventLoopFuture<Void> {
        let fields = properties
        let meta = table ?? Self.tableMeta
        let data = fields.compactMap {
            return $0.count == 0 ? nil : ClickHouseColumn($0.key, $0.getClickHouseArray())
        }
        guard data.count > 0 else {
            // no values -> nothing to do
            return connection.eventLoop.makeSucceededFuture(())
        }
        
        return connection.insert(into: "\(meta.database).\(meta.table)", data: data)
    }
    
    /// Delete this table. This operation cannot be undone.
    public static func deleteTable(on connection: ClickHouseConnectionProtocol, table: TableModelMeta? = nil) throws -> EventLoopFuture<Void> {
        let meta = table ?? Self.tableMeta
        let query = meta.dropTableQuery()
        connection.logger.info("\(query)")
        if meta.isCluster {
            // deletes on a cluster, return some information
            return connection.query(sql: query).transform(to: ())
        } else {
            return connection.command(sql: query)
        }
    }
    
    /// Execute a SQL SELECT statemant and apply all returned columns to this entity
    public static func select(on connection: ClickHouseConnectionProtocol, sql: String) -> EventLoopFuture<Self> {
        connection.logger.debug("\(sql)")
        let this = Self.init()
        let properties = this.properties
        return connection.query(sql: sql).flatMapThrowing { res -> Self in
            try res.columns.forEach { column in
                guard let prop = properties.first(where: {$0.key == column.name}) else {
                    return
                }
                try prop.setClickHouseArray(data: column.values)
            }
            return this
        }
    }
    
    // select: [KeyPath<Self, ClickHouseColumnConvertible>]? = nil,
    /// If final ist set to true, all duplicate merges are ensured, but perfmrance suffers
    public static func select(on connection: ClickHouseConnectionProtocol, final: Bool = false, where whereClause: String? = nil, order: String? = nil, limit: Int? = nil, table: TableModelMeta? = nil) throws -> EventLoopFuture<Self> {
        
        let this = Self.init()
        let properties = this.properties
        let selectFields = /*select?.map({this[keyPath: $0]}) ??*/ properties
        
        let tableName = table?.table ?? Self.tableMeta.table
        
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
        return Self.select(on: connection, sql: sql)
    }
}

