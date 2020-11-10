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

        let onCluster = cluster.map { "ON CLUSTER \($0)" } ?? ""
        let engineReplicated = "ReplicatedReplacingMergeTree('/clickhouse/{cluster}/tables/{database}.{table}/{shard}', '{replica}')"
        let engineNormal = "ReplacingMergeTree()"
        let engine = isCluster ? engineReplicated : engineNormal

        var query =
        """
        CREATE TABLE IF NOT EXISTS \(database).\(table) \(onCluster) (\(columnDescriptions))
        ENGINE = \(engine)
        PRIMARY KEY (\(ids.joined(separator: ",")))
        """
        if let partitionBy = partitionBy.first {
          query += "PARTITION BY toYYYYMM(toDateTime(\(partitionBy)))"
        }
        query += "ORDER BY (\(order.joined(separator: ",")))"
        return query
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
    
    /// Only include column rows where the isIncluded array is true.
    public func filter(_ isIncluded: [Bool]) {
        precondition(isIncluded.count == count)
        properties.forEach {
            $0.filter(isIncluded)
        }
    }
    
    public func append(_ other: Self) {
        zip(properties, other.properties).forEach {
            $0.0.append($0.1.getClickHouseArray())
        }
    }
    
    /// Reserve space in all data columns
    public func reserveCapacity(_ capacity: Int) {
        properties.forEach {
            $0.reserveCapacity(capacity)
        }
    }
    
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
        if let cluster = meta.cluster {
            let query = "DROP TABLE IF EXISTS \(meta.database).\(meta.table) ON CLUSTER \(cluster)"
            connection.logger.info("\(query)")
            // deletes on a cluster, return some information
            return connection.query(sql: query).transform(to: ())
        } else {
            let query = "DROP TABLE IF EXISTS \(meta.database).\(meta.table)"
            connection.logger.info("\(query)")
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
    
    /// Query data from database.
    /// If final ist set to true, all duplicate merges are ensured, but perfmrance suffers
    public static func select(on connection: ClickHouseConnectionProtocol, fields: [String]? = nil, final: Bool = false, where whereClause: String? = nil, order: String? = nil, limit: Int? = nil, offset: Int? = nil, table: TableModelMeta? = nil) throws -> EventLoopFuture<Self> {
        
        let meta = table ?? tableMeta
        let fields = fields ?? Self.init().properties.map { "`\($0.key)`" }
        
        var sql = "SELECT "
        sql += fields.joined(separator: ",")
        sql += "FROM \(meta.database).\(meta.table)"
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
            if let offset = offset {
                sql += ",\(offset)"
            }
        }
        return select(on: connection, sql: sql)
    }
}
