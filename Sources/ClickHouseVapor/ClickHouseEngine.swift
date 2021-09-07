//
//  ClickHouseEngine.swift
//  ClickHouseVapor
//
//  Created by Patrick Zippenfenig on 2020-11-11.
//

import Foundation
import enum ClickHouseNIO.ClickHouseTypeName

/// Abstract a clickhouse storage engine. For example the `ReplacingMergeTree` requires special CREATE syntax.
/// This could also be used to
public protocol ClickHouseEngine {
    /// Generate a SQL query to create the table using the defined model columns
    func createTableQuery(columns: [ClickHouseColumnConvertible]) -> String
    
    /// Set if operations run on a cluster. In this case, the create statement will return some query data.
    var cluster: String? { get }
    
    /// Name of the table
    var table: String { get }
    
    /// Name of the database
    var database: String? { get }
}

extension ClickHouseEngine {
    public var isUsingCluster: Bool {
        cluster != nil
    }
    
    /// Returns the tablename and database name encoded with a dot
    public var tableWithDatabase: String {
        if let database = database {
            return "`\(database)`.`\(table)`"
        }
        return "`\(table)`"
    }
}

public struct ClickHouseEngineReplacingMergeTree: ClickHouseEngine {
    public var table: String
    public var database: String?
    public var cluster: String?
    public var partitionBy: String?
    
    public init(table: String, database: String?, cluster: String?, partitionBy: String?) {
        self.table = table
        self.database = database
        self.cluster = cluster
        self.partitionBy = partitionBy
    }

    public func createTableQuery(columns: [ClickHouseColumnConvertible]) -> String {
        let ids = columns.compactMap { $0.isPrimary ? $0.key : nil }
        assert(ids.count >= 1)
        let order = columns.compactMap { $0.isOrderBy ? $0.key : nil }
        assert(order.count >= 1)

        let columnDescriptions = columns.map { field -> String in
            if field.isLowCardinality && field.clickHouseTypeName().supportsLowCardinality {
                return "\(field.key) LowCardinality(\(field.clickHouseTypeName().string))"
            } else {
                return "\(field.key) \(field.clickHouseTypeName().string)"
            }
        }.joined(separator: ",")

        let onCluster = cluster.map { "ON CLUSTER \($0)" } ?? ""
        let engineReplicated = "ReplicatedReplacingMergeTree('/clickhouse/{cluster}/tables/{database}.{table}/{shard}', '{replica}')"
        let engineNormal = "ReplacingMergeTree()"
        let engine = cluster != nil ? engineReplicated : engineNormal

        var query =
        """
        CREATE TABLE IF NOT EXISTS \(tableWithDatabase) \(onCluster) (\(columnDescriptions))
        ENGINE = \(engine)
        PRIMARY KEY (\(ids.joined(separator: ",")))
        """
        if let partitionBy = partitionBy {
          query += "PARTITION BY (\(partitionBy))"
        }
        query += "ORDER BY (\(order.joined(separator: ",")))"
        return query
    }
}


extension ClickHouseTypeName {
    var supportsLowCardinality: Bool {
        // basically all numerical data types except for Decimal support LowCardinality
        // https://clickhouse.tech/docs/en/sql-reference/data-types/lowcardinality/
        switch self {
        case .float:
            return true
        case .float64:
            return true
        case .int8:
            return true
        case .int16:
            return true
        case .int32:
            return true
        case .int64:
            return true
        case .uint8:
            return true
        case .uint16:
            return true
        case .uint32:
            return true
        case .uint64:
            return true
        case .uuid:
            return false
        case .fixedString(_):
            return true
        case .string:
            return true
        }
    }
}
