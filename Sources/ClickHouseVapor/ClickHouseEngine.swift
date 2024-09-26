//
//  ClickHouseEngine.swift
//  ClickHouseVapor
//
//  Created by Patrick Zippenfenig on 2020-11-11.
//

import Foundation
import enum ClickHouseNIO.ClickHouseTypeName

/// Abstract a clickHouse storage engine. For example the `ReplacingMergeTree` requires special CREATE syntax.
/// This could also be used to
public protocol ClickHouseEngine: Sendable {
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
        self.cluster != nil
    }

    /// Returns the table name and database name encoded with a dot
    public var tableWithDatabase: String {
        if let database = self.database {
            return "`\(database)`.`\(self.table)`"
        }
        return "`\(self.table)`"
    }
}

public struct ClickHouseEngineReplacingMergeTree: ClickHouseEngine {
    public let table: String
    public let database: String?
    public let cluster: String?
    public let partitionBy: String?

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
            query += " PARTITION BY (\(partitionBy))"
        }
        query += " ORDER BY (\(order.joined(separator: ",")))"
        return query
    }
}

extension ClickHouseTypeName {
    var supportsLowCardinality: Bool {
        // basically all numerical data types except for Decimal support LowCardinality
        // https://clickhouse.tech/docs/en/sql-reference/data-types/lowcardinality/
        switch self {
        case .float, .float64, .int8, .int16, .int32, .int64, .uint8, .uint16, .uint32, .uint64:
            return true
        case .uuid:
            return false
        case .fixedString, .string:
            return true
        case .nullable(let type):
            return type.supportsLowCardinality
        case .array, .boolean, .date, .date32, .dateTime, .dateTime64, .enum16, .enum8:
            return false
        }
    }
}
