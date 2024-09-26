//
//  Application+ClickHouseNIO.swift
//  ClickHouseVapor
//
//  Created by Patrick Zippenfenig on 2020-11-10.
//

import ClickHouseNIO
import Vapor

@_exported import struct NIO.TimeAmount

@_exported import struct ClickHouseNIO.ClickHouseDate
@_exported import struct ClickHouseNIO.ClickHouseDate32
@_exported import struct ClickHouseNIO.ClickHouseDateTime
@_exported import struct ClickHouseNIO.ClickHouseDateTime64
@_exported import struct ClickHouseNIO.ClickHouseEnum8
@_exported import struct ClickHouseNIO.ClickHouseEnum16

/// Vapor `Application.ClickHouse` and `Request.ClickHouse` implement this procotol to be used later for queries
public protocol ClickHouseConnectionProtocol {
    var eventLoop: EventLoop { get }
    var logger: Logger { get }

    func ping() -> EventLoopFuture<Void>
    func query(sql: String) -> EventLoopFuture<ClickHouseQueryResult>
    func command(sql: String) -> EventLoopFuture<Void>
    func insert(into table: String, data: [ClickHouseColumn]) -> EventLoopFuture<Void>
}

/// ClickHouse hostname, credentials and pool configuration
public struct ClickHousePoolConfiguration {
    public let configuration: ClickHouseConfiguration
    public let maxConnectionsPerEventLoop: Int
    public let requestTimeout: TimeAmount

    public init(
        hostname: String = "localhost",
        port: Int = ClickHouseConnection.defaultPort,
        user: String? = nil,
        password: String? = nil,
        database: String? = nil,
        maxConnectionsPerEventLoop: Int = 1,
        requestTimeout: TimeAmount = .seconds(10)
    ) throws {
        self.configuration = try ClickHouseConfiguration(
            hostname: hostname,
            port: port,
            user: user,
            password: password,
            database: database
        )
        self.maxConnectionsPerEventLoop = maxConnectionsPerEventLoop
        self.requestTimeout = requestTimeout
    }
}

/// Make ClickHouse Connection work with the connection pool
extension ClickHouseConnection: ConnectionPoolItem { }

extension ClickHouseConfiguration: ConnectionPoolSource {
    public func makeConnection(logger: Logger, on eventLoop: EventLoop) -> EventLoopFuture<ClickHouseConnection> {
        return ClickHouseConnection.connect(configuration: self, on: eventLoop, logger: logger)
    }
}

extension Application {
    public var clickHouse: ClickHouse {
        .init(application: self)
    }

    public struct ClickHouse {
        struct ConfigurationKey: StorageKey {
            typealias Value = ClickHousePoolConfiguration
        }

        public var configuration: ClickHousePoolConfiguration? {
            get {
                self.application.storage[ConfigurationKey.self]
            }
            nonmutating set {
                self.application.storage[ConfigurationKey.self] = newValue
            }
        }

        struct PoolKey: StorageKey, LockKey {
            typealias Value = EventLoopGroupConnectionPool<ClickHouseConfiguration>
        }

        internal var pool: EventLoopGroupConnectionPool<ClickHouseConfiguration> {
            let lock = self.application.locks.lock(for: PoolKey.self)
            lock.lock()
            defer { lock.unlock() }

            if let existing = self.application.storage[PoolKey.self] {
                return existing
            } else {
                guard let configuration = self.configuration else {
                    fatalError("ClickHouse not configured. Use app.clickHouse.configuration = ...")
                }
                let new = EventLoopGroupConnectionPool(
                    source: configuration.configuration,
                    maxConnectionsPerEventLoop: configuration.maxConnectionsPerEventLoop,
                    requestTimeout: configuration.requestTimeout,
                    logger: self.application.logger,
                    on: self.application.eventLoopGroup
                )
                self.application.storage.set(PoolKey.self, to: new) {
                    $0.shutdown()
                }
                return new
            }
        }

        let application: Application
    }
}

extension Application.ClickHouse: ClickHouseConnectionProtocol {
    public var eventLoop: EventLoop {
        return application.eventLoopGroup.next()
    }

    public var logger: Logger {
        return application.logger
    }

    public func ping() -> EventLoopFuture<Void> {
        pool.withConnection {
            $0.ping()
        }
    }

    public func query(sql: String) -> EventLoopFuture<ClickHouseQueryResult> {
        pool.withConnection {
            $0.query(sql: sql)
        }
    }

    public func command(sql: String) -> EventLoopFuture<Void> {
        pool.withConnection {
            $0.command(sql: sql)
        }
    }

    public func insert(into table: String, data: [ClickHouseColumn]) -> EventLoopFuture<Void> {
        pool.withConnection {
            $0.insert(into: table, data: data)
        }
    }
}

extension Request {
    public var clickHouse: ClickHouse {
        .init(request: self)
    }

    public struct ClickHouse {
        let request: Request
    }
}

extension Request.ClickHouse: ClickHouseConnectionProtocol {
    public var eventLoop: EventLoop {
        return request.eventLoop
    }

    public var logger: Logger {
        return request.logger
    }

    public func ping() -> EventLoopFuture<Void> {
        self.request.application.clickHouse.pool.withConnection {
            $0.ping()
        }
    }

    public func query(sql: String) -> EventLoopFuture<ClickHouseQueryResult> {
        self.request.application.clickHouse.pool.withConnection {
            $0.query(sql: sql)
        }
    }

    public func command(sql: String) -> EventLoopFuture<Void> {
        self.request.application.clickHouse.pool.withConnection {
            $0.command(sql: sql)
        }
    }

    public func insert(into table: String, data: [ClickHouseColumn]) -> EventLoopFuture<Void> {
        self.request.application.clickHouse.pool.withConnection {
            $0.insert(into: table, data: data)
        }
    }
}
