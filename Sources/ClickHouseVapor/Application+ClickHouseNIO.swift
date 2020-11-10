//
//  Application+ClickHouseNIO.swift
//  ClickHouseVapor
//
//  Created by Patrick Zippenfenig on 2020-11-10.
//

import Vapor
import ClickHouseNIO

@_exported import struct ClickHouseNIO.ClickHouseConfiguration

extension ClickHouseConnection: ConnectionPoolItem { }

public final class ClickHouseConnectionSource: ConnectionPoolSource {
    private let configuration: ClickHouseConfiguration

    public init(configuration: ClickHouseConfiguration) {
        self.configuration = configuration
    }
    public func makeConnection(logger: Logger, on eventLoop: EventLoop) -> EventLoopFuture<ClickHouseConnection> {
        return ClickHouseConnection.connect(configuration: configuration, on: eventLoop, logger: logger)
    }
}


extension Application {
    public var clickHouse: ClickHouse {
        .init(application: self)
    }

    public struct ClickHouse {
        struct ConfigurationKey: StorageKey {
            typealias Value = ClickHouseConfiguration
        }

        public var configuration: ClickHouseConfiguration? {
            get {
                self.application.storage[ConfigurationKey.self]
            }
            nonmutating set {
                self.application.storage[ConfigurationKey.self] = newValue
            }
        }


        struct PoolKey: StorageKey, LockKey {
            typealias Value = EventLoopGroupConnectionPool<ClickHouseConnectionSource>
        }

        internal var pool: EventLoopGroupConnectionPool<ClickHouseConnectionSource> {
            if let existing = self.application.storage[PoolKey.self] {
                return existing
            } else {
                let lock = self.application.locks.lock(for: PoolKey.self)
                lock.lock()
                defer { lock.unlock() }
                guard let configuration = self.configuration else {
                    fatalError("ClickHouse not configured. Use app.clickHouse.configuration = ...")
                }
                let new = EventLoopGroupConnectionPool(
                    source: ClickHouseConnectionSource(configuration: configuration),
                    maxConnectionsPerEventLoop: 2,
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

extension Application.ClickHouse {
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

extension Request.ClickHouse {
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
