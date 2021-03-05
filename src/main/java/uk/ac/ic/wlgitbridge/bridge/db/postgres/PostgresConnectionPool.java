package uk.ac.ic.wlgitbridge.bridge.db.postgres;

import org.apache.commons.dbcp2.BasicDataSource;
import uk.ac.ic.wlgitbridge.util.Log;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Wrapper around a Postgres connection pool
 *
 * This should be constructed once and passed to any
 * sub-systems that need to get connections from the pool.
 */
public class PostgresConnectionPool {

    private BasicDataSource pool;

    public PostgresConnectionPool(PostgresOptions options) {
        Log.info("Initialize postgres connection pool");
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setDriverClassName("org.postgresql.Driver");
        dataSource.setUrl(options.getUrl());
        dataSource.setUsername(options.getUsername());
        dataSource.setPassword(options.getPassword());
        int poolInitialSize = options.getPoolInitialSize();
        int poolMaxTotal = options.getPoolMaxTotal();
        int poolMaxWaitMillis = options.getPoolMaxWaitMillis();
        if (poolInitialSize < 1) {
            throw new RuntimeException("Invalid poolInitialSize: " + poolInitialSize);
        }
        if (poolMaxTotal < poolInitialSize) {
            throw new RuntimeException("Invalid poolMaxTotal and poolInitialSize: " + poolMaxTotal + ", " + poolInitialSize);
        }
        dataSource.setInitialSize(poolInitialSize);
        dataSource.setMaxTotal(poolMaxTotal);
        dataSource.setMaxWaitMillis(poolMaxWaitMillis);
        dataSource.setAutoCommitOnReturn(true);
        this.pool = dataSource;
    }

    public Connection getConnection() throws SQLException {
        return this.pool.getConnection();
    }

    public synchronized void close() {
        Log.info("Closing connection pool");
        try {
            this.pool.close();
        } catch (Exception e) {
            Log.error("Error while closing connection pool", e);
        }
    }
}
