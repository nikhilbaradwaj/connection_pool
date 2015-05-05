package com.nbaradwaj.connectionpool;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * This is the connection pool class implementation that provides the basic
 * pooling behavior.
 */
public interface ConnectionPool {

    /**
     * Gets a connection from the connection pool.
     * 
     * @return a valid connection from the pool.
     */
    Connection getConnection() throws SQLException;

    /**
     * Releases a connection back into the connection pool.
     * 
     * @param connection the connection to return to the pool
     * @throws java.sql.SQLException
     */
    void releaseConnection(Connection connection) throws SQLException;
}
