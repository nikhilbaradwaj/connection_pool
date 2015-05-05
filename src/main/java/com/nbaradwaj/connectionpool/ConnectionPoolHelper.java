package com.nbaradwaj.connectionpool;

import java.sql.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ConnectionPoolHelper {
	
	 private static final Logger LOGGER = LoggerFactory.getLogger("ConnectionPool");

	/**
     * Close connection and eat any exception.
     *
     * @param connection the connection to close
     */
    public static void closeConnection(final Connection connection)
    {
       try {
          LOGGER.debug("Closing connection {}", connection);
          if (connection != null && !connection.isClosed()) {
             connection.close();
          }
       }
       catch (Throwable e) {
          LOGGER.debug("Exception closing connection {}", connection.toString(), e);
       }
    }
}  