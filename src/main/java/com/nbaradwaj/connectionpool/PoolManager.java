package com.nbaradwaj.connectionpool;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * The pool manager class manages the house keeping of the connections in the pool
 * and schedules tasks for connection leak detection, connection validity etc.
 *
 * @author Nikhil Baradwaj
 *
 */
public class PoolManager {
	
	protected static final Logger LOGGER = LoggerFactory.getLogger("ConnectionPool");
	private ConnectionConfig config;
	private LinkedBlockingQueue<ConnectionItem> idleConnections;
	private LinkedBlockingQueue<ConnectionItem> activeConnections;
	private ConnectionPool pool;
	private static AtomicLong nextConnectionId;
	
	/**
	 * Constructor
	 * @param config The configuration of the connection pool
	 * @param idleConnections The list containing all the idle connections
	 * @param connectionsInUse The list containing all the active connections
	 */
	public PoolManager(ConnectionConfig config, LinkedBlockingQueue<ConnectionItem> idleConnections, LinkedBlockingQueue<ConnectionItem> activeConnections, ConnectionPool pool) {
		this.config = config;
		this.idleConnections = idleConnections;
		this.activeConnections = activeConnections;
		this.pool = pool;
	}
	
	/**
	 * Adds a minimum number of connections to the pool to prepare for serving the clients
	 */
	public void addConnections() {
		final int connectionsToAdd;
		//Make sure the number of connections to add does not overflow the max size of the pool.
		if (config.getMaximumPoolSize() - (idleConnections.size() + activeConnections.size()) > config.getMinimumIdleConnections() - idleConnections.size()) {
			connectionsToAdd = config.getMaximumPoolSize() - (idleConnections.size() + activeConnections.size());
		} else {
			connectionsToAdd = config.getMinimumIdleConnections() - idleConnections.size();
		}
		
		//Check if creating a single connection is working. If not, there might be something wrong with the datasource,
		//in which case we might not be able to add more connections to the pool.
		
		//If the connection can be created, let's start some threads to create the connections and add them to the pool.
        for (int i = 0; i < connectionsToAdd; i++) {
     	  try {
 			  addConnection();
	 	  } catch (SQLException e) {
	 		  e.printStackTrace();
		  }
        }
	}
	
	/**
	 * Adds a single connection to the pool.
	 * @return Boolean success or failure of connection creation.
	 * @throws SQLException
	 */
	private boolean addConnection() throws SQLException {
		Connection connection = null;
         try {
        	 final String username = this.config.getUsername();
        	 final String password = this.config.getPassword(); 
            connection = (username == null && password == null) ? this.config.getDataSource().getConnection() : this.config.getDataSource().getConnection(username, password);
            
            if (!connection.isValid((int) TimeUnit.MILLISECONDS.toSeconds(this.config.getValidationTimeout()))) {
            	throw new SQLException("Connection is not valid.");
            }
            
            this.idleConnections.add(new ConnectionItemImpl(connection, this.pool, nextConnectionId.getAndIncrement()));
            return true;
         }
         catch (Exception e) {
        	 ConnectionPoolHelper.closeConnection(connection);
            LOGGER.debug("Connection attempt to database {} failed: {}", this.config.getPoolName(), e.getMessage(), e);
         }
         return false;
      }
	
	/**
	 * Closes the connection and removes the connection from the connection pool
	 * @param connection
	 */
	public void removeConnection(ConnectionItem connection) {
		ConnectionPoolHelper.closeConnection(connection.getConnection());
		if (idleConnections.contains(connection)) {
			idleConnections.remove(connection);
		}
		if (activeConnections.contains(connection)) {
			activeConnections.remove(connection);
		}
		LOGGER.debug("Removing connection {} from the pool", connection.toString());
	}
}
