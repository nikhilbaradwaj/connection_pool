package com.nbaradwaj.connectionpool;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This is the connection pool class implementation that provides the basic
 * pooling behavior.
 *
 * @author Nikhil Baradwaj
 */
public class ConnectionPoolImpl implements ConnectionPool {
	/**
	 * Logger 
	 */
	protected static final Logger LOGGER = LoggerFactory.getLogger("ConnectionPool");
	
	/**
	 * Configuration parameters of the connection pool
	 */
	private ConnectionConfig config;
	
	/**
	 * This restricts the number of consumers of the connection pool at any given time
	 */
	private Semaphore poolLock;
	
	/**
	 * This is the list that holds all the open idle connections
	 */
	private LinkedBlockingQueue<ConnectionItem> idleConnections;
	
	/**
	 * This is the list that holds all the active connections currently in use
	 */
	private LinkedBlockingQueue<ConnectionItem> activeConnections;
	
	/**
	 * The pool manager instance manages the house keeping of the connections in the pool
	 * and schedules tasks for connection leak detection, connection validity etc. It
	 * also fills the pool with minimum connections when the number drops below a threshold.
	 */
	private PoolManager poolManager;
	
	/**
     * Construct with the specified configuration.
     *
     * @param configuration A ConnectionConfig instance
     */
	public ConnectionPoolImpl(ConnectionConfig config) {
		//Read the configuration for the connection pool
		this.config = config;
		
		// Set the maximum number of consumers for the connection pool
		this.poolLock = new Semaphore(this.config.getMaximumPoolConsumers());
		
		//initialize the data structures that hold the connections in the pool.
		idleConnections = new LinkedBlockingQueue<ConnectionItem>();
		activeConnections = new LinkedBlockingQueue<ConnectionItem>();
		
		//Create a pool manager and initialize the connection pool
		this.getPoolManager().addConnections();
	}

	@Override
	public Connection getConnection() throws SQLException {
		return this.getConnection(this.config.getConnectionTimeout());
	}

	@Override
	public void releaseConnection(Connection connection) throws SQLException {
		this.releaseConnection((ConnectionItem) connection);
	}
	
	/**
	* Get a connection from the pool, or timeout after the specified number of milliseconds.
	*
	* @param connectionTimeout the maximum time to wait for a connection from the pool
	* @return a java.sql.Connection instance
	* @throws SQLException thrown if a timeout occurs trying to obtain a connection
	*/
	protected Connection getConnection(final long connectionTimeout) throws SQLException
	{	
		poolLock.acquireUninterruptibly();
		long timeout = connectionTimeout;
		final long start = System.currentTimeMillis();
		
		try {
			do {
				final ConnectionItemImpl connectionItem = (ConnectionItemImpl) this.idleConnections.poll(timeout, TimeUnit.MILLISECONDS);
				if (connectionItem == null) {
					break; // We timed out.
				}
	
				final long now = System.currentTimeMillis();
				if (connectionItem.state().intValue() == ConnectionItem.STATE_EVICTED || !connectionItem.isValid((int) TimeUnit.MILLISECONDS.toSeconds(this.config.getValidationTimeout()))) {
					this.getPoolManager().removeConnection(connectionItem); // Throw away the dead connection and try again
					timeout = connectionTimeout - (now - start);
				}
				else {
					//TODO start the leak test task
					activeConnections.add(connectionItem);
					connectionItem.state().set(ConnectionItem.STATE_IN_USE);
					return connectionItem;
				}
			}
			while (timeout > 0L);
		}
	  catch (InterruptedException e) {
	     throw new SQLException("Interrupted during connection acquisition", e);
	  }
	  finally {
	     poolLock.release();
	  }
	
	  logPoolState("Timeout failure ");
	  throw new SQLTimeoutException(String.format("Timeout after %dms of waiting for a connection.", (System.currentTimeMillis() - start)));
	}
	
	
	/**
	 * Release a connection back to the pool, or permanently close it if it is broken.
	 *
	 * @param bagEntry the PoolBagEntry to release back to the pool
	 */
    public final void releaseConnection(final ConnectionItem connection)
    {
       if (connection.state().get() == ConnectionItem.STATE_EVICTED) {
          LOGGER.debug("Connection returned to pool {} is broken or evicted.  Closing connection.", this.config.getPoolName());
          getPoolManager().removeConnection(connection);
       }
       else {
    	   connection.state().set(ConnectionItem.STATE_NOT_IN_USE);
    	   activeConnections.remove(connection);
    	   idleConnections.add(connection);
       }
    }
    
    /**
     * Return the instance of the pool manager.
     * @return
     */
    private PoolManager getPoolManager() { 
 	   if (this.poolManager == null) {
	 	   this.poolManager = new PoolManager(this.config, this.idleConnections, this.activeConnections, this);
 	   }
 	   return this.poolManager;
    }
    
    public final void logPoolState(String... prefix)
    {
       if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("{}pool stats {} (total={}, inUse={}, avail={}, waiting={})",
                       (prefix.length > 0 ? prefix[0] : ""), this.config.getPoolName(),
                       idleConnections.size() + activeConnections.size(), activeConnections.size(), idleConnections.size(), 0);
       }
    }
    
    public int totalConnections() {
    	return idleConnections.size() + activeConnections.size();
    }
    
    public int idleConnectionsCount() {
    	return idleConnections.size();
    }

}
