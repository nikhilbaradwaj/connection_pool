package com.nbaradwaj.connectionpool;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import com.nbaradwaj.connectionpool.ConnectionConfig;
import com.nbaradwaj.connectionpool.ConnectionItem;
import com.nbaradwaj.connectionpool.ConnectionPoolImpl;

public class ConnectionPoolTest {	
	
	static ConnectionConfig config;
	static ConnectionPoolImpl pool;
	
	@BeforeClass
	public static void setup() throws SQLException {
		//Configuration with mock data source
		config = new ConnectionConfig();
		config.setDataSource(Mockito.mock(DataSource.class));
		//mock connection
		Connection connection = Mockito.mock(Connection.class);
		Mockito.when(config.getDataSource().getConnection()).thenReturn(connection);
		Mockito.when(connection.isValid((int) TimeUnit.MILLISECONDS.toSeconds(config.getValidationTimeout()))).thenReturn(true);
		//create the connection pool
		pool = new ConnectionPoolImpl(config);
	}
	
	/**
	 * When pool is created, it should have some connections ready to serve.
	 */
	@Test
	public void testConnectionsExistOnPoolCreation() {
		Assert.assertTrue(pool.totalConnections() > 0); 
	}
	
	/**
	 * Validate the states of the connection after the connection is given to the consumer. 
	 * @throws SQLException
	 */
	@Test
	public void testGetConnection() throws SQLException {
		int count = pool.idleConnectionsCount();
		Connection connection =  pool.getConnection(config.getConnectionTimeout());
		Assert.assertEquals(((ConnectionItem) connection).state().get() , ConnectionItem.STATE_IN_USE);
		Assert.assertEquals(count - 1 , pool.idleConnectionsCount());
	}
	
	/**
	 * Releasing a connection to the pool increases the total idle connections available by 1.
	 * @throws SQLException
	 */
	@Test
	public void testReleaseConnection() throws SQLException {
		Connection connection =  pool.getConnection(config.getConnectionTimeout());
		int count = pool.idleConnectionsCount();
		pool.releaseConnection(connection);
		Assert.assertEquals(count + 1, pool.idleConnectionsCount());
		Assert.assertEquals(((ConnectionItem) connection).state().get(), ConnectionItem.STATE_NOT_IN_USE);
	}
	
	/**
	 * If consumer closes the connection instead of calling ConnectionPool::releaseConnection,
	 * then release the connection to the pool instead of closing the internal SQL connection.
	 * @throws SQLException
	 */
	@Test
	public void testCloseInternalSQLConnection() throws SQLException {
		Connection connection =  pool.getConnection(config.getConnectionTimeout());
		int count = pool.idleConnectionsCount();
		connection.close();
		Assert.assertEquals(count + 1, pool.idleConnectionsCount());
		Assert.assertEquals(((ConnectionItem) connection).state().get(), ConnectionItem.STATE_NOT_IN_USE);
	}
	
	/**
	 * Getting connection when there are no idle connections in the pool after connection timeout should 
	 * throw SQL timeout exception
	 * 
	 */
	@Test(expected=SQLTimeoutException.class)
	public void testNoIdleConnectionsWhenGetConnection() throws SQLException {
		config.setConnectionTimeout(TimeUnit.SECONDS.toMillis(1));
		for (int i= 0; i < pool.totalConnections(); i++) {
			pool.getConnection();
		}
		pool.getConnection();
	}
	
	@AfterClass
	public static void cleanup() {
		config = null;
		pool = null;
	}

}
