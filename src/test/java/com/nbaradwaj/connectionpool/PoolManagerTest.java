package com.nbaradwaj.connectionpool;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import com.nbaradwaj.connectionpool.ConnectionConfig;
import com.nbaradwaj.connectionpool.ConnectionItem;
import com.nbaradwaj.connectionpool.ConnectionPool;
import com.nbaradwaj.connectionpool.PoolManager;

public class PoolManagerTest {
	
	public static PoolManager manager;
	public static ConnectionConfig config;
	public static ConnectionPool pool;
	public static LinkedBlockingQueue<ConnectionItem> idleConnections;
	public static LinkedBlockingQueue<ConnectionItem> activeConnections;
	
	@BeforeClass
	public static void setup() {
		config = new ConnectionConfig();
		config.setDataSource(Mockito.mock(DataSource.class));
		pool = Mockito.mock(ConnectionPool.class);
		
		idleConnections = new LinkedBlockingQueue<ConnectionItem>();
		activeConnections = new LinkedBlockingQueue<ConnectionItem>();
		manager = new PoolManager(config, idleConnections, activeConnections, pool);
	}
	
	@AfterClass
	public static void cleanup() {
		idleConnections.clear();
		activeConnections.clear();
		manager = null;
	}

	/**
	 * Test to check if the pool has some idle connections when the pool manager
	 * tries to create and add some connections to the pool. This assumes that a connection creation is successful.
	 */
	@Test
	public void testAddingConnectionsToPool() throws SQLException {
		//Get a mock connection
		Connection connection = Mockito.mock(Connection.class);
		Mockito.when(config.getDataSource().getConnection()).thenReturn(connection);
		Mockito.when(connection.isValid((int) TimeUnit.MILLISECONDS.toSeconds(config.getValidationTimeout()))).thenReturn(true);
		
		manager.addConnections();
		assertFalse(idleConnections.size() == 0);
	}
	
	/**
	 * Test to check if the connection is removed from the pool when closed by the manager. The total
	 * number of connections in the pool should reduce by one.
	 */
	@Test
	public void testRemoveConnection() {
		ConnectionItem connection = Mockito.mock(ConnectionItem.class);
		idleConnections.add(connection);
		int count = idleConnections.size();
		manager.removeConnection(connection);
		assertTrue(idleConnections.size() == count-1);
	}

}
