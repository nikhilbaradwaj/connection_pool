package com.nbaradwaj.connectionpool;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class ConnectionItemImpl implements ConnectionItem, Connection {
	
	private AtomicInteger state;
	private Connection connection;
	private ConnectionPool pool;
	private long id;

	@Override
	public AtomicInteger state() {
		return this.state;
	}
	
	@Override
	public Connection getConnection() {
		return connection;
	}
	
	public ConnectionItemImpl(Connection connection, ConnectionPool pool, long id) {
		this.connection = connection;
		this.pool = pool;
		this.state = new AtomicInteger(ConnectionItem.STATE_NOT_IN_USE);
		this.id = id;
	}

	@Override
	public boolean isWrapperFor(Class<?> arg0) throws SQLException {
		try {
			return this.connection.isWrapperFor(arg0);
		} catch (SQLException e) {
			throw e;
		}
	}

	@Override
	public <T> T unwrap(Class<T> arg0) throws SQLException {
		try {
			return this.connection.unwrap(arg0);
		} catch (SQLException e) {
			throw e;
		}
	}

	@Override
	public void clearWarnings() throws SQLException {
		try {
			this.connection.clearWarnings();
		} catch (SQLException e) {
			throw e;
		}
	}


	@Override
	public void close() throws SQLException {
		try {
			//TODO - close any open statements and rollback if required.
			this.pool.releaseConnection(this);
		} catch (SQLException e) {
			throw e;
		}
	}

	@Override
	public void commit() throws SQLException {
		try {
			this.connection.commit();
		} catch (SQLException e) {
			throw e;
		}
	}

	@Override
	public Array createArrayOf(String typeName, Object[] elements)
			throws SQLException {
		try {
			return this.connection.createArrayOf(typeName, elements);
		} catch (SQLException e) {
			throw e;
		}
	}

	@Override
	public Blob createBlob() throws SQLException {
		try {
			return this.connection.createBlob();
		} catch (SQLException e) {
			throw e;
		}
	}

	@Override
	public Clob createClob() throws SQLException {
		try {
			return this.connection.createClob();
		} catch (SQLException e) {
			throw e;
		}
	}

	@Override
	public NClob createNClob() throws SQLException {
		try {
			return this.connection.createNClob();
		} catch (SQLException e) {
			throw e;
		}
	}

	@Override
	public SQLXML createSQLXML() throws SQLException {
		try {
			return this.connection.createSQLXML();
		} catch (SQLException e) {
			throw e;
		}
	}

	@Override
	public Statement createStatement() throws SQLException {
		try {
			return this.connection.createStatement();
		} catch (SQLException e) {
			throw e;
		}
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency)
			throws SQLException {
		try {
			return this.connection.createStatement(resultSetType, resultSetConcurrency);
		} catch (SQLException e) {
			throw e;
		}
	}

	@Override
	public Statement createStatement(int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		try {
			return this.connection.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
		} catch (SQLException e) {
			throw e;
		}
	}

	@Override
	public Struct createStruct(String typeName, Object[] attributes)
			throws SQLException {
		try {
			return this.connection.createStruct(typeName, attributes);
		} catch (SQLException e) {
			throw e;
		}
	}

	@Override
	public boolean getAutoCommit() throws SQLException {
		try {
			return this.connection.getAutoCommit();
		} catch (SQLException e) {
			throw e;
		}
	}

	@Override
	public String getCatalog() throws SQLException {
		try {
			return this.connection.getCatalog();
		} catch (SQLException e) {
			throw e;
		}
	}

	@Override
	public Properties getClientInfo() throws SQLException {
		try {
			return this.connection.getClientInfo();
		} catch (SQLException e) {
			throw e;
		}
	}

	@Override
	public String getClientInfo(String name) throws SQLException {
		try {
			return this.connection.getClientInfo(name);
		} catch (SQLException e) {
			throw e;
		}
	}

	@Override
	public int getHoldability() throws SQLException {
		try {
			return this.connection.getHoldability();
		} catch (SQLException e) {
			throw e;
		}
	}

	@Override
	public DatabaseMetaData getMetaData() throws SQLException {
		try {
			return this.connection.getMetaData();
		} catch (SQLException e) {
			throw e;
		}
	}

	@Override
	public int getTransactionIsolation() throws SQLException {
		try {
			return this.connection.getTransactionIsolation();
		} catch (SQLException e) {
			throw e;
		}
	}

	@Override
	public Map<String, Class<?>> getTypeMap() throws SQLException {
		try {
			return this.connection.getTypeMap();
		} catch (SQLException e) {
			throw e;
		}
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		try {
			return this.connection.getWarnings();
		} catch (SQLException e) {
			throw e;
		}
	}

	@Override
	public boolean isClosed() throws SQLException {
		try {
			return this.connection.isClosed();
		} catch (SQLException e) {
			throw e;
		}
	}

	@Override
	public boolean isReadOnly() throws SQLException {
		try {
			return this.connection.isReadOnly();
		} catch (SQLException e) {
			throw e;
		}
	}

	@Override
	public boolean isValid(int timeout) throws SQLException {
		try {
			return this.connection.isValid(timeout);
		} catch (SQLException e) {
			throw e;
		}
	}

	@Override
	public String nativeSQL(String sql) throws SQLException {
		try {
			return this.connection.nativeSQL(sql);
		} catch (SQLException e) {
			throw e;
		}
	}

	@Override
	public CallableStatement prepareCall(String sql) throws SQLException {
		try {
			return this.connection.prepareCall(sql);
		} catch (SQLException e) {
			throw e;
		}
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType,
			int resultSetConcurrency) throws SQLException {
		try {
			return this.connection.prepareCall(sql);
		} catch (SQLException e) {
			throw e;
		}
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		try {
			return this.connection.prepareCall(sql, resultSetType, resultSetConcurrency);
		} catch (SQLException e) {
			throw e;
		}
	}

	@Override
	public PreparedStatement prepareStatement(String sql) throws SQLException {
		try {
			return this.connection.prepareStatement(sql);
		} catch (SQLException e) {
			throw e;
		}
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
			throws SQLException {
		try {
			return this.connection.prepareStatement(sql, autoGeneratedKeys);
		} catch (SQLException e) {
			throw e;
		}
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
			throws SQLException {
		try {
			return this.connection.prepareStatement(sql, columnIndexes);
		} catch (SQLException e) {
			throw e;
		}
	}

	@Override
	public PreparedStatement prepareStatement(String sql, String[] columnNames)
			throws SQLException {
		try {
			return this.connection.prepareStatement(sql, columnNames);
		} catch (SQLException e) {
			throw e;
		}
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType,
			int resultSetConcurrency) throws SQLException {
		try {
			return this.connection.prepareStatement(sql, resultSetType, resultSetConcurrency);
		} catch (SQLException e) {
			throw e;
		}
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		try {
			return this.connection.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
		} catch (SQLException e) {
			throw e;
		}
	}

	@Override
	public void releaseSavepoint(Savepoint savepoint) throws SQLException {
		try {
		    this.connection.releaseSavepoint(savepoint);
		} catch (SQLException e) {
			throw e;
		}
	}

	@Override
	public void rollback() throws SQLException {
		try {
			this.connection.rollback();
		} catch (SQLException e) {
			throw e;
		}
	}

	@Override
	public void rollback(Savepoint savepoint) throws SQLException {
		try {
			this.connection.rollback(savepoint);
		} catch (SQLException e) {
			throw e;
		}
	}

	@Override
	public void setAutoCommit(boolean autoCommit) throws SQLException {
		try {
			this.connection.setAutoCommit(autoCommit);
		} catch (SQLException e) {
			throw e;
		}
	}

	@Override
	public void setCatalog(String catalog) throws SQLException {
		try {
			this.connection.setCatalog(catalog);
		} catch (SQLException e) {
			throw e;
		}
	}

	@Override
	public void setClientInfo(Properties properties)
			throws SQLClientInfoException {
		try {
			this.connection.setClientInfo(properties);
		} catch (SQLClientInfoException e) {
			throw e;
		}
	}

	@Override
	public void setClientInfo(String name, String value)
			throws SQLClientInfoException {
		try {
			this.connection.setClientInfo(name, value);
		} catch (SQLClientInfoException e) {
			throw e;
		}
	}

	@Override
	public void setHoldability(int holdability) throws SQLException {
		try {
			this.connection.setHoldability(holdability);
		} catch (SQLException e) {
			throw e;
		}
	}

	@Override
	public void setReadOnly(boolean readOnly) throws SQLException {
		try {
			this.connection.setReadOnly(readOnly);
		} catch (SQLException e) {
			throw e;
		}
	}

	@Override
	public Savepoint setSavepoint() throws SQLException {
		try {
			return this.connection.setSavepoint();
		} catch (SQLException e) {
			throw e;
		}
	}

	@Override
	public Savepoint setSavepoint(String name) throws SQLException {
		try {
			return this.connection.setSavepoint(name);
		} catch (SQLException e) {
			throw e;
		}
	}

	@Override
	public void setTransactionIsolation(int level) throws SQLException {
		try {
			this.connection.setTransactionIsolation(level);
		} catch (SQLException e) {
			throw e;
		}
	}

	@Override
	public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
		try {
			this.connection.setTypeMap(map);
		} catch (SQLException e) {
			throw e;
		}
	}

}
