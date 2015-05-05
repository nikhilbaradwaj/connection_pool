package com.nbaradwaj.connectionpool;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.Enumeration;
import java.util.Properties;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectionConfig {
	
	/**
	 * Timeout and size constants
	 */
	private static final long CONNECTION_TIMEOUT = TimeUnit.SECONDS.toMillis(30);
	private static final long VALIDATION_TIMEOUT = TimeUnit.SECONDS.toMillis(5);
	private static final long MAX_LIFETIME = TimeUnit.MINUTES.toMillis(30);
	private static final int MAX_POOL_SIZE = 10;
	private static final int MAX_POOL_CONSUMERS = 1000;
	
	/**
	 * Timeout and size parameters of the connection pool
	 */
	private volatile long connectionTimeout;
    private volatile long validationTimeout;
    private volatile long leakDetectionThreshold;
    private volatile long maxLifetime;
    private volatile int maxPoolSize;
    private volatile int minIdleConnections;
    private volatile int maxPoolConsumers;

    /**
     * Properties of the data source and connection
     */
    private String catalog;
    private String connectionTestQuery;
    private String dataSourceClassName;
    private String dataSourceJndiName;
    private String driverClassName;
    private String jdbcUrl;
    private String password;
    private DataSource dataSource;
    private String username;
    private Boolean isAutoCommit;
    
    private String poolName;
    
    public boolean useJdbcValidation;
    private Properties dataSourceProperties;
    private ThreadFactory threadFactory; 

	   /**
	    * Default constructor
	    */
	   public ConnectionConfig()
	   {
	      dataSourceProperties = new Properties();
	      
	      connectionTimeout = CONNECTION_TIMEOUT;
	      validationTimeout = VALIDATION_TIMEOUT;
	      isAutoCommit = true;
	      minIdleConnections = -1;
	      useJdbcValidation = false;
	      maxPoolSize = MAX_POOL_SIZE;
	      maxLifetime = MAX_LIFETIME;
	      maxPoolConsumers = MAX_POOL_CONSUMERS;
	      
	      String systemProp = System.getProperty("connectionPool.configurationFile");
	      if ( systemProp != null) {
	         loadProperties(systemProp);
	      }
	   }
	   
	   /**
	    * Construct a ConnectionConfig from the specified property file name.  <code>propertyFileName</code>
	    * will first be treated as a path in the file-system, and if that fails the 
	    * ClassLoader.getResourceAsStream(propertyFileName) will be tried.
	    *
	    * @param propertyFileName the name of the property file
	    */
	   public ConnectionConfig(String propertyFileName)
	   {
	      this();

	      loadProperties(propertyFileName);
	   }

	   /**
	    * Get the default catalog name to be set on connections.
	    *
	    * @return the default catalog name
	    */
	   public String getCatalog()
	   {
	      return catalog;
	   }

	   /**
	    * Set the default catalog name to be set on connections.
	    *
	    * @param catalog the catalog name, or null
	    */
	   public void setCatalog(String catalog)
	   {
	      this.catalog = catalog;
	   }

	   /**
	    * Get the SQL query to be executed to test the validity of connections.
	    * 
	    * @return the SQL query string, or null 
	    */
	   public String getConnectionTestQuery()
	   {
	      return connectionTestQuery;
	   }

	   /**
	    * Set the SQL query to be executed to test the validity of connections. Using
	    * the JDBC4 <code>Connection.isValid()</code> method to test connection validity can
	    * be more efficient on some databases and is recommended.
	    *
	    * @param connectionTestQuery a SQL query string
	    */
	   public void setConnectionTestQuery(String connectionTestQuery)
	   {
	      this.connectionTestQuery = connectionTestQuery;
	   }
	   
	   public long getConnectionTimeout()
	   {
	      return connectionTimeout;
	   }
	   
	   public int getMaximumPoolConsumers() {
		   return maxPoolConsumers;
	   }
	   
	   public void setConnectionTimeout(long connectionTimeoutMs)
	   {
	      if (connectionTimeoutMs == 0) {
	         this.connectionTimeout = Integer.MAX_VALUE;
	      }
	      else if (connectionTimeoutMs < 1000) {
	         throw new IllegalArgumentException("connectionTimeout cannot be less than 1000ms");
	      }
	      else {
	         this.connectionTimeout = connectionTimeoutMs;
	      }
	   }

	   public long getValidationTimeout()
	   {
	      return validationTimeout;
	   }

	   public void setValidationTimeout(long validationTimeoutMs)
	   {
	      if (validationTimeoutMs < 1000) {
	         throw new IllegalArgumentException("validationTimeout cannot be less than 1000ms");
	      }
	      else {
	         this.validationTimeout = validationTimeoutMs;
	      }
	   }

	   /**
	    * Get the {@link DataSource} that has been explicitly specified to be wrapped by the
	    * pool.
	    *
	    * @return the {@link DataSource} instance, or null
	    */
	   public DataSource getDataSource()
	   {
	      return dataSource;
	   }

	   /**
	    * Set a {@link DataSource} for the pool to explicitly wrap.  This setter is not
	    * available through property file based initialization.
	    *
	    * @param dataSource a specific {@link DataSource} to be wrapped by the pool
	    */
	   public void setDataSource(DataSource dataSource)
	   {
	      this.dataSource = dataSource;
	   }

	   public String getDataSourceClassName()
	   {
	      return dataSourceClassName;
	   }

	   public void setDataSourceClassName(String className)
	   {
	      this.dataSourceClassName = className;
	   }

	   public void addDataSourceProperty(String propertyName, Object value)
	   {
	      dataSourceProperties.put(propertyName, value);
	   }

	   public String getDataSourceJNDI()
	   {
	      return this.dataSourceJndiName;
	   }

	   public void setDataSourceJNDI(String jndiDataSource)
	   {
	      this.dataSourceJndiName = jndiDataSource;
	   }

	   public Properties getDataSourceProperties()
	   {
	      return dataSourceProperties;
	   }

	   public void setDataSourceProperties(Properties dsProperties)
	   {
	      dataSourceProperties.putAll(dsProperties);
	   }

	   public void setDriverClassName(String driverClassName)
	   {
	      try {
	         Class<?> driverClass = this.getClass().getClassLoader().loadClass(driverClassName);
	         driverClass.newInstance();
	         this.driverClassName = driverClassName;
	      }
	      catch (Exception e) {
	         throw new RuntimeException("driverClassName specified class '" + driverClassName + "' could not be loaded", e);
	      }
	   }	   

	   public String getJdbcUrl()
	   {
	      return jdbcUrl;
	   }

	   public void setJdbcUrl(String jdbcUrl)
	   {
	      this.jdbcUrl = jdbcUrl;
	   }

	   /**
	    * Get the default auto-commit behavior of connections in the pool.
	    *
	    * @return the default auto-commit behavior of connections
	    */
	   public boolean isAutoCommit()
	   {
	      return isAutoCommit;
	   }

	   /**
	    * Set the default auto-commit behavior of connections in the pool.
	    *
	    * @param isAutoCommit the desired auto-commit default for connections
	    */
	   public void setAutoCommit(boolean isAutoCommit)
	   {
	      this.isAutoCommit = isAutoCommit;
	   }
	   
	   public long getLeakDetectionThreshold()
	   {
	      return leakDetectionThreshold;
	   }

	   public void setLeakDetectionThreshold(long leakDetectionThresholdMs)
	   {
	      this.leakDetectionThreshold = leakDetectionThresholdMs;
	   }

	   public long getMaxLifetime()
	   {
	      return maxLifetime;
	   }

	   public void setMaxLifetime(long maxLifetimeMs)
	   {
	      this.maxLifetime = maxLifetimeMs;
	   }

	   public int getMaximumPoolSize()
	   {
	      return maxPoolSize;
	   }

	   public void setMaximumPoolSize(int maxPoolSize)
	   {
	      if (maxPoolSize < 1) {
	         throw new IllegalArgumentException("maxPoolSize cannot be less than 1");
	      }
	      this.maxPoolSize = maxPoolSize;
	   }

	   public int getMinimumIdleConnections()
	   {
	      return minIdleConnections;
	   }

	   public void getMinimumIdleConnections(int minIdleConnections)
	   {
	      if (minIdleConnections < 0) {
	         throw new IllegalArgumentException("minimumIdle cannot be negative");
	      }
	      this.minIdleConnections = minIdleConnections;
	   }

	   /**
	    * Get the default password to use for DataSource.getConnection(username, password) calls.
	    * @return the password
	    */
	   public String getPassword()
	   {
	      return password;
	   }

	   /**
	    * Set the default password to use for DataSource.getConnection(username, password) calls.
	    * @param password the password
	    */
	   public void setPassword(String password)
	   {
	      this.password = password;
	   }

	   public String getPoolName()
	   {
	      return poolName;
	   }

	   /**
	    * Set the name of the connection pool.  This is primarily used for the MBean
	    * to uniquely identify the pool configuration.
	    *
	    * @param poolName the name of the connection pool to use
	    */
	   public void setPoolName(String poolName)
	   {
	      this.poolName = poolName;
	   }

	   
	   /**
	    * Get the default username used for DataSource.getConnection(username, password) calls.
	    *
	    * @return the username
	    */
	   public String getUsername()
	   {
	      return username;
	   }

	   /**
	    * Set the default username used for DataSource.getConnection(username, password) calls.
	    *
	    * @param username the username
	    */
	   public void setUsername(String username)
	   {
	      this.username = username;
	   }

	   /**
	    * Get the thread factory used to create threads.
	    *
	    * @return the thread factory (may be null, in which case the default thread factory is used)
	    */
	   public ThreadFactory getThreadFactory()
	   {
	      return threadFactory;
	   }

	   /**
	    * Set the thread factory to be used to create threads.
	    *
	    * @param threadFactory the thread factory (setting to null causes the default thread factory to be used)
	    */
	   public void setThreadFactory(ThreadFactory threadFactory)
	   {
	      this.threadFactory = threadFactory;
	   }

	   public void validate()
	   {
	      Logger logger = LoggerFactory.getLogger(getClass());

	      validateNumerics();

	      if (driverClassName != null && jdbcUrl == null) {
	         logger.error("when specifying driverClassName, jdbcUrl must also be specified");
	         throw new IllegalStateException("when specifying driverClassName, jdbcUrl must also be specified");
	      }
	      else if (driverClassName != null && dataSourceClassName != null) {
	         logger.error("both driverClassName and dataSourceClassName are specified, one or the other should be used");
	         throw new IllegalStateException("both driverClassName and dataSourceClassName are specified, one or the other should be used");
	      }
	      else if (jdbcUrl != null) {
	         // OK
	      }
	      else if (dataSource == null && dataSourceClassName == null) {
	         logger.error("one of either dataSource, dataSourceClassName, or jdbcUrl and driverClassName must be specified");
	         throw new IllegalArgumentException("one of either dataSource or dataSourceClassName must be specified");
	      }
	      else if (dataSource != null && dataSourceClassName != null) {
	         logger.warn("both dataSource and dataSourceClassName are specified, ignoring dataSourceClassName");
	      }	      
	      if (poolName == null) {
	         poolName = "ConnecitonPool";
	      }	      
	   }

	   private void validateNumerics()
	   {
	      Logger logger = LoggerFactory.getLogger(getClass());

	      if (validationTimeout > connectionTimeout && connectionTimeout != 0) {
	         logger.warn("validationTimeout is greater than connectionTimeout, setting validationTimeout to connectionTimeout.");
	         validationTimeout = connectionTimeout;
	      }

	      if (minIdleConnections < 0 || minIdleConnections > maxPoolSize) {
	         minIdleConnections = maxPoolSize;
	      }

	      if (maxLifetime < 0) {
	         logger.error("maxLifetime cannot be negative.");
	         throw new IllegalArgumentException("maxLifetime cannot be negative.");
	      }
	      else if (maxLifetime > 0 && maxLifetime < TimeUnit.SECONDS.toMillis(30)) {
	         logger.warn("maxLifetime is less than 30000ms, using default {}ms.", MAX_LIFETIME);
	         maxLifetime = MAX_LIFETIME;
	      }
	      
	      if (leakDetectionThreshold != 0 && leakDetectionThreshold < TimeUnit.SECONDS.toMillis(2)) {
	         logger.warn("leakDetectionThreshold is less than 2000ms, setting to minimum 2000ms.");
	         leakDetectionThreshold = 2000L;
	      }
	   }
	   
	   protected void loadProperties(String propertyFileName)
	   {
	      final File propFile = new File(propertyFileName);
	      try {
		      final InputStream stream = new FileInputStream(propFile);
		      if (stream != null) {
	            Properties props = new Properties();
	            props.load(stream);
	            this.setProperties(props);
	         }
	      }   
	      catch (IOException io) {
	         throw new RuntimeException("Error loading properties file", io);
	      }
	   }	
	   
	   private void setProperties(Properties properties)
	   {
	      if (properties == null) {
	         return;
	      }

	      Enumeration<?> propertyNames = properties.propertyNames();
	      while (propertyNames.hasMoreElements()) {
	         Object key = propertyNames.nextElement();
	         String propName = key.toString();
	         Object propValue = properties.getProperty(propName);
	         if (propValue == null) {
	            propValue = properties.get(key);
	         }

	         if (propName.startsWith("dataSource.")) {
	            this.addDataSourceProperty(propName.substring("dataSource.".length()), propValue);
	         }
	         else {
	            this.setProperty(propName, propValue);
	         }
	      }
	   }
	   
	   private void setProperty(String propName, Object propValue)
	   {
		  Logger logger = LoggerFactory.getLogger(getClass());
		   
	      String capitalized = "set" + propName.substring(0, 1).toUpperCase() + propName.substring(1);
	      PropertyDescriptor propertyDescriptor;
	      try {
	         propertyDescriptor = new PropertyDescriptor(propName, this.getClass(), null, capitalized);
	      }
	      catch (IntrospectionException e) {
	         capitalized = "set" + propName.toUpperCase();
	         try {
	            propertyDescriptor = new PropertyDescriptor(propName, this.getClass(), null, capitalized);
	         }
	         catch (IntrospectionException e1) {
	            logger.error("Property {} does not exist on target {}", propName, this.getClass());
	            throw new RuntimeException(e);
	         }
	      }

	      try {
	         Method writeMethod = propertyDescriptor.getWriteMethod();
	         Class<?> paramClass = writeMethod.getParameterTypes()[0];
	         if (paramClass == int.class) {
	            writeMethod.invoke(this, Integer.parseInt(propValue.toString()));
	         }
	         else if (paramClass == long.class) {
	            writeMethod.invoke(this, Long.parseLong(propValue.toString()));
	         }
	         else if (paramClass == boolean.class) {
	            writeMethod.invoke(this, Boolean.parseBoolean(propValue.toString()));
	         }
	         else if (paramClass == String.class) {
	            writeMethod.invoke(this, propValue.toString());
	         }
	         else {
	            writeMethod.invoke(this, propValue);
	         }
	      }
	      catch (Exception e) {
	         logger.error("Exception setting property {} in the configuration", propName, e);
	         throw new RuntimeException(e);
	      }
	   }
}
