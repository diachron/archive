package eu.fp7.diachron.archive.core.datamanagement;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import virtuoso.jdbc4.VirtuosoConnectionPoolDataSource;
import virtuoso.jdbc4.VirtuosoDataSource;

/**
 * 
 * Provides access to the store and definitions for the connection.
 *
 */
public class StoreConnection {
	private static final String DEFAULT_BULK_LOAD_PATH = "c:/RDF/";
	private static final String DEFAULT_USERNAME = "dba";
	private static final String DEFAULT_PWD = "dba";
	private static final String DEFAULT_HOST = "localhost";
	private static final int DEFAULT_PORT = 1111;
	private static final int DEFAULT_INITIAL_POOL = 2;
	private static final int DEFAULT_MAX_POOL = 10;
	private static final boolean DEFAULT_CONN_POOL = true;

	
	private static String username = DEFAULT_USERNAME;
	private static String pwd = DEFAULT_PWD;
	private static String host = DEFAULT_HOST;
	private static int port = DEFAULT_PORT;
	private static int initialPool = DEFAULT_INITIAL_POOL;
	private static int maxPool = DEFAULT_MAX_POOL;
	private static boolean connPool = DEFAULT_CONN_POOL;

	private static String bulkLoadPath = DEFAULT_BULK_LOAD_PATH;
	
	private static final Logger logger = LoggerFactory.getLogger(StoreConnection.class);
	
	private static String loadConfigParam(Properties prop, String paramName, String paramLabel, String defaultValue) {
		if (prop.getProperty(paramName)!=null) {
			return prop.getProperty(paramName);
		} else {
			logger.warn(paramLabel + " WASN'T FOUND IN RDF STORE CONFIGURATION. REVERTING TO DEFAULT VALUE");
			return defaultValue;
		}
	}
	
	private static int loadConfigParam(Properties prop, String paramName, String paramLabel, int defaultValue) {
		if (prop.getProperty(paramName)!=null) {
			return Integer.parseInt(prop.getProperty(paramName));
		} else {
			logger.warn(paramLabel + " WASN'T FOUND IN RDF STORE CONFIGURATION. REVERTING TO DEFAULT VALUE");
			return defaultValue;
		}
	}
	
	private static boolean loadConfigParam(Properties prop, String paramName, String paramLabel, boolean defaultValue) {
		if (prop.getProperty(paramName)!=null) {
			return Boolean.parseBoolean(prop.getProperty(paramName));
		} else {
			logger.warn(paramLabel + " WASN'T FOUND IN RDF STORE CONFIGURATION. REVERTING TO DEFAULT VALUE");
			return defaultValue;
		}
	}
	
	/**
	 * Initializes the StoreConnection object.
	 * @throws IOException 
	 * @throws SQLException 
	 */
	public static VirtuosoDataSource staticVirtuosoDataSource() {
	    try {
    		//initializing the connection parameters from the configuration file, 
    		//if a parameter or the whole file isn't found the initialization will be done with default values
    		
    		InputStream input = StoreConnection.class.getClassLoader().getResourceAsStream("virt-connection.properties");
    		Properties prop = new Properties();
    			
    		if (input == null) {
    			logger.warn("RDF STORE CONFIGURATION WASN'T FOUND. REVERTING TO DEFAULT PARAMETERS");
    		} else {
    			prop.load(input);
    			username = loadConfigParam(prop, "virtuoso-username", "USERNAME", DEFAULT_USERNAME);
    			pwd = loadConfigParam(prop, "virtuoso-pwd", "PASSWORD", DEFAULT_PWD);
    			host = loadConfigParam(prop, "virtuoso-host", "HOST", DEFAULT_HOST);
    			port = loadConfigParam(prop, "virtuoso-port", "PORT", DEFAULT_PORT);
    			initialPool = loadConfigParam(prop, "virtuoso-initial-pool", "INITIAL-POOL", DEFAULT_INITIAL_POOL);
    			maxPool = loadConfigParam(prop, "virtuoso-max-pool", "MAX-POOL", DEFAULT_MAX_POOL);
    			connPool = loadConfigParam(prop, "virtuoso-connection-pooling", "CONNECTION-POOLING", DEFAULT_CONN_POOL);
    			bulkLoadPath = loadConfigParam(prop, "virtuoso-bulk-load-path", "BULK-LOAD-PATH", DEFAULT_BULK_LOAD_PATH);
    			input.close();
    		}
    
    		VirtuosoDataSource vDatasource;
    		if (connPool) {
    			//create the pool Datasource
    			vDatasource = new VirtuosoConnectionPoolDataSource();
    			
    			((VirtuosoConnectionPoolDataSource)vDatasource).setInitialPoolSize(initialPool);
    			((VirtuosoConnectionPoolDataSource)vDatasource).setMaxPoolSize(maxPool);
    			
    			//initial the pool
    			((VirtuosoConnectionPoolDataSource)vDatasource).fill();
    		} else {
    			vDatasource = new VirtuosoDataSource();
    		}
    		
    		vDatasource.setCharset("UTF-8");
    		vDatasource.setPassword(pwd);
    		vDatasource.setPortNumber(port);
    		vDatasource.setUser(username);
    		vDatasource.setServerName(host);
    		
    		return vDatasource;
	    } catch (Exception e) {
	      throw new RuntimeException("Could not initialize static virtuoso connection", e);
	    }
   }
	
	/**
	 * @return the bULK_LOAD_PATH
	 */
	public static String getBulkLoadPath() {
		return bulkLoadPath;
	}
	
	
}
