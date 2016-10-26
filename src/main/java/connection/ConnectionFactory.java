package connection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.AbstractMap.SimpleEntry;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.collections4.keyvalue.MultiKey;
import org.apache.commons.collections4.map.LRUMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectionFactory {

	private static Logger log = LoggerFactory.getLogger(ConnectionFactory.class);

	private static final String DRIVER_NAME = "org.postgresql.Driver";
	private final static int VALIDATION_TIMEOUT = 3000;
	private final static int MAX_CACHED_FACTORIES = 10;
	
	private final String host;
	private final Integer port;
	private final String databaseName;
	private final String connectionUrl;
	private final String schema;

	private final Map<String, SimpleEntry<String, Connection>> userConnections = new HashMap<>();
	
	private static final LRUMap<MultiKey<Object>, ConnectionFactory> factoryCache = new LRUMap<>(MAX_CACHED_FACTORIES);
	
	public static ConnectionFactory getConnectionFactory(String host, Integer port, String databaseName, String schema) {
		
		ConnectionFactory cachedFactory = factoryCache.get(new MultiKey<>(host, port, databaseName));
		
		if(cachedFactory != null) {
			log.debug("found ConnectionFactory in cache, returning cached value");
			return cachedFactory;
		}
		log.debug("creating new ConnectionFactory");
		
		try {

			Class.forName(DRIVER_NAME);

		} catch (ClassNotFoundException exception) {

			log.error("Driver \" " + DRIVER_NAME
					+ " \" is not available on the classpath! Check your maven dependencies.");
			System.exit(1);

		}

		ConnectionFactory connectionFactory = new ConnectionFactory(host, port, databaseName, schema);
		factoryCache.put(new MultiKey<>(host, port, databaseName), connectionFactory);
		return connectionFactory;
	}

	private ConnectionFactory(String host, Integer port, String databaseName, String schema) {
		connectionUrl = "jdbc:postgresql://" + (this.host = host) + ":" + (this.port = port) + "/"
				+ (this.databaseName = databaseName) + "?currentSchema=" + (this.schema = schema);
	}

	public Connection getConnection(String username, String password) {

		try {

			Entry<String, Connection> passwordToConnection = userConnections.get(username);

			if (passwordToConnection != null) {

				if (passwordToConnection.getKey().equals(password)) {
					return passwordToConnection.getValue();
				} else {
					log.debug("Username [" + username
							+ "] already in use, connection attempt made with different password");
					return null;
				}

			} else {

				Connection connection = DriverManager.getConnection(connectionUrl, username, password);
				userConnections.put(username, new SimpleEntry<String, Connection>(password, connection));

				ensureConnectionExit(connection, username);

				log.debug("Retrieved new connection");
				return connection;

			}

		} catch (SQLException e) {

			log.error("Connection failed!", e);
			return null;

		}

	}

	public boolean validateConnection(String username) {

		try {

			Connection connection;

			if (userConnections.get(username) == null) {
				return false;
			} else if ((connection = userConnections.get(username).getValue()).isValid(VALIDATION_TIMEOUT)) {
				return true;
			} else {
				if (!connection.isClosed()) {
					log.debug("Connection for user [" + username
							+ "] is no longer valid, but is not closed. Closing connection.");
					connection.close();
				}
				userConnections.remove(username);
				return false;
			}

		} catch (SQLException e) {
			log.debug("Error validation connection, assuming connection is invalid");
			return false;
		}

	}

	public String getHost() {
		return host;
	}

	public Integer getPort() {
		return port;
	}

	public String getDatabaseName() {
		return databaseName;
	}
	
	public String getSchema() {
		return schema;
	}

	public String getConnectionUrl() {
		return connectionUrl;
	}

	private void ensureConnectionExit(Connection connection, String username) {

		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			try {
				if (!connection.isClosed()) {
					log.debug("Connection still open for user [" + username
							+ "] during system exit. Shutting connection down.");
					connection.close();
				} else {
					
				}
			} catch (SQLException e) {
				log.debug("Error closing connection, assuming connection was invalid. Swallowing this Exception.");
			}
		}));

	}

}
