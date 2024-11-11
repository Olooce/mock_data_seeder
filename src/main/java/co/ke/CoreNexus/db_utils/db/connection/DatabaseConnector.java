package co.ke.CoreNexus.db_utils.db.connection;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * mock_data_scedar (co.ke.CoreNexus.db_utils.db.connection)
 * Created by: oloo
 * On: 11/11/2024. 19:37
 * Description:
 **/

public class DatabaseConnector {

    private static final HikariDataSource dataSource;

    // Initialize the connection pool
    static {
        try {
            Properties properties = loadProperties();
            HikariConfig config = new HikariConfig();

            // Set database connection properties
            String jdbcUrl = properties.getProperty("db.url");
            config.setJdbcUrl(jdbcUrl);
            config.setUsername(properties.getProperty("db.username"));
            config.setPassword(properties.getProperty("db.password"));

            // Set driver class dynamically based on the JDBC URL
            String driverClass = getDriverClass(jdbcUrl);
            config.setDriverClassName(driverClass);

            // Set HikariCP properties dynamically from db.properties
            config.setMaximumPoolSize(Integer.parseInt(properties.getProperty("db.pool.maxPoolSize", "10")));
            config.setMinimumIdle(Integer.parseInt(properties.getProperty("db.pool.minIdle", "2")));
            config.setIdleTimeout(Long.parseLong(properties.getProperty("db.pool.idleTimeout", "600000")));
            config.setConnectionTimeout(Long.parseLong(properties.getProperty("db.pool.connectionTimeout", "30000")));
            config.setLeakDetectionThreshold(Long.parseLong(properties.getProperty("db.pool.leakDetectionThreshold", "2000")));

            dataSource = new HikariDataSource(config);
            System.out.println("Database connection pool initialized successfully.");
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to initialize the database connection pool.", e);
        }
    }

    // Load properties from the db.properties file
    private static Properties loadProperties() throws IOException {
        Properties properties = new Properties();
        try (InputStream input = DatabaseConnector.class.getClassLoader().getResourceAsStream("db.properties")) {
            if (input == null) {
                throw new IOException("Property file '" + "db.properties" + "' not found in the classpath.");
            }
            properties.load(input);
        }
        return properties;
    }

    // Retrieve a connection from the pool
    public static Connection getConnection() throws SQLException {
        if (dataSource == null) {
            throw new IllegalStateException("Data source is not initialized.");
        }
        return dataSource.getConnection();
    }

    // Close the data source and release all connections
    public static void close() {
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
            System.out.println("Database connection pool closed.");
        }
    }

    // Dynamically determine the driver class from the JDBC URL
    private static String getDriverClass(String jdbcUrl) {
        if (jdbcUrl.startsWith("jdbc:postgresql://")) {
            return "org.postgresql.Driver";
        } else if (jdbcUrl.startsWith("jdbc:mysql://")) {
            return "com.mysql.cj.jdbc.Driver";
        } else if (jdbcUrl.startsWith("jdbc:sqlite:")) {
            return "org.sqlite.JDBC";
        } else if (jdbcUrl.startsWith("jdbc:oracle:thin:@")) {
            return "oracle.jdbc.OracleDriver";
        } else if (jdbcUrl.startsWith("jdbc:sqlserver://")) {
            return "com.microsoft.sqlserver.jdbc.SQLServerDriver";
        } else if (jdbcUrl.startsWith("jdbc:h2:")) {
            return "org.h2.Driver";
        } else {
            throw new IllegalArgumentException("Unsupported JDBC URL: " + jdbcUrl);
        }
    }


    // Shutdown hook to close the data source on JVM shutdown
    static {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (!dataSource.isClosed()) {
                dataSource.close();
                System.out.println("Database connection pool closed on JVM shutdown.");
            }
        }));
    }
}