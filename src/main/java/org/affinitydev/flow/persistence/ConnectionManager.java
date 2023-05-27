package org.affinitydev.flow.persistence;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.jetbrains.annotations.NotNull;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Uses HikariCP to obtain a DataSource.
 */
public class ConnectionManager {
    private final int poolSize;
    private final String dataSourceClassName;
    private final DataSource dataSource;
    private final DBType dbType;
    private final boolean autoReconnect;
    private final boolean useSSL;

    public final static String MYSQL_CLASS_NAME = "com.mysql.cj.jdbc.Driver";

    private final static int DEFAULT_POOL_SIZE = 5;

    /**
     *Initialize the ConnectionManager and attempt to initialize data source. Uses MySQL as default database.
     *MySQL Class Name: com.mysql.cj.jdbc.Driver
     * @param credentials The database user credentials
     * @param poolSize The size of the pool
     * @param dataSourceClassName The class name of the data source
     */
    public ConnectionManager(@NotNull Credentials credentials, int poolSize, @NotNull String dataSourceClassName) {
        this.poolSize = poolSize;
        this.dataSourceClassName = dataSourceClassName;
        this.dbType = DBType.MYSQL;
        this.autoReconnect = true;
        this.useSSL = false;
        this.dataSource = initDataSource(credentials);
    }

    public ConnectionManager(@NotNull Credentials credentials, int poolSize, @NotNull String dataSourceClassName, DBType dbType) {
        this.poolSize = poolSize;
        this.dataSourceClassName = dataSourceClassName;
        this.dbType = dbType;
        this.autoReconnect = true;
        this.useSSL = false;
        this.dataSource = initDataSource(credentials);
    }

    public ConnectionManager(@NotNull Credentials credentials, int poolSize, @NotNull String dataSourceClassName, DBType dbType, boolean autoReconnect, boolean useSSL) {
        this.poolSize = poolSize;
        this.dataSourceClassName = dataSourceClassName;
        this.dbType = dbType;
        this.autoReconnect = autoReconnect;
        this.useSSL = useSSL;
        this.dataSource = initDataSource(credentials);
    }

    public ConnectionManager(@NotNull Credentials credentials, int poolSize) {
        this.poolSize = poolSize;
        this.dataSourceClassName = MYSQL_CLASS_NAME;
        this.dbType = DBType.MYSQL;
        this.autoReconnect = true;
        this.useSSL = false;
        this.dataSource = initDataSource(credentials);
    }

    /**
     * @param credentials Database credentials
     * @return HikariDataSource
     */
    private DataSource initDataSource(@NotNull Credentials credentials) {
        return initDataSource(credentials, getJdbcUrlPrefix() + credentials.getHost() + ":" + credentials.getPort() + "/" + credentials.getDatabase() + "?autoReconnect=" + autoReconnect + "&useSSL=" + useSSL);
    }

    private DataSource initDataSource(@NotNull Credentials credentials, String url) {
        Properties properties = new Properties();
        properties.setProperty("dataSource.serverName", credentials.getHost());
        properties.setProperty("dataSource.portNumber", String.valueOf(credentials.getPort()));
        properties.setProperty("dataSource.user", credentials.getUsername());
        properties.setProperty("dataSource.password", credentials.getPassword());
        properties.setProperty("dataSource.databaseName", credentials.getDatabase());

        HikariConfig config = new HikariConfig(properties);
        config.setMaximumPoolSize(getPoolSize());
        config.setDriverClassName(getDataSourceClassName());
        config.setJdbcUrl(url);
        return new HikariDataSource(config);
    }

    private String getJdbcUrlPrefix() {
        return switch (dbType) {
            case MARIA_DB -> "jdbc:mariadb://";
            case POSTGRESQL -> "jdbc:postgresql://";
            default -> "jdbc:mysql://";
        };
    }

    /**
     * @return The pool size. Returns DEFAULT_POOL_SIZE if less than 1.
     */
    public int getPoolSize() {
        if (poolSize > 1) {
            return poolSize;
        } else {
            return DEFAULT_POOL_SIZE;
        }
    }

    public String getDataSourceClassName() {
        return dataSourceClassName;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    /**
     * @return Connection
     * @throws SQLException Throw an SQLException if connection error
     */
    public Connection getConnection() throws SQLException {
        return getDataSource().getConnection();
    }


}
