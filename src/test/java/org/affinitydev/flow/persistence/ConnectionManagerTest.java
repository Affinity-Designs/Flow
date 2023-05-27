package org.affinitydev.flow.persistence;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

class ConnectionManagerTest {
    private ConnectionManager connectionManager;
    private ConnectionManager connectionManager2;
    private ConnectionManager connectionManage3;



    @BeforeEach
    void setup() throws SQLException {
        Credentials credentials = new Credentials("server", "localhost", "root", "password", 3306);
        connectionManager = new ConnectionManager(credentials, 10, "com.mysql.cj.jdbc.Driver");
        connectionManager2 = new ConnectionManager(credentials, 10);
        connectionManage3 = new ConnectionManager(credentials, 5, "com.mysql.cj.jdbc.Driver", DBType.MYSQL);
    }

    @Test
    void getPoolSize() {
        Assertions.assertEquals(10, connectionManager.getPoolSize());
        Assertions.assertEquals(10, connectionManager2.getPoolSize());
        Assertions.assertEquals(5, connectionManage3.getPoolSize());
    }

    @Test
    void getDataSourceClassName() {
        Assertions.assertEquals("com.mysql.cj.jdbc.Driver", connectionManager.getDataSourceClassName());
        Assertions.assertEquals("com.mysql.cj.jdbc.Driver", connectionManager2.getDataSourceClassName());
        Assertions.assertEquals("com.mysql.cj.jdbc.Driver", connectionManage3.getDataSourceClassName());
    }

    @Test
    void getDataSource() {
        Assertions.assertNotNull(connectionManager.getDataSource());
        Assertions.assertNotNull(connectionManager2.getDataSource());
        Assertions.assertNotNull(connectionManage3.getDataSource());
    }

    @Test
    void getConnection() throws SQLException {
        Assertions.assertNotNull(connectionManager.getConnection());
        Assertions.assertNotNull(connectionManager2.getConnection());
    }
}