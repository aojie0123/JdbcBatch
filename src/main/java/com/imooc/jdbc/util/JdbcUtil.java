package com.imooc.jdbc.util;

import java.io.InputStream;
import java.sql.*;
import java.util.Properties;

public class JdbcUtil {
    public static void commit(Connection connection) {
        if (connection != null) {
            try {
                connection.commit();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void rollback(Connection connection) {
        if (connection != null) {
            try {
                connection.rollback();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void begin(Connection connection) {
        if (connection != null) {
            try {
                connection.setAutoCommit(false);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static Connection getConnection() throws Exception {
        Properties properties = new Properties();
        InputStream is = JdbcUtil.class.getClassLoader().getResourceAsStream("jdbc.properties");
        properties.load(is);
        String driver = properties.getProperty("driver");
        String username = properties.getProperty("username");
        String password = properties.getProperty("password");
        String url = properties.getProperty("url");
        Class.forName(driver);

        return DriverManager.getConnection(url, username, password);
    }

    public static void closeResource(Connection connection, Statement statement, ResultSet resultSet) {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
