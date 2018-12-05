package com.dachen.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class PrestoUtil {
    private static String driver = "com.facebook.presto.jdbc.PrestoDriver";
    private static String url = "jdbc:presto://ns:8080/hive/pro";
    private static String user = "root";
    private static String password = "";

    private PrestoUtil(){}

    static {
        /**
         * 驱动注册
         */
        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            throw new ExceptionInInitializerError(e);
        }

    }
    public static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(url, user, password);
    }
}
