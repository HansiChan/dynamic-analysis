package com.dachen.util;

import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

@Component
public final class ImpalaUtil {
    private static String driver = "com.cloudera.impala.jdbc4.Driver";
    private static String url = "jdbc:impala://nn:21050/pro";
    private static String user = "";
    private static String password = "";

    private ImpalaUtil() {
    }

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
