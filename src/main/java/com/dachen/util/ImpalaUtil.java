package com.dachen.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;

@Component
public class ImpalaUtil {
	
	private static final Logger logger = LoggerFactory.getLogger(ImpalaUtil.class);
	
	@Value("${impala.url}")
	private String url;
	@Value("${impala.user}")
	private String user;
	@Value("${impala.password}")
	private String password;
	
    private ImpalaUtil() {
    }
   
    static {
    	 /**
         * 驱动注册
         */
        try {
            Class.forName("com.cloudera.impala.jdbc41.Driver");
        } catch (ClassNotFoundException e) {
            throw new ExceptionInInitializerError(e);
        }
    }
    public Connection getConnection() throws SQLException {
        return DriverManager.getConnection(url, user, password);
    }
 
    public  void close(Connection conn,Statement stat){
    	try {
        	if(Objects.nonNull(conn)) conn.close();
        	if(Objects.nonNull(stat)) stat.close();
        } catch (SQLException e) {
        	logger.error("ImpalaUtil close ERROR:{}",e.getMessage());
        }
    }
}