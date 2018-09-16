package com.yiche.db;

import com.yiche.utils.FinalVar;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Created by weiyongxu on 2018/7/4.
 */
@Component
public class PrestoConnect implements DbConnect{
    private final Logger logger = LoggerFactory.getLogger(PrestoConnect.class);


    @Value("${presto.driverName}")
    private  String driverName  ;
    @Value("${presto.url}")
    private  String url ;
    @Value("${presto.user}")
    private  String user ;
    @Value("${presto.password}")
    private  String password;

    @Override
    public Connection getConnection() throws SQLException {
        try {
            Class.forName(driverName);
            Connection connection = DriverManager.getConnection(url, user, password);
            FinalVar.dbConnCount++;
            return connection ;
        } catch (Exception e) {
            logger.error("create presto connection exception: [{}]", e);
            FinalVar.dbConnCount--;
            throw new SQLException(e);
        }
    }

    @Override
    public boolean closeConnection(Connection conn) {
        try {
            if (null != conn && !conn.isClosed()) {
                conn.close();
                FinalVar.dbConnCount--;
            }
        } catch (SQLException e) {
            logger.error("close hive connect exception:[{}]", e);
        }
        return true;
    }
}
