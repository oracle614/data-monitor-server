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
public class HiveConnect implements DbConnect {
    private final Logger logger = LoggerFactory.getLogger(HiveConnect.class);


    @Value("${hive.driverName}")
    private  String driverName  ;
    @Value("${hive.url}")
    private  String url ;
    @Value("${hive.user}")
    private  String user ;
    @Value("${hive.password}")
    private  String password;

    @Override
    public Connection getConnection() throws SQLException {
        try {
            Class.forName(driverName);
            Connection connection = DriverManager.getConnection(url, user, password);
            FinalVar.hiveCount++;
            return connection ;
        } catch (Exception e) {
            logger.error("create hive connection exception: [{}]", e);
            FinalVar.hiveCount--;
            throw new SQLException(e);
        }
    }

    @Override
    public boolean closeConnection(Connection conn) {
        try {
            if (null != conn && !conn.isClosed()) {
                conn.close();
                FinalVar.hiveCount--;
            }
        } catch (SQLException e) {
            logger.error("close hive connect exception:[{}]", e);
        }
        return true;
    }
}
