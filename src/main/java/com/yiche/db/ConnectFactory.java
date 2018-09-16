package com.yiche.db;

import com.yiche.em.DbType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Created by weiyongxu on 2018/7/4.
 */
@Component
public class ConnectFactory {
    @Autowired
    private HiveConnect hiveConnect;

    @Autowired
    private PrestoConnect prestoConnect;

    public Connection createConnect(DbType dataType) throws SQLException {
        Connection connection = null;
        switch (dataType) {
            case Hive:
                connection = hiveConnect.getConnection();
                break;

            case Presto:
                connection = prestoConnect.getConnection();
                break;

            default:
                connection = hiveConnect.getConnection();
                break;
        }
        return connection;
    }
}
