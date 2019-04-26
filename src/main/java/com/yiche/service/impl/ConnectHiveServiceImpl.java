package com.yiche.service.impl;

import com.yiche.service.ConnectHiveService;
import com.yiche.utils.FinalVar;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

@Service
public class ConnectHiveServiceImpl implements ConnectHiveService {

    private final Logger logger = LoggerFactory.getLogger(ConnectHiveServiceImpl.class);


    @Value("${hive.driverName}")
    private   String driverName  ;
    @Value("${hive.url}")
    private  String url ;
    @Value("${hive.user}")
    private  String user ;
    @Value("${hive.password}")
    private  String password;



    public  Connection getConn() {
        try {
            Class.forName(driverName);
            Connection connection=   DriverManager.getConnection(url, user, password);
            FinalVar.hiveCount++;
            return connection ;
        } catch (ClassNotFoundException e) {
            logger.error("hive链接失败",e);
        } catch (SQLException e) {
            logger.error("hive链接失败",e);
        }
             return  null;
    }
}
