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
//    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
//    private static String url = "jdbc:hive2://172.20.0.25:10000/default";
//    private static String user = "hive";
//    private static String url = "jdbc:hive2://192.168.15.47:10000/default";
//    private static String user = "liuming1";
//
//    private static String password = "";
//
//    private static volatile  Connection conn;


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


//        if (conn == null) {
//            synchronized (ConnectHive.class) {
//                if (conn == null) {
//                    try {
//                        Class.forName(driverName);
//                        conn = DriverManager.getConnection(url, user, password);
//                    } catch (ClassNotFoundException e) {
//                        e.printStackTrace();
//                    } catch (SQLException e) {
//                        e.printStackTrace();
//                    }finally {
//                        if(conn!=null){
//                            try {
//                                conn.close();
//                            } catch (SQLException e) {
//                                e.printStackTrace();
//                            }
//                        }
//                    }
//                    }
//                }
//            }
//
//        }