package com.yiche.utils;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

@Service
public class ConnectHive {


    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
//    private static String url = "jdbc:hive2://172.20.0.25:10000/default";
//    private static String user = "hive";
    private static String url = "jdbc:hive2://192.168.15.47:10000/default";
    private static String user = "liuming1";
//
    private static String password = "";
//
//    private static volatile  Connection conn;



    public static  Connection getConn() {
        try {
            FinalVar.hiveCount++;
            Class.forName(driverName);
            return DriverManager.getConnection(url, user, password);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
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