package com.yiche;

import com.yiche.bean.DimensionResultBean;
import com.yiche.bean.RuleRunningLogBean;
import com.yiche.dao.DimensionResultBeanMapper;
import com.yiche.dao.RuleRunningLogBeanMapper;
import com.yiche.db.ConnectFactory;
import com.yiche.em.DbType;
import com.yiche.utils.*;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

@SpringBootTest
@RunWith(SpringJUnit4ClassRunner.class)
public class test {

    @Autowired
    RuleRunningLogBeanMapper ruleRunningLogBeanMapper;

    @Autowired
    private ConnectFactory connectFactory;

    @Test
    public void test02(){
        RuleRunningLogBean ruleRunningLogBean = new RuleRunningLogBean();
        ruleRunningLogBean.setError("error");
        ruleRunningLogBean.setPriority("priority");
        ruleRunningLogBean.setNumber("number");
        ruleRunningLogBean.setCheckday(1);
        ruleRunningLogBeanMapper.insert(ruleRunningLogBean);
    }


    @Test
    public void test03() throws Exception{


        Date date =  DateFormatSafe.getDay(-4);

       System.out.println(date);
       String A =  DateFormatSafe.dateFormat("2018-11-23", date);
       System.out.println(A);

       boolean a = DateUtils.isWeekend(A);
       System.out.println(a);
    }

    @Test
    public void test2() {


        Statement stmt = null;
        Connection con = null;
        ResultSet res = null;

//        try {
//            con = connectFactory.createConnect(DbType.Presto);
//            stmt = con.createStatement();
//            String SQL = "show partitions from testzgc.t2";
//            try {
//                res = stmt.executeQuery(SQL);
//
//                while (res.next()) {
//
//                    String A = res.getString(1);
//                    String B = res.getString(1);
//                    String C = res.getString(1);
//                    System.out.println(A + "-" + B + "-" + C);
//
//                    System.out.println(    res.getMetaData().getColumnName(1));
//                    System.out.println(    res.getMetaData().getColumnName(1));
//
////                    partitionList.add(res.getMetaData().getColumnName(1) + "=" + res.getString(1));
////                    if (FinalVar.MONTH.equals(partitionType)) {
////                        if (res.getString(1).equals(DateFormatSafe.formatMonth(DateFormatSafe.getMonth(day)))) {
////                            expectedPartition = res.getMetaData().getColumnName(1) + "=" + DateFormatSafe.formatMonth(DateFormatSafe.getMonth(day));
////                            break;
////                        } else {
////                            expectedPartition = "";
////                        }
////                    } else {
////                        //日
////                        if (res.getString(1).equals(DateFormatSafe.format(DateFormatSafe.getDay(day)))) {
////                            expectedPartition = res.getMetaData().getColumnName(1) + "=" + DateFormatSafe.format(DateFormatSafe.getDay(day));
////                            break;
////                        } else if (res.getString(1).equals(DateFormatSafe.formatSign(DateFormatSafe.getDay(day)))) {
////                            expectedPartition = res.getMetaData().getColumnName(1) + "=" + DateFormatSafe.formatSign(DateFormatSafe.getDay(day));
////                            break;
////                        } else {
////                            expectedPartition = "";
////                        }
////                    }
//                }
//            } catch (SQLException e) {
//                e.printStackTrace();
//                throw new Exception(e.getMessage());
//            } finally {
//                closeQuery(res);
//            }
//
//
//        } catch (Exception e) {
//
//        } finally {
//            closeAfterQuery(stmt, con);
//        }
    }
    private void closeAfterQuery(Statement stmt, Connection con) {
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e) {
            }
        }
        if (con != null) {
            try {
                con.close();
                FinalVar.dbConnCount--;
            } catch (SQLException e) {
                FinalVar.dbConnCount++;
            }
        }
    }
    public void closeQuery(ResultSet res) {
        if (res != null) {
            try {
                res.close();
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                res = null;
            }
        }
    }

//    @Test
//    public void test04() {
////        int a = 717674435;
////        long e = 1934434342232112424L;
////        Long YT = 4141142L;
////        long rrr = e + YT;
////        System.out.println(rrr);
////        double t = 1353454353;
//////        Double y = 52324234242342324234242;
////        Integer b = 2121121212;
////        String a1 = TypeConvert.IntegerConvertString(33212312);
//        String cronExpression ="0x0 4 * * 2";
//        CronExpression ce;
//        try {
//            ce = new CronExpression(cronExpression );
//
//            Date date = new Date();
////        return
//          Date A=  ce.getNextValidTimeAfter(date);
//
//          System.out.println(A);
//
//        } catch (Exception e) {
//            e.printStackTrace();
////            logger.error("parse cron expression error, expression:{}, e:{}", cronExpression, e);
////            return null;
//        }
//
//        Date date = new Date();
////        return
//// ce.getNextValidTimeAfter(date);
//
//    }

@Test
public void compareTest() {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    String nowTime = sdf.format(new Date());
    System.out.println(nowTime);
   Date valueTime = null;
    try {
        valueTime = sdf.parse(DateFormatSafe.formatSign(new Date()) + " " +"15:54:00");
        System.out.println(valueTime);
    } catch (ParseException e) {
        e.printStackTrace();
    }
    String ruleExeTime = sdf.format(valueTime);
    System.out.println(ruleExeTime);
            /*
            当前时间小于预计执行时间   则移除该项
             */
    System.out.println(nowTime.compareTo(ruleExeTime) );
}
}
