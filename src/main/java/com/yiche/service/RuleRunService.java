package com.yiche.service;

import com.yiche.bean.ColumnRuleBean;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Date;
import java.util.List;
import java.util.Map;


public interface RuleRunService {

    int insert(ColumnRuleBean columnRuleBean);

    String getNewestPartition(String dataBase, String tableName, Statement stmt) throws Exception;

    String getPartitions(String dataBase, String tableName, Statement stmt) throws Exception;

    boolean isResultData(String dataBase,String tableName,String partitions,Statement stmt);

    String getHDFSPath(String dataBase,String tableName,Statement stmt);

    boolean isFileExist(String path);

    Integer getCount(String dataBase,String tableName,String column,Statement stmt,String partitions,Date date,String where,String partitionType) throws Exception;

     String getColumnType(String dataBase, String tableName, String column, Statement stmt);

      Integer selectLengthOrNum(String dataBase, String tableName,Statement stmt,String select,String partitions
              ,String where,Date date,String partitionType);

     Integer selectRepeatNum(String dataBase, String tableName,Statement stmt,String column,String partitions,String where,Date date,String partitionType);

     Integer selectLike(String dataBase, String tableName,Statement stmt,String column,String partitions,String where,Date date,String partitionType);

     Integer selectSevenDayDate(String dataBase, String tableName, Statement stmt, String partitions
            , String where, String fromDate, String  toDate);

      String getSql(String column, Statement stmt, String partitions, Date date, String sql);
}