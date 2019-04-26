package com.yiche.service;

import com.yiche.bean.ColumnRuleBean;
import com.yiche.bean.DimensionColumnBean;
import org.springframework.util.MultiValueMap;

import java.sql.Statement;
import java.util.Date;
import java.util.List;


public interface RuleRunService {

    int insert(ColumnRuleBean columnRuleBean);

    String getExpectedPartition(String dataBase, String tableName, Statement stmt, Integer day, String partitionType) throws Exception;

    String getPartitions(String dataBase, String tableName, Statement stmt) throws Exception;

    List<String> getPartitionList(String dataBase, String tableName, Statement stmt) throws Exception;

    boolean isResultData(String dataBase, String tableName, String partitions, Statement stmt);

    String getHDFSPath(String dataBase, String tableName, Statement stmt);

    boolean isFileExist(String path);

    Long getCount(String dataBase, String tableName, String column, Statement stmt, String partitions, Date date, String where, String partitionType) throws Exception;

    Long getCount(String dataBase, String tableName, String column, Statement stmt, String partition) throws Exception;

    String getColumnType(String dataBase, String tableName, String column, Statement stmt);

    Integer selectLengthOrNum(String dataBase, String tableName, Statement stmt, String select, String partitions
            , String where, Date date, String partitionType);

    Integer selectRepeatNum(String dataBase, String tableName, Statement stmt, String column, String partitions, String where, Date date, String partitionType);

    Integer selectLike(String dataBase, String tableName, Statement stmt, String column, String partitions, String where, Date date, String partitionType);

    Integer selectSevenDayDate(String dataBase, String tableName, Statement stmt, String partitions
            , String where, String fromDate, String toDate);

    String getSql(String column, Statement stmt, String partitions, Date date, String sql);

    MultiValueMap getDimensionData(String dataBase, String tableName, String column, Statement stmt, String partitions, Date date, String where,
                                         String partitionType, List<String> columnList) throws Exception;

     MultiValueMap getDimensionDataGroupBy(String dataBase, String tableName, String column, Statement stmt, String partitions, Date date, String where,
                                                 String partitionType,List<String> columnList) throws Exception;
}