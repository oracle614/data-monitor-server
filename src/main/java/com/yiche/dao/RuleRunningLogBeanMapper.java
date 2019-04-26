package com.yiche.dao;

import com.yiche.bean.RuleRunningLogBean;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

@Mapper
public interface RuleRunningLogBeanMapper {
    int deleteByPrimaryKey(Integer id);

    int insert(RuleRunningLogBean record);

    RuleRunningLogBean selectByPrimaryKey(Integer id);

    List<RuleRunningLogBean> selectAll();

    int updateByPrimaryKey(RuleRunningLogBean record);


    List<RuleRunningLogBean> getResultLogByPage(@Param("index") Integer index, @Param("limit") Integer limit);

    List<RuleRunningLogBean> getDataByDataBaseAndTable(@Param("databaseName") String dataBaseName
            , @Param("tableName") String tableName, @Param("index") Integer index, @Param("limit") Integer limit);

    Integer getResultLogAllCount();


    Integer getResultLogStatusCount(@Param("databaseName") String dataBaseName
            , @Param("tableName") String tableName, @Param("status") String status);

    Integer getResultLogCount(@Param("databaseName") String dataBaseName
            , @Param("tableName") String tableName);


    List<RuleRunningLogBean> getRunningLogByType(@Param("databaseName") String dataBaseName
            , @Param("tableName") String tableName, @Param("columnName") String columnName
            , @Param("type") String type);

    Integer getTableRuleCountByItem(@Param("parentId") Integer parentId, @Param("id") Integer id, @Param("day") Integer day);

    Integer getTableItemCount(@Param("parentId") Integer parentId, @Param("id") Integer id, @Param("day") Integer day);

    List<RuleRunningLogBean> getTableLogByItem(@Param("parentId") Integer parentId, @Param("id") Integer id, @Param("day") Integer day);

    Integer getColumnRuleCountByItem(@Param("parentId") Integer parentId, @Param("id") Integer id, @Param("day") Integer day);

    Integer getColumnItemCount(@Param("parentId") Integer parentId, @Param("id") Integer id);

    List<RuleRunningLogBean> getColumnLogByItem(@Param("parentId") Integer parentId, @Param("id") Integer id, @Param("day") Integer day);

    /**
     * 获取项目下完成时间的规则 已经执行
     *
     * @param parentId
     * @param id
     * @param day
     * @return
     */
    List<RuleRunningLogBean> getReadyItemAllFinish(@Param("parentId") Integer parentId, @Param("id") Integer id,
                                                   @Param("day") Integer day, @Param("status") String status,
                                                   @Param("partitionType") String partitionType);

    /**
     * 获取项目下字段的规则 已经执行
     *
     * @param parentId
     * @param id
     * @param day
     * @return
     */
    List<RuleRunningLogBean> getColumnItemFinish(@Param("parentId") Integer parentId, @Param("id") Integer id,
                                                 @Param("day") Integer day, @Param("status") String status,
                                                 @Param("partitionType") String partitionType);

    /**
     * 获取项目下非完成时间的规则 已经执行
     *
     * @param parentId
     * @param id
     * @param day
     * @return
     */
    List<RuleRunningLogBean> getTableItemDnRFinish(@Param("parentId") Integer parentId, @Param("id") Integer id,
                                                   @Param("day") Integer day, @Param("status") String status,
                                                   @Param("partitionType") String partitionType);

    int updateStatusByRuleId(@Param("value") String value, @Param("ruleId") String ruleId,
                             @Param("day") Integer day);

    List<RuleRunningLogBean> queryTodayNoPassRule();


    List<RuleRunningLogBean> queryTodayResultByTableName(@Param("databaseName")String databaseName,@Param("tableName") String tableName);
    /**
     * 获取未就绪规则
     *
     * @param parentId
     * @param id
     * @param day
     * @param value
     * @param partitionType
     * @return
     */


    List<RuleRunningLogBean> getTimeRuleNotReady(@Param("parentId") Integer parentId, @Param("id") Integer id,
                                                 @Param("day") Integer day, @Param("value") String value,
                                                 @Param("partitionType") String partitionType);

}