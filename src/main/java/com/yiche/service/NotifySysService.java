package com.yiche.service;

import com.yiche.bean.IndexProMail;

import java.util.List;

public interface NotifySysService {

    /**
     * 数据报告
     * @param alarmUniqueId
     * @param mailList
     */
    void notifyBuilder(String project, String alarmUniqueId, List<IndexProMail> mailList);


    /**
     * 报警
     *
     * @param alarmUniqueId
     * @param dateBase
     * @param tableName
     * @param content
     * @param column
     * @param error
     * @param value
     * @param valueCompare
     * @param scope
     * @param id
     * @param project
     * @param waveScope
     * @param checkDay
     */
    void notifyBuilder(String alarmUniqueId, String dateBase
            , String tableName, String content, String column, String error, String value
            , String valueCompare, String scope, String id, String project, String waveScope, Integer checkDay, String partitionType, String user, String priority);

    /**
     * sql
     *
     * @param alarmUniqueId
     * @param dateBase
     * @param tableName
     * @param content
     */
    void notifyBuilder(String alarmUniqueId, String dateBase
            , String tableName, String content);

    /**
     * sql
     *
     * @param alarmUniqueId
     * @param subject
     * @param content
     */
    void notifyBuilder(String alarmUniqueId, String subject, String content);


    /**
     * 维度报警
     * @param alarmUniqueId
     * @param dateBase
     * @param tableName
     * @param content
     * @param column
     * @param error
     * @param id
     * @param project
     * @param checkDay
     * @param partitionType
     * @param user
     * @param priority
     */
    public void notifyBuilder(String alarmUniqueId, String dateBase
            , String tableName, String content, String column, String error, String id, String project, Integer checkDay, String partitionType,
                              String user, String priority,String errorMsg);
}
