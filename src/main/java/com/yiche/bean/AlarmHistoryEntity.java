package com.yiche.bean;

import java.util.Date;

/**
 * Created by weiyongxu on 2018/7/31.
 */
public class AlarmHistoryEntity {
    private Long alarmHistoryId;
    private String databaseName;
    private String tableName;
    private String ruleId;
    private Date createTime;

    public Long getAlarmHistoryId() {
        return alarmHistoryId;
    }

    public void setAlarmHistoryId(Long alarmHistoryId) {
        this.alarmHistoryId = alarmHistoryId;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getRuleId() {
        return ruleId;
    }

    public void setRuleId(String ruleId) {
        this.ruleId = ruleId;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }
}
