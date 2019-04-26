package com.yiche.bean;

import com.fasterxml.jackson.annotation.JsonFormat;

import java.util.Date;

public class TableRuleBean {
    private Integer id;

    private String tid;

    private String monitorType;

    private String tcondition;

    private String tableName;

    private String databaseName;

    private String hCompare;

    private String tCompare;

    private String self;

    private String sevenWaveAvg;

    private String alarmType;

    private String remark;

    private String towner;

    private String content;

    private String reciever;

    @JsonFormat(locale = "zh", timezone = "GMT+8", pattern = "yyyy-MM-dd HH:mm:ss")
    private Date createTime;

    private String monitorTime;

    private String username;

    private String status;

    private Integer checkDay;

    private String columnName;

    private String contentSql;

    private String number;

    private String partitionType;

    private Date nextExecTime;

    private String alarmUniqueId;

    private String priority;

    private String dimension;

    private String exeTime;

    public String getPriority() {
        return priority;
    }

    public void setPriority(String priority) {
        this.priority = priority;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getTid() {
        return tid;
    }

    public void setTid(String tid) {
        this.tid = tid;
    }

    public String getMonitorType() {
        return monitorType;
    }

    public void setMonitorType(String monitorType) {
        this.monitorType = monitorType;
    }

    public String getTcondition() {
        return tcondition;
    }

    public void setTcondition(String tcondition) {
        this.tcondition = tcondition;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String gethCompare() {
        return hCompare;
    }

    public void sethCompare(String hCompare) {
        this.hCompare = hCompare;
    }

    public String gettCompare() {
        return tCompare;
    }

    public void settCompare(String tCompare) {
        this.tCompare = tCompare;
    }

    public String getSelf() {
        return self;
    }

    public void setSelf(String self) {
        this.self = self;
    }

    public String getSevenWaveAvg() {
        return sevenWaveAvg;
    }

    public void setSevenWaveAvg(String sevenWaveAvg) {
        this.sevenWaveAvg = sevenWaveAvg;
    }

    public String getAlarmType() {
        return alarmType;
    }

    public void setAlarmType(String alarmType) {
        this.alarmType = alarmType;
    }

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }

    public String getTowner() {
        return towner;
    }

    public void setTowner(String towner) {
        this.towner = towner;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getReciever() {
        return reciever;
    }

    public void setReciever(String reciever) {
        this.reciever = reciever;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public String getMonitorTime() {
        return monitorTime;
    }

    public void setMonitorTime(String monitorTime) {
        this.monitorTime = monitorTime;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Integer getCheckDay() {
        return checkDay;
    }

    public void setCheckDay(Integer checkDay) {
        this.checkDay = checkDay;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getContentSql() {
        return contentSql;
    }

    public void setContentSql(String contentSql) {
        this.contentSql = contentSql;
    }

    public String getNumber() {
        return number;
    }

    public void setNumber(String number) {
        this.number = number;
    }

    public String getPartitionType() {
        return partitionType;
    }

    public void setPartitionType(String partitionType) {
        this.partitionType = partitionType;
    }

    public Date getNextExecTime() {
        return nextExecTime;
    }

    public void setNextExecTime(Date nextExecTime) {
        this.nextExecTime = nextExecTime;
    }

    public void setAlarmUniqueId(String alarmUniqueId) {
        this.alarmUniqueId = alarmUniqueId;
    }

    public String getAlarmUniqueId() {
        return alarmUniqueId;
    }

    public String getExeTime() {
        return exeTime;
    }

    public void setExeTime(String exeTime) {
        this.exeTime = exeTime;
    }

    public String getDimension() {
        return dimension;
    }

    public void setDimension(String dimension) {
        this.dimension = dimension;
    }
}