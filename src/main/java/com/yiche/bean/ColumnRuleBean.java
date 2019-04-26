package com.yiche.bean;

import com.fasterxml.jackson.annotation.JsonFormat;

import java.util.Date;

public class ColumnRuleBean {
    private Integer cid;

    private String id;

    private String monitorType;

    private String ccondition;

    private String tableName;

    private String columnName;

    private String databaseName;

    private String calculateType;

    private String deviation;

    private String hCompare;

    private String tCompare;

    private String self;

    private String remark;

    private String cowner;

    private String content;

    private String reciever;

    @JsonFormat(locale = "zh", timezone = "GMT+8", pattern = "yyyy-MM-dd HH:mm:ss")
    private Date createTime;

    private String countType;

    private String aggregateFunction;

    private String dayIncrement;

    private String alarmType;

    private String username;

    private String status;

    private Integer checkDay;

    private String number;

    private String partitionType;

    private Date nextExecTime;

    private String alarmUniqueId;

    private String priority;
    private String exeTime;

    public String getPriority() {
        return priority;
    }

    public void setPriority(String priority) {
        this.priority = priority;
    }

    public Integer getCid() {
        return cid;
    }

    public void setCid(Integer cid) {
        this.cid = cid;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getMonitorType() {
        return monitorType;
    }

    public void setMonitorType(String monitorType) {
        this.monitorType = monitorType;
    }

    public String getCcondition() {
        return ccondition;
    }

    public void setCcondition(String ccondition) {
        this.ccondition = ccondition;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getCalculateType() {
        return calculateType;
    }

    public void setCalculateType(String calculateType) {
        this.calculateType = calculateType;
    }

    public String getDeviation() {
        return deviation;
    }

    public void setDeviation(String deviation) {
        this.deviation = deviation;
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

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }

    public String getCowner() {
        return cowner;
    }

    public void setCowner(String cowner) {
        this.cowner = cowner;
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

    public String getCountType() {
        return countType;
    }

    public void setCountType(String countType) {
        this.countType = countType;
    }

    public String getAggregateFunction() {
        return aggregateFunction;
    }

    public void setAggregateFunction(String aggregateFunction) {
        this.aggregateFunction = aggregateFunction;
    }

    public String getDayIncrement() {
        return dayIncrement;
    }

    public void setDayIncrement(String dayIncrement) {
        this.dayIncrement = dayIncrement;
    }

    public String getAlarmType() {
        return alarmType;
    }

    public void setAlarmType(String alarmType) {
        this.alarmType = alarmType;
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

    public String getAlarmUniqueId() {
        return alarmUniqueId;
    }

    public void setAlarmUniqueId(String alarmUniqueId) {
        this.alarmUniqueId = alarmUniqueId;
    }

    public String getExeTime() {
        return exeTime;
    }

    public void setExeTime(String exeTime) {
        this.exeTime = exeTime;
    }
}