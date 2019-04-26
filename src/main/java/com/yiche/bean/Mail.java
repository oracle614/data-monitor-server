package com.yiche.bean;

public class Mail {


    //监控ID
    private String id;
    //监控内容
    private String info;
    //日期
    private String date;
    //库表|字段
    private String table;
    //监控类型
    private String type;
    //正常范围
    private String scope;
    //波动范围
    private String waveScope;
    //实际值
    private String value;
    //对比值
    private String valueCompare;
    //影响范围
    private String project;
    //优先级
    private String priority;
    //责任人
    private String user;

    //维度名
    private String dimensionName;

    //对比字段
    private String columnName;

    private String error;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getInfo() {
        return info;
    }

    public void setInfo(String info) {
        this.info = info;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getScope() {
        return scope;
    }

    public void setScope(String scope) {
        this.scope = scope;
    }

    public String getWaveScope() {
        return waveScope;
    }

    public void setWaveScope(String waveScope) {
        this.waveScope = waveScope;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getValueCompare() {
        return valueCompare;
    }

    public void setValueCompare(String valueCompare) {
        this.valueCompare = valueCompare;
    }

    public String getProject() {
        return project;
    }

    public void setProject(String project) {
        this.project = project;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPriority() {

        return priority;
    }

    public void setPriority(String priority) {
        this.priority = priority;
    }

    public String getDimensionName() {
        return dimensionName;
    }

    public void setDimensionName(String dimensionName) {
        this.dimensionName = dimensionName;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }
}
