package com.yiche.bean;

import java.util.Date;

public class DimensionResultBean {
    private Integer id;

    private String tableId;

    private String value;

    private String valueCompare;

    private String dimension;

    private String columnName;

    private String percent;

    private String error;

    private Date createTime;

    private String wave;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getTableId() {
        return tableId;
    }

    public void setTableId(String tableId) {
        this.tableId = tableId;
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

    public String getDimension() {
        return dimension;
    }

    public void setDimension(String dimension) {
        this.dimension = dimension;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getPercent() {
        return percent;
    }

    public void setPercent(String percent) {
        this.percent = percent;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }


    public DimensionResultBean(String tableId, String value, String valueCompare, String dimension, String columnName, String percent, String error,String wave) {
        this.tableId = tableId;
        this.value = value;
        this.valueCompare = valueCompare;
        this.dimension = dimension;
        this.columnName = columnName;
        this.percent = percent;
        this.error = error;
        this.wave = wave;
    }

    public DimensionResultBean(){

    }

    public String getWave() {
        return wave;
    }

    public void setWave(String wave) {
        this.wave = wave;
    }
}