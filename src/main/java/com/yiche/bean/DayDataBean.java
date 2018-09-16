package com.yiche.bean;

import java.util.Date;

public class DayDataBean {
    private Integer id;

    private String databaseName;

    private String tableName;

    private Integer allCount;

    private Integer allRepeat;

    private Date createTime;

    private String columnName;

    private Integer nullNum;

    private Integer maxLenthNum;

    private Integer minLenthNum;

    private Integer avgLenthNum;

    private Integer zeroNum;

    private Integer specialNum;

    private Double sum;

    private Double xxhg;

    private Double ydpj;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
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

    public Integer getAllCount() {
        return allCount;
    }

    public void setAllCount(Integer allCount) {
        this.allCount = allCount;
    }

    public Integer getAllRepeat() {
        return allRepeat;
    }

    public void setAllRepeat(Integer allRepeat) {
        this.allRepeat = allRepeat;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public Integer getNullNum() {
        return nullNum;
    }

    public void setNullNum(Integer nullNum) {
        this.nullNum = nullNum;
    }

    public Integer getMaxLenthNum() {
        return maxLenthNum;
    }

    public void setMaxLenthNum(Integer maxLenthNum) {
        this.maxLenthNum = maxLenthNum;
    }

    public Integer getMinLenthNum() {
        return minLenthNum;
    }

    public void setMinLenthNum(Integer minLenthNum) {
        this.minLenthNum = minLenthNum;
    }

    public Integer getAvgLenthNum() {
        return avgLenthNum;
    }

    public void setAvgLenthNum(Integer avgLenthNum) {
        this.avgLenthNum = avgLenthNum;
    }

    public Integer getZeroNum() {
        return zeroNum;
    }

    public void setZeroNum(Integer zeroNum) {
        this.zeroNum = zeroNum;
    }

    public Integer getSpecialNum() {
        return specialNum;
    }

    public void setSpecialNum(Integer specialNum) {
        this.specialNum = specialNum;
    }

    public Double getSum() {
        return sum;
    }

    public void setSum(Double sum) {
        this.sum = sum;
    }

    public Double getXxhg() {
        return xxhg;
    }

    public void setXxhg(Double xxhg) {
        this.xxhg = xxhg;
    }

    public Double getYdpj() {
        return ydpj;
    }

    public void setYdpj(Double ydpj) {
        this.ydpj = ydpj;
    }
}