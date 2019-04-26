package com.yiche.bean;

public class PartitionDataCountBean {
    private Integer id;

    private String databaseName;

    private String tableName;

    private Long dataCount;

    private String partition;

    private String isWeekend;


    @Override
    public String toString() {
        return "PartitionDataCountBean{" +
                "id=" + id +
                ", databaseName='" + databaseName + '\'' +
                ", tableName='" + tableName + '\'' +
                ", dataCount='" + dataCount + '\'' +
                ", partition='" + partition + '\'' +
                '}';
    }

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

    public String getPartition() {
        return partition;
    }

    public void setPartition(String partition) {
        this.partition = partition;
    }

    public String getIsWeekend() {
        return isWeekend;
    }

    public void setDataCount(Long dataCount) {
        this.dataCount = dataCount;
    }

    public Long getDataCount() {
        return dataCount;
    }

    public void setIsWeekend(String isWeekend) {
        this.isWeekend = isWeekend;
    }
}
