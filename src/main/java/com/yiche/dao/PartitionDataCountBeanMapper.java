package com.yiche.dao;

import com.yiche.bean.PartitionDataCountBean;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

@Mapper
public interface PartitionDataCountBeanMapper {

    int insert(PartitionDataCountBean partitionDataCountBean);

    List<PartitionDataCountBean> getHistoryCount(@Param("databaseName") String dataBaseName, @Param("tableName") String tableName);

    List<PartitionDataCountBean> getHistoryCountByIsWeekend(@Param("databaseName") String dataBaseName, @Param("tableName") String tableName, @Param("isWeekend") String isWeekend);

    List<PartitionDataCountBean> getHistoryCountByPartition(@Param("databaseName") String dataBaseName, @Param("tableName") String tableName, @Param("partition") String partition);

    void deleteById(@Param("id") Integer id);

}