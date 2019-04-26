package com.yiche.dao;

import com.yiche.bean.DimensionResultBean;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;
@Mapper
public interface DimensionResultBeanMapper {
    int deleteByPrimaryKey(Integer id);

    int insert(DimensionResultBean record);

    DimensionResultBean selectByPrimaryKey(Integer id);

    List<DimensionResultBean> selectAll();

    int updateByPrimaryKey(DimensionResultBean record);

    List<DimensionResultBean>queryTodayResultByTableId(@Param("tableId") String tableId);
}