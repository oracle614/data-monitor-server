package com.yiche.dao;

import com.yiche.bean.DayDataBean;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;
@Mapper
public interface DayDataBeanMapper {
    int deleteByPrimaryKey(Integer id);

    int insert(DayDataBean record);

    DayDataBean selectByPrimaryKey(Integer id);

    List<DayDataBean> selectAll();

    int updateByPrimaryKey(DayDataBean record);

    List<DayDataBean> selectDayDataToday (@Param("databaseName")String dataBaseName, @Param("tableName")String tableName
            , @Param("columnName")String columnName,@Param("checkDay") Integer checkDay);

    List<DayDataBean> selectByTableInDay (@Param("databaseName")String dataBaseName, @Param("tableName")String tableName
            , @Param("columnName")String columnName   , @Param("day")Integer day);

    List<DayDataBean>  getDataByDataBaseAndTable(@Param("databaseName")String dataBaseName,@Param("tableName")String tableName);


}