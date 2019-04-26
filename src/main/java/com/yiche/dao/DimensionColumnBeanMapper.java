package com.yiche.dao;

import com.yiche.bean.DimensionColumnBean;
import org.apache.ibatis.annotations.Mapper;

import java.util.List;
@Mapper
public interface DimensionColumnBeanMapper {
    int deleteByPrimaryKey(Integer id);

    int insert(DimensionColumnBean record);

    DimensionColumnBean selectByPrimaryKey(Integer id);

    List<DimensionColumnBean> selectAll();

    int updateByPrimaryKey(DimensionColumnBean record);

    List<String> getColumnById(String tableId);
}