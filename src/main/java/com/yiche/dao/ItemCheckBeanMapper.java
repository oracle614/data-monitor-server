package com.yiche.dao;

import com.yiche.bean.ItemCheckBean;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;
@Mapper
public interface ItemCheckBeanMapper {
    int deleteByPrimaryKey(Integer id);

    int insert(ItemCheckBean record);

    ItemCheckBean selectByPrimaryKey(Integer id);

    List<ItemCheckBean> selectAll();

    int updateByPrimaryKey(ItemCheckBean record);

    List<ItemCheckBean> selectItem(@Param("itemId") Integer itemId,@Param("day") Integer day);
}