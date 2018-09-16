package com.yiche.dao;

import com.yiche.bean.ItemModuleListBean;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;
@Mapper
public interface ItemModuleListBeanMapper {
    int deleteByPrimaryKey(Integer id);

    int insert(ItemModuleListBean record);

    ItemModuleListBean selectByPrimaryKey(Integer id);

    List<ItemModuleListBean> selectAll();

    int updateByPrimaryKey(ItemModuleListBean record);

    List<ItemModuleListBean>  selectProByTableId(@Param("tableId") String tableId);
    List<ItemModuleListBean>  selectModuleByTableId(@Param("tableId") String tableId);

    List<ItemModuleListBean>   selectModuleByColumnId(@Param("columnId") String columnId);
    List<ItemModuleListBean>  selectProByColumnId(@Param("columnId") String columnId);



}