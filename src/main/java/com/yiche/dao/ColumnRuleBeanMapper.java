package com.yiche.dao;

import com.yiche.bean.ColumnRuleBean;
import com.yiche.bean.TableRuleBean;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.Date;
import java.util.List;
@Mapper
public interface ColumnRuleBeanMapper {
    int deleteByPrimaryKey(Integer cid);

    int insert(ColumnRuleBean record);

    ColumnRuleBean selectByPrimaryKey(Integer cid);

    List<ColumnRuleBean> queryNeedRunList(@Param("nowTime")String nowTime);

    int updateByPrimaryKey(ColumnRuleBean record);


    /**
     * 获取项目下所有字段规则
     * @param parentId
     * @param id
     * @return
     */
    List<ColumnRuleBean> getColumnItemAll(@Param("parentId")Integer parentId, @Param("id")Integer id,
                                          @Param("partitionType")String partitionType);

    /**
     * 获取项目下所有字段规则未完成
     * @param parentId
     * @param id
     * @param day
     * @return
     */
    List<ColumnRuleBean> getColumnItemDnf(@Param("parentId")Integer parentId, @Param("id")Integer id, @Param("day")Integer day,
                                          @Param("partitionType")String partitionType);

    List<ColumnRuleBean> queryAll();

    void updateExecTime(@Param("nextExecTime") Date nextExecTime, @Param("cid") Long cid);

    String getColumnRuleById(@Param("id") String id);

}