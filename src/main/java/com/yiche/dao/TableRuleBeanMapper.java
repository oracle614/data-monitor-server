package com.yiche.dao;

import com.yiche.bean.TableRuleBean;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.Date;
import java.util.List;
@Mapper
public interface TableRuleBeanMapper {
    int deleteByPrimaryKey(Integer id);

    int insert(TableRuleBean record);

    TableRuleBean selectByPrimaryKey(Integer id);

    List<TableRuleBean> queryNeedRunList(@Param("nowTime")String nowTime);

    int updateByPrimaryKey(TableRuleBean record);




    /**
     * 获取项目下完成时间的规则
     * @param parentId
     * @param id
     * @return
     */
    List<TableRuleBean> getReadyItemAll(@Param("parentId")Integer parentId, @Param("id")Integer id, @Param("partitionType")String partitionType);

    /**
     * 获取项目下完成时间的规则 未执行
     * @param parentId
     * @param id
     * @param day
     * @return
     */
    List<TableRuleBean> getReadyItemAllDnf(@Param("parentId")Integer parentId, @Param("id")Integer id, @Param("day")Integer day,
                                           @Param("partitionType")String partitionType);


    /**
     * 获取项目下非完成时间的规则
     * @param parentId
     * @param id
     * @return
     */
    List<TableRuleBean> getItemAllDnr(@Param("parentId")Integer parentId, @Param("id")Integer id,
                                      @Param("partitionType")String partitionType);

    /**
     * 获取项目下非完成时间的规则 未执行
     * @param parentId
     * @param id
     * @param day
     * @return
     */
    List<TableRuleBean> getItemAllDnrDnf(@Param("parentId")Integer parentId, @Param("id")Integer id, @Param("day")Integer day,
                                         @Param("partitionType")String partitionType);

    List<TableRuleBean>  getTimeRuleNoPass(@Param("value")String value,@Param("day")Integer day);


    List<TableRuleBean>  getTableRuleByName(@Param("dateBaese")String dateBaese,@Param("tableName")String tableName,
                                            @Param("monitorType")String monitorType,@Param("self")String self);

    List<TableRuleBean> queryAll();

    void updateExecTime(@Param("nextExecTime") Date nextExecTime, @Param("id") Long id);
}