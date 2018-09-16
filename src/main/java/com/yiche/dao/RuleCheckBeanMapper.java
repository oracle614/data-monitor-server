package com.yiche.dao;

import com.yiche.bean.RuleCheckBean;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.Date;
import java.util.List;
@Mapper
public interface RuleCheckBeanMapper {
    int deleteByPrimaryKey(Integer id);

    int insert(RuleCheckBean record);

    RuleCheckBean selectByPrimaryKey(Integer id);

    List<RuleCheckBean> selectAll();

    int updateByPrimaryKey(RuleCheckBean record);

    List<RuleCheckBean>  getRuleCheck(@Param("databaseName")String dataBaseName
            , @Param("tableName")String tableName, @Param("columnName")String columnName
            , @Param("type")String type,@Param("checkDay") Integer checkDay);

    List<RuleCheckBean> getRuleCheckByRuleId(@Param("ruleId")String ruleId,@Param("checkDay") Integer checkDay);

    List<RuleCheckBean> getHistoryByRuleidAndDate(@Param("ruleId")String ruleId, @Param("checkDay") String checkDay);
}