package com.yiche.dao;

import com.yiche.bean.RuleNotReadyBean;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;
@Mapper
public interface RuleNotReadyBeanMapper {
    int deleteByPrimaryKey(Integer id);

    int insert(RuleNotReadyBean record);

    RuleNotReadyBean selectByPrimaryKey(Integer id);

    List<RuleNotReadyBean> selectAll();

    int updateByPrimaryKey(RuleNotReadyBean record);


    int  updateStatusByFatherId(@Param("fatherId")String fatherId, @Param("status")Integer status);

    List<RuleNotReadyBean>  getByRuleId(@Param("ruleId")String ruleId, @Param("status")Integer status);
}