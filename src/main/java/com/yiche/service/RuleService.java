package com.yiche.service;

import com.yiche.bean.TableRuleBean;

import java.sql.Statement;
import java.util.Map;

public interface RuleService {

    /**
     * 查看表的规则，并遍历去比较
     * @param item
     * @param partitions
     * @param stmt
     * @return
     */
     String countCompare(TableRuleBean item, String partitions, Statement stmt);

    /**
     * 获取某天的记录结果
     * @param item
     * @param dataMap
     * @param stmt
     * @param partitions
     * @param dayNum
     * @return
     */
    Integer compareResult(TableRuleBean item, Map<String,String> dataMap, Statement stmt
            , String partitions, int dayNum);
}
