package com.yiche.service.impl;


import com.yiche.bean.TableRuleBean;
import com.yiche.service.RuleService;

import java.sql.Statement;
import java.util.Map;

public class CountRuleServiceImpl  implements RuleService{

    @Override
    public String countCompare(TableRuleBean item, String partitions, Statement stmt) {
        return null;
    }

    @Override
    public Integer compareResult(TableRuleBean item, Map<String, String> dataMap, Statement stmt, String partitions, int dayNum) {
        return null;
    }
}
