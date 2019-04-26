package com.yiche.service;

import com.yiche.bean.RuleRunningLogBean;
import com.yiche.bean.TableRuleBean;

import java.util.List;
import java.util.Vector;

public interface TableRuleService {


    Vector<TableRuleBean> getTableRule();

    void tableRuleRun();

    String tableRuleRun(TableRuleBean  item);

    List<RuleRunningLogBean>  getResultLogByPage(String index,String limit);

    void getTimeRuleNoPass (TableRuleBean tableRuleBean);
}
