package com.yiche.service;

import com.yiche.bean.ColumnRuleBean;

import java.util.List;
import java.util.Vector;

public interface ColumnRuleService {

    Vector<ColumnRuleBean> getColumnRule();

    void columnRuleRun();

    String columnRuleRun(ColumnRuleBean columnRuleBean);
}
