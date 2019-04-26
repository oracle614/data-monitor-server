package com.yiche.service;

import com.yiche.bean.RuleRunningLogBean;

import java.util.List;

public interface ExcutingRuleService {


    void runTableRule();

    void runColumnRule();

    List<RuleRunningLogBean> getDataByDataBaseAndTable(String dataBaseName, String tableName, Integer index, Integer limit);

    Integer getResultLogAllCount();

    Integer getResultLogCount(String dataBaseName, String tableName);

    Integer getResultLogStatusCount(String dataBaseName, String tableName, String status);

    void warning(String alarmUniqueId, String dateBase, String tableName, String content, String column, String error, String value
            , String valueCompare, String scope, String id, String project, String waveScope, Integer checkDay, String partitionType, String user, String priority);

    boolean isPartitionReady(String expectedPartition);

    void warningWhenExecRuleException(String subject, String body);

    void runIndexPro(String partitionType);

    void getTimeRuleNoPass();

    void runDataWarehourse(String partitionType);

    void runPro(String partitionType);

    void runDistributorPro(String partitionType);

    void sendTodayNoPassReport();

    void globalReport(String partitionType);

    void yipai(String partitionType);

    void dimensionWarning(String alarmUniqueId, String dateBase, String tableName, String content, String column, String error, String id, String project, Integer checkDay, String partitionType, String user, String priority, String errorMsg);
}
