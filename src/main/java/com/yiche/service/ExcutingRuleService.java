package com.yiche.service;

import com.yiche.bean.RuleRunningLogBean;
import com.yiche.bean.TableRuleBean;
import org.apache.ibatis.annotations.Param;

import java.util.List;

public interface ExcutingRuleService {


    void runTableRule();

    void runColumnRule();

    List<RuleRunningLogBean> getDataByDataBaseAndTable(String dataBaseName, String tableName, Integer index, Integer limit);

    Integer getResultLogAllCount();

    Integer getResultLogCount(String dataBaseName, String tableName);

    void warnning(String alarmUniqueId, String dateBase, String tableName, String content, String column, String error, String value
            , String valueCompare, String scope, String id, String project, String waveScope, Integer checkDay, String partitionType);


    boolean isPartitionReady(String partitions, Integer day, String partitionType);

    void warningWhenExecRuleException(String subject, String body);

    void runIndexPro(String partitionType);

    void getTimeRuleNoPass();

   // void sqlWarnning(String alarmUniqueId, String dateBase, String tableName, String content);

    void runDataWarehourse(String partitionType, Integer id, String name);

    void runPro(String partitionType, Integer id, String proName);

    void runDistributorPro(String partitionType, Integer id, String proName);


}
