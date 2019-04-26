package com.yiche.service;


import com.yiche.entity.JobOozieLineageEntity;

import java.io.IOException;
import java.util.Date;
import java.util.List;

/**
 * Created by weiyongxu on 2018/7/27.
 */
public interface SyncRuleExecTimeService {

    List<JobOozieLineageEntity> getLineageByHdbAndHtable(String databaseName, String tableName) throws IOException;

    Date getRuleExecTime(String databaseName, String tableName);

    void updateRuleExecTime();

    void checkRuleExecTime();
}
