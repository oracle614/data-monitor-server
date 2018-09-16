package com.yiche.service.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.yiche.bean.ColumnRuleBean;
import com.yiche.bean.TableRuleBean;
import com.yiche.dao.ColumnRuleBeanMapper;
import com.yiche.dao.TableRuleBeanMapper;
import com.yiche.entity.JobOozieLineageEntity;
import com.yiche.service.SyncRuleExecTimeService;
import com.yiche.utils.CronExpression;
import com.yiche.utils.DateUtils;
import com.yiche.utils.HttpClientUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Date;
import java.util.List;

/**
 * Created by weiyongxu on 2018/7/27.
 */
@Service("syncRuleExecTimeService")
public class SyncRuleExecTimeServiceImpl implements SyncRuleExecTimeService{

    private Logger logger = LoggerFactory.getLogger(SyncRuleExecTimeService.class);

    @Autowired
    private ColumnRuleBeanMapper columnRuleBeanMapper;

    @Autowired
    private TableRuleBeanMapper tableRuleBeanMapper;

    @Value("${api.yu.url}")
    private String apiYuUrl = "http://yu.yiche.com/";

    private static String defaultExecTime = "00:00:00";

    @Override
    public void updateRuleExecTime() {
        logger.info("start update rule exectime");

        Date now = new Date();

        List<TableRuleBean> tableRuleList = tableRuleBeanMapper.queryAll();
        if(tableRuleList.size() > 0) {
            for (TableRuleBean tableRuleBean: tableRuleList) {
                Date ruleExecTime = getRuleExecTime(tableRuleBean.getDatabaseName(), tableRuleBean.getTableName());
                if(null != ruleExecTime) {
                    tableRuleBeanMapper.updateExecTime(ruleExecTime, Long.valueOf(tableRuleBean.getId()));
                } else {
                    String day = DateUtils.format(now, DateUtils.DATE_PATTERN);
                    ruleExecTime = DateUtils.parseStr(day + " " + defaultExecTime);
                    tableRuleBeanMapper.updateExecTime(ruleExecTime, Long.valueOf(tableRuleBean.getId()));
                    logger.error("get exectime fail, tableruleId:{}, databaseName:{},tableName{}", tableRuleBean.getId(), tableRuleBean.getDatabaseName(), tableRuleBean.getTableName());
                }
            }
        }

        List<ColumnRuleBean> columnRuleList = columnRuleBeanMapper.queryAll();
        if(columnRuleList.size() > 0) {
            for (ColumnRuleBean columnRuleBean: columnRuleList) {
                Date ruleExecTime = getRuleExecTime(columnRuleBean.getDatabaseName(), columnRuleBean.getTableName());
                if(ruleExecTime != null) {
                    columnRuleBeanMapper.updateExecTime(ruleExecTime, Long.valueOf(columnRuleBean.getCid()));
                } else {
                    String day = DateUtils.format(now, DateUtils.DATE_PATTERN);
                    ruleExecTime = DateUtils.parseStr(day + " " + defaultExecTime);
                    columnRuleBeanMapper.updateExecTime(ruleExecTime, Long.valueOf(columnRuleBean.getCid()));
                    logger.error("get exectime fail, columnruleId:{}, databaseName:{},tableName{}", columnRuleBean.getCid(), columnRuleBean.getDatabaseName(), columnRuleBean.getTableName());
                }
            }
        }

        logger.info("over update rule exectime");
    }


    // 在updateRuleExecTime执行完成后执行，防止updateRuleExecTime没调用成功导致任务不再执行
    @Override
    public void checkRuleExecTime() {
        logger.info("start check rule exectime");

        Date now = new Date();
        String day = DateUtils.format(now, DateUtils.DATE_PATTERN);
        Date defaultRuleExecTime = DateUtils.parseStr(day + " " + defaultExecTime);

        List<TableRuleBean> tableRuleList = tableRuleBeanMapper.queryAll();

        for (TableRuleBean tableRuleBean: tableRuleList) {
            Date nextExecTime = tableRuleBean.getNextExecTime();
            if(nextExecTime == null || nextExecTime.getTime() < defaultRuleExecTime.getTime()) {
                tableRuleBeanMapper.updateExecTime(defaultRuleExecTime, Long.valueOf(tableRuleBean.getId()));
                logger.info("check table rule exec time and update, nextExecTime:{},defaultRuleExecTime:{},tableRuleId:{}", nextExecTime, defaultRuleExecTime, tableRuleBean.getId());
            }
        }

        List<ColumnRuleBean> columnRuleList = columnRuleBeanMapper.queryAll();
        for (ColumnRuleBean columnRuleBean: columnRuleList) {
            Date nextExecTime = columnRuleBean.getNextExecTime();
            if(nextExecTime == null || nextExecTime.getTime() < defaultRuleExecTime.getTime()) {
                columnRuleBeanMapper.updateExecTime(defaultRuleExecTime, Long.valueOf(columnRuleBean.getCid()));
                logger.info("check column rule exec time and update, nextExecTime:{},defaultRuleExecTime:{},columnRuleId:{}", nextExecTime, defaultRuleExecTime, columnRuleBean.getCid());
            }
        }

        logger.info("over check rule exectime");
    }

    @Override
    public List<JobOozieLineageEntity> getLineageByHdbAndHtable(String databaseName, String tableName) throws IOException{
        String url = apiYuUrl + "api/v1/lineage/oozie/info?type=output&hdb=" + databaseName + "&htable=" + tableName;
        String res = null;
        try {
            res = HttpClientUtils.get(url);
        } catch (Exception e) {
            logger.error("get lineage fail, url:{},e:{}", url, e);
            throw new IOException(e.getMessage());
        }

        return buildJobOozieLineageEntityList(res);

    }

    @Override
    public Date getRuleExecTime(String databaseName, String tableName) {
        List<JobOozieLineageEntity> list;
        try {
            list = getLineageByHdbAndHtable(databaseName, tableName);
        } catch (IOException e) {
            logger.error("get rule exec time error, databaseName:{},tableName{}", databaseName, tableName);
            return null;
        }
        if(list.size() == 0) {
            logger.error("get rule exec time fail, list empty, databaseName:{},tableName{}", databaseName, tableName);
            return null;
        }

        JobOozieLineageEntity jobOozieLineageEntity = list.get(0);
        String cronExpression = jobOozieLineageEntity.getQuartz();
        return parseCronExpression(cronExpression);
    }

    private List<JobOozieLineageEntity> buildJobOozieLineageEntityList(String lineage) throws IOException{
        JSONObject jsonObject = JSONObject.parseObject(lineage);
        String code = jsonObject.getString("code");
        if(!"0".equals(code)) {
            logger.error("api return code error, code:{}, result:{}", code, lineage);
            throw new IOException("api code error");
        }

        JSONArray jobArray = jsonObject.getJSONArray("jobs");
        List<JobOozieLineageEntity> list = JSONObject.parseArray(jobArray.toJSONString(), JobOozieLineageEntity.class);
        return list;
    }

    private Date parseCronExpression(String cronExpression) {
        CronExpression ce;
        try {
            ce = new CronExpression(cronExpression + " ?");
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("parse cron expression error, expression:{}, e:{}", cronExpression, e);
            return null;
        }

        Date date = new Date();
        return ce.getNextValidTimeAfter(date);

    }
}
