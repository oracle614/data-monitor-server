package com.yiche.service.impl;

import com.sun.xml.bind.v2.TODO;
import com.yiche.bean.*;
import com.yiche.bussiness.PlatformBussiness;
import com.yiche.dao.*;
import com.yiche.db.ConnectFactory;
import com.yiche.em.DbType;
import com.yiche.service.*;
import com.yiche.utils.*;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.MultiValueMap;

import java.awt.*;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


@Service
public class TableRuleServiceImpl implements TableRuleService {


    private final Logger logger = LoggerFactory.getLogger(TableRuleServiceImpl.class);

    @Autowired
    TableRuleBeanMapper tableRuleBeanMapper;

    @Autowired
    RuleRunService ruleRunService;

    @Autowired
    DayDataBeanMapper dayDataBeanMapper;

    @Autowired
    RuleRunningLogBeanMapper ruleRunningLogBeanMapper;

    @Autowired
    NotifySysService notifySysService;

    @Autowired
    PlatformBussiness platformBussiness;

    @Autowired
    RuleCheckBeanMapper ruleCheckBeanMapper;

    /*@Autowired
    ConnectHiveService connectHiveService;*/

    @Autowired
    private ConnectFactory connectFactory;

    @Autowired
    ExcutingRuleService excutingRuleService;

    @Autowired
    ItemModuleListBeanMapper itemModuleListBeanMapper;

    @Autowired
    RuleNotReadyBeanMapper ruleNotReadyBeanMapper;

    @Autowired
    private SyncRuleExecTimeService syncRuleExecTimeService;

    @Autowired
    private DimensionColumnBeanMapper dimensionColumnBeanMapper;

    @Autowired
    private PartitionDataCountBeanMapper partitionDataCountBeanMapper;

    @Autowired
    private DimensionResultBeanMapper dimensionResultBeanMapper;
    @Value("${hdfs.url}")
    private String hdfsUrl;


    @Override
    public Vector<TableRuleBean> getTableRule() {
        Vector<TableRuleBean> vector = new Vector<>();
        String nowTime = DateUtils.format(new Date(), DateUtils.DATE_TIME_PATTERN);
        List<TableRuleBean> ruleList = tableRuleBeanMapper.queryNeedRunList(nowTime);
        filterTableRulesHasBeenRun(ruleList);
        filterTableRulesNotNeedRun(ruleList);
        filterTableRulesNotUpToETA(ruleList);
        if (ruleList.isEmpty()) {
            return vector;
        }
        vector.addAll(ruleList);
        return vector;
    }

    public void filterTableRulesNotNeedRun(List<TableRuleBean> tableRuleList) {
        if (tableRuleList.isEmpty()) {
            return;
        }
        Iterator<TableRuleBean> iterator = tableRuleList.iterator();
        while (iterator.hasNext()) {
            TableRuleBean item = iterator.next();
            if (!partionTypeMonthIsNeedRun(item)) {
                iterator.remove();
            }
        }
    }

    /*
    未到达预计执行时间   则过滤掉
     */
    public void filterTableRulesNotUpToETA(List<TableRuleBean> tableRuleList) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String nowTime = sdf.format(new Date());
        if (tableRuleList.isEmpty()) {
            return;
        }
        Iterator<TableRuleBean> iterator = tableRuleList.iterator();
        while (iterator.hasNext()) {
            TableRuleBean item = iterator.next();
            Date valueTime = null;
            if (!(item.getExeTime() == null || item.getExeTime().isEmpty())) {

                try {
                    valueTime = sdf.parse(DateFormatSafe.formatSign(new Date()) + " " + item.getExeTime());
                } catch (ParseException e) {
                    e.printStackTrace();
                }

                String ruleExeTime = sdf.format(valueTime);

            /*
            当前时间小于预计执行时间   则移除该项
             */
                if (nowTime.compareTo(ruleExeTime) < 0) {
                    iterator.remove();
                }
            }


        }
    }

    public boolean partionTypeMonthIsNeedRun(TableRuleBean item) {
        if (FinalVar.MONTH.equals(item.getPartitionType()) && !DateFormatSafe.isFirstDay()) {
            return false;
        }
        return true;
    }


    public void filterTableRulesHasBeenRun(List<TableRuleBean> tableRuleList) {
        if (tableRuleList.isEmpty()) {
            return;
        }

        Iterator<TableRuleBean> iterator = tableRuleList.iterator();
        while (iterator.hasNext()) {
            TableRuleBean item = iterator.next();
            if (ruleAlreadyRuned(item)) {
                iterator.remove();
            }
        }
    }

    @Override
    public void tableRuleRun() {
        //获取所有表规则
        Vector<TableRuleBean> vector = getTableRule();
        if (vector == null || vector.size() == 0) {
            logger.info("No need run table rules");
            return;
        }

        //固定5个线程去跑所有表规则
        ExecutorService cachedThreadPool = Executors.newFixedThreadPool(5);

        vector.forEach(item -> {
            cachedThreadPool.execute(() -> {
                logger.info("{}:运行,连接总数:!{}!", Thread.currentThread().getName(), FinalVar.dbConnCount);
                Statement stmt = null;
                Connection con = null;
                try {
                    con = connectFactory.createConnect(DbType.Presto);
                    stmt = con.createStatement();

//                    String partitions = ruleRunService.getNewestPartition(item.getDatabaseName(), item.getTableName(), stmt);
                    String partitions = ruleRunService.getExpectedPartition(item.getDatabaseName(), item.getTableName(), stmt, item.getCheckDay(), item.getPartitionType());

                    logger.info("获取最新分区:itemId:{},dataBase:{},tableName:{},patitions:{}", item.getTid(), item.getDatabaseName(), item.getTableName(), partitions);

                    /*
                    1.获取期望分区
                    2.判断期望分区是否存在(为""）
                     */

                    if (!excutingRuleService.isPartitionReady(partitions)) {
                        logger.info("分区没有生成:itemId:{},dataBase:{},tableName:{}", item.getTid(), item.getDatabaseName(), item.getTableName());

                        if (item.getMonitorType().equals(FinalVar.RULE_END_TIME)) {
                            tableEndTimeRuleExecWhenPartitionNotReady(item, true);
                        }
                        return;
                    }

                    switch (item.getMonitorType()) {
                        case FinalVar.RULE_TABLE_COUNT:
                            tableDataCountRuleExec(item, partitions, stmt, true);
                            break;
                        case "执行时长":
                            logger.error("没有处理执行时长,itemId:{}", item.getTid());
                            break;
                        case FinalVar.RULE_AUTO_THRESHOLD:
                            logger.info("自动阈值规则,itemId:{}", item.getTid());
                            adaptiveThresholdRuleExec(item, partitions, stmt, true);
                            break;
                        case FinalVar.RULE_END_TIME:
                            tableEndTimeRuleExec(item, true);
                            break;
                        case FinalVar.RULE_DIMENSION_GROUP:
                            tableDimensionGroupRuleExec(item, partitions, stmt, true);
                            break;
                        case FinalVar.RULE_DIMENSION:
                            tableDimensionRuleExec(item, partitions, stmt, true);
                            break;
                        default:
                            logger.error("没有此种类型规则,itemId:{}", item.getTid());
                            break;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    logger.error("执行规则异常,itemId:{},exception:{}", item.getTid(), e);
                    alarmWhenExecRuleException(item.getId(), e.getMessage());
                } finally {
                    closeAfterQuery(stmt, con);
                }
            });
        });

        cachedThreadPool.shutdown();

    }

    private void alarmWhenExecRuleException(int id, String msg) {

        excutingRuleService.warningWhenExecRuleException("表规则执行异常(id:" + id + ")", msg);
    }

    public void closeAfterQuery(Statement stmt, Connection con) {
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e) {
                logger.error("stmt关闭失败", e);
            }
        }
        if (con != null) {
            try {
                con.close();
                FinalVar.dbConnCount--;
                logger.info("hiveConnect关闭");
            } catch (SQLException e) {
                FinalVar.dbConnCount++;
                logger.error("hiveConnect关闭失败", e);
            }
        }
    }


    public String tableRuleRun(TableRuleBean item) {
        logger.info("{}:运行,hive连接总数:!{}!", Thread.currentThread().getName(), FinalVar.dbConnCount);

        Statement stmt = null;
        Connection con = null;
        try {
            con = connectFactory.createConnect(DbType.Presto);
            stmt = con.createStatement();

            String partitions = ruleRunService.getExpectedPartition(item.getDatabaseName(), item.getTableName(), stmt, item.getCheckDay(), item.getPartitionType());
            logger.info("获取最新分区:itemId:{},dataBase:{},tableName:{},partitions:{}", item.getTid(), item.getDatabaseName(), item.getTableName(), partitions);
            /*
             1.获取期望分区
             2.判断期望分区是否存在(为""）
             */
            if (!excutingRuleService.isPartitionReady(partitions)) {
                logger.info("分区没有生成:itemId:{},dataBase:{},tableName:{}", item.getTid(), item.getDatabaseName(), item.getTableName());

                if (FinalVar.RULE_END_TIME.equals(item.getMonitorType())) {
                    tableEndTimeRuleExecWhenPartitionNotReady(item, false);
                    return "规则执行完成";
                }
                return "日分区未生成";
            }

            switch (item.getMonitorType()) {
                case FinalVar.RULE_TABLE_COUNT:
                    tableDataCountRuleExec(item, partitions, stmt, false);
                    break;
                case "执行时长":
                    logger.error("没有处理执行时长,itemId:{}", item.getTid());
                    break;
                case FinalVar.RULE_AUTO_THRESHOLD:
                    logger.error("自动阈值规则,itemId:{}", item.getTid());
                    adaptiveThresholdRuleExec(item, partitions, stmt, false);
                    break;
                case FinalVar.RULE_END_TIME:
                    tableEndTimeRuleExec(item, false);
                    break;
                default:
                    logger.error("没有此种类型规则,itemId:{}", item.getTid());
                    break;
            }
        } catch (Exception e) {
            logger.error("执行规则异常", e);
            alarmWhenExecRuleException(item.getId(), e.getMessage());
        } finally {
            closeAfterQuery(stmt, con);
        }
        return "规则执行完成";
    }

    @Override
    public void getTimeRuleNoPass(TableRuleBean item) {
        logger.info("开始执行未通过规则，{}:运行,hive连接总数:!{}!", Thread.currentThread().getName(), FinalVar.dbConnCount);
        Statement stmt = null;
        Connection con = null;
        try {
            con = connectFactory.createConnect(DbType.Presto);
            stmt = con.createStatement();
            //判断分区和数据是否生成

            String partitions = ruleRunService.getExpectedPartition(item.getDatabaseName(), item.getTableName(), stmt, item.getCheckDay(), item.getPartitionType());
// String partitions="pt=2018-06-12";

                    /*
                    1.获取期望分区
                    2.判断期望分区是否存在(为""）
                     */
            if (!excutingRuleService.isPartitionReady(partitions)) {
                logger.info("当天分区没有生成:dataBase:{},tableName{},patitions:{},type:{},checkDay:{},partitionType:{}",
                        item.getDatabaseName(), item.getTableName(), partitions, item.getMonitorType(), item.getCheckDay(), item.getPartitionType());
                List<ItemModuleListBean> itemModuleListBeanList = itemModuleListBeanMapper.selectProByTableId(item.getTid());
                String name = getProName(item.getTid());
                //报警;
                excutingRuleService.warning(item.getAlarmUniqueId(), item.getDatabaseName(), item.getTableName(), item.getContent()
                        , null, "完成时间", "未就绪"
                        , item.getSelf(), null, item.getNumber(), name, null, item.getCheckDay(), item.getPartitionType(), item.getTowner(), item.getPriority());
                return;
            } else {
                logger.info("未通过规则检测,分区已经生成,tid:{},partitions:{}", item.getTid(), partitions);
                WarnResultBean warnResultBean = finishTimeCompare(item);
                //报警
                excutingRuleService.warning(item.getAlarmUniqueId(), item.getDatabaseName(), item.getTableName(), item.getContent()
                        , null, "完成时间", warnResultBean.getValue()
                        , item.getSelf(), null, item.getNumber(), warnResultBean.getProject(),
                        warnResultBean.getScope(), item.getCheckDay(), item.getPartitionType(), item.getTowner(), item.getPriority());
                //修改log
                ruleRunningLogBeanMapper.updateStatusByRuleId(warnResultBean.getValue(), item.getTid(),
                        0);
            }
        } catch (Exception e) {
            logger.error("执行规则异常", e);
            alarmWhenExecRuleException(item.getId(), e.getMessage());
        } finally {
            closeAfterQuery(stmt, con);
        }
    }

    @Override
    public List<RuleRunningLogBean> getResultLogByPage(String index, String limit) {
        return ruleRunningLogBeanMapper.getResultLogByPage(Integer.valueOf(index), Integer.valueOf(limit));
    }

    public void tableDataCountRuleExec(TableRuleBean item, String partitions, Statement stmt, boolean addCheckLog) throws Exception {
        WarnResultBean warnResultBean;
//        if(StringUtils.isEmpty(item.getDimension())) {
        warnResultBean = countCompare(item, partitions, stmt, item.getTcondition());
//       }else{
//            warnResultBean = countCompareDimension(item, partitions, stmt, item.getTcondition());
//       }
        insertResultLog(item.getTowner(), "表级别", item.getMonitorType()
                , warnResultBean.isFlag() ? FinalVar.PASS : FinalVar.NOPASS
                , warnResultBean, item.getDatabaseName(), item.getTableName(), item.getTid(), item.getContent(), item.getPartitionType(),
                warnResultBean.getError(), item.getPriority(), item.getNumber(), item.getCheckDay());

        if (addCheckLog) {
            addRuleCheck(item, FinalVar.RULE_TABLE_COUNT, FinalVar.TABLECOLUMN);
        }

        logger.info("表规则数据量检测执行完成:database:{},tableName:{},itemId:{}", item.getDatabaseName(),
                item.getTableName(), item.getTid());
    }

    public void tableDimensionGroupRuleExec(TableRuleBean item, String partitions, Statement stmt, boolean addCheckLog) throws Exception {
        boolean isPass;
        String proName = getProName(item.getTid());
        isPass = countCompareDimensionGroup(item, partitions, stmt, item.getTcondition(), proName);
        if (!isPass) {
            sendDimensionWarning(item, proName);
        }
        insertDimesionResultLog(item.getTowner(), "表级别", item.getMonitorType()
                , isPass ? FinalVar.PASS : FinalVar.NOPASS
                , item.getDatabaseName(), item.getTableName(), item.getTid(), item.getContent(), item.getPartitionType(), item.getPriority(), item.getNumber(), item.getCheckDay(), proName);
        if (addCheckLog) {
            addRuleCheck(item, FinalVar.RULE_DIMENSION_GROUP, FinalVar.TABLECOLUMN);
        }
        logger.info("表规则维度检测执行完成:database:{},tableName:{},itemId:{}", item.getDatabaseName(),
                item.getTableName(), item.getTid());
    }

    public void tableDimensionRuleExec(TableRuleBean item, String partitions, Statement stmt, boolean addCheckLog) throws Exception {
        boolean isPass;
        String proName = getProName(item.getTid());
        isPass = countCompareDimension(item, partitions, stmt, item.getTcondition(), proName);
        if (!isPass) {
            sendDimensionWarning(item, proName);
        }
        insertDimesionResultLog(item.getTowner(), "表级别", item.getMonitorType()
                , isPass ? FinalVar.PASS : FinalVar.NOPASS
                , item.getDatabaseName(), item.getTableName(), item.getTid(), item.getContent(), item.getPartitionType(), item.getPriority(), item.getNumber(), item.getCheckDay(), proName);
        if (addCheckLog) {
            addRuleCheck(item, FinalVar.RULE_DIMENSION, FinalVar.TABLECOLUMN);
        }
        logger.info("表规则维度检测执行完成:database:{},tableName:{},itemId:{}", item.getDatabaseName(),
                item.getTableName(), item.getTid());
    }

    public void tableEndTimeRuleExec(TableRuleBean item, boolean addCheckLog) throws Exception {
        WarnResultBean warnResultBean = finishTimeCompare(item);
        insertResultLog(item.getTowner(), "表级别", item.getMonitorType()
                , warnResultBean.isFlag() ? FinalVar.PASS : FinalVar.NOPASS
                , warnResultBean, item.getDatabaseName(), item.getTableName(), item.getTid(), item.getContent(), item.getPartitionType(),
                warnResultBean.getError(), item.getPriority(), item.getNumber(), item.getCheckDay());


        if (addCheckLog) {
            addRuleCheck(item, FinalVar.RULE_END_TIME, FinalVar.TABLECOLUMN);
        }

        logger.info("success完成规则执行完成:database:{},tableName:{},itemtId:{}", item.getDatabaseName(),
                item.getTableName(), item.getTid());
    }


    private void sqlRule(TableRuleBean item, String partitions, Statement stmt) {
        /*if(isCheck(item, FinalVar.RULE_SQL,FinalVar.TABLECOLUMN)){
            return;
        }*/
        WarnResultBean warnResultBean = sqlCompare(item, partitions, stmt);
        insertResultLog(item.getTowner(), "表级别", item.getMonitorType()
                , FinalVar.PASS
                , warnResultBean, item.getDatabaseName(), item.getTableName(), item.getTid(), item.getContent(), item.getPartitionType(),
                warnResultBean.getError(), item.getPriority(), item.getNumber(), item.getCheckDay());
        logger.info("database:{},tableName:{}-sql自定义规则执行完成", item.getDatabaseName()
                , item.getTableName());
    }


    private void tableEndTimeRuleExecWhenPartitionNotReady(TableRuleBean item, boolean addCheckLog) throws Exception {
        WarnResultBean warnResultBean = finishTimeCompareNoPar(item);
        if (warnResultBean.isFlag()) {
            return;
        }

        insertResultLog(item.getTowner(), "表级别", item.getMonitorType()
                , FinalVar.NOPASS, warnResultBean, item.getDatabaseName(), item.getTableName(), item.getTid(), item.getContent(), item.getPartitionType(),
                warnResultBean.getError(), item.getPriority(), item.getNumber(), item.getCheckDay());

        if (addCheckLog) {
            addRuleCheck(item, FinalVar.RULE_END_TIME, FinalVar.TABLECOLUMN);
        }

        logger.info("success完成规则执行完成,database:{},tableName:{},itemId:{}", item.getDatabaseName()
                , item.getTableName(), item.getTid());
    }


    private WarnResultBean finishTimeCompareNoPar(TableRuleBean item) throws Exception {
        logger.info("执行finishTimeNoPar规则:dataBase:{},tableName:{}", item.getDatabaseName(), item.getTableName());
        Calendar ruleCalendar = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date valueTime = null;
        try {
            valueTime = sdf.parse(DateFormatSafe.formatSign(new Date()) + " " + item.getSelf());
            ruleCalendar.setTime(valueTime);
        } catch (ParseException e) {
            e.printStackTrace();
            throw new Exception("规则时间解析失败");
        }

        String ruleTime = sdf.format(valueTime);
        List<ItemModuleListBean> itemModuleListBeanList = itemModuleListBeanMapper.selectProByTableId(item.getTid());
        //获取规则关联的模块名称
        String name = getProName(item.getTid());
        String nowTime = sdf.format(new Date());
        WarnResultBean warnResultBean = new WarnResultBean();
        warnResultBean.setFlag(true);
        logger.info("当前时间:{},     规则时间:{}", nowTime, ruleTime);
        if (nowTime.compareTo(ruleTime) > 0) {
            excutingRuleService.warning(item.getAlarmUniqueId(), item.getDatabaseName(), item.getTableName(), item.getContent()
                    , null, "完成时间", "未就绪", ruleTime, null, item.getNumber(), name, null,
                    item.getCheckDay(), item.getPartitionType(), item.getTowner(), item.getPriority());
            warnResultBean.setProject(name);
            warnResultBean.setValue("未就绪");
            warnResultBean.setCompareValue(ruleTime);
            warnResultBean.setError("完成时间");
            warnResultBean.setFlag(false);
            return warnResultBean;
        }

        return warnResultBean;
    }

    private WarnResultBean finishTimeCompare(TableRuleBean item) throws Exception {
        logger.info("执行finishTime规则:id:{},dataBase:{},tableName:{}", item.getNumber(), item.getDatabaseName(), item.getTableName());
        String path = String.format("/bitauto/sign/%s/%s/%s/_SUCCESS", item.getDatabaseName(), item.getTableName()
                , DateFormatSafe.formatSign(DateFormatSafe.getDay(item.getCheckDay())));
        logger.info("success文件path:{}", path);
        Calendar ruleCalendar = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date valueTime = null;
        try {
            valueTime = sdf.parse(DateFormatSafe.formatSign(new Date()) + " " + item.getSelf());
            ruleCalendar.setTime(valueTime);
        } catch (ParseException e) {
            logger.info("value:{}", item.getSelf());
            logger.error("规则时间解析失败", e);
        }
        String ruleTime = sdf.format(valueTime);
        HttpFSFileSystem hdfs = new HttpFSFileSystem();
        String name = getProName(item.getTid());
        WarnResultBean warnResultBean = new WarnResultBean();
        warnResultBean.setProject(name);
        warnResultBean.setFlag(true);
        warnResultBean.setCompareValue(ruleTime);
        warnResultBean.setError("success完成时间");
        URI uri;
        Long time = null;
        try {
            uri = new URI(hdfsUrl + path);
            hdfs.initialize(uri, "luozhenyu");
            FileStatus fileStatus = hdfs.getFileStatus(uri);
            time = fileStatus.getModificationTime();
        } catch (URISyntaxException e) {
            logger.error("链接hdfs解析失败", e);
            throw new Exception("链接hdfs解析失败");
//            updateTableSonStatus(item.getTid());
        } catch (IOException e) {
            logger.info("文件不存在", e);
            warnResultBean.setValue("success文件未生成");
            logger.info(" succes文件没有生成  table:{}", item.getDatabaseName() + "." + item.getTableName());
//            updateTableSonStatus(item.getTid());
            return warnResultBean;
        }
        String modifyTime = sdf.format(time);
        warnResultBean.setValue(modifyTime);
        long t = valueTime.getTime() > time ? valueTime.getTime() - time : time - valueTime.getTime();
        String diffTime = getTime(t);
        logger.info("修改时间:{}, 规则时间:{},时间差:{}   ", modifyTime, ruleTime, diffTime);
        warnResultBean.setScope(diffTime);
//        updateTableSonStatus(item.getTid());
        return warnResultBean;
    }

    private String getTime(long diff) {
        long days = diff / (1000 * 60 * 60 * 24);
        long hours = (diff - days * (1000 * 60 * 60 * 24)) / (1000 * 60 * 60);
        long minutes = (diff - days * (1000 * 60 * 60 * 24) - hours * (1000 * 60 * 60)) / (1000 * 60);
        return "" + days + "天" + hours + "小时" + minutes + "分";
    }

    private WarnResultBean sqlCompare(TableRuleBean item, String partitions, Statement stmt) {
        logger.info("执行数据量规则:id:{}dataBase:{},tableName:{}", item.getNumber(), item.getDatabaseName(), item.getTableName());
//      String  result = ruleRunService.getSql(item.getColumnName(),stmt,partitions,DateFormatSafe.getDay(item.getCheckDay()),item.getSql());
//            excutingRuleService.sqlWarnning(item.getAlarmUniqueId(),item.getDatabaseName(),item.getTableName(),result);
        WarnResultBean warnResultBean = new WarnResultBean();
        return warnResultBean;
    }

    private WarnResultBean countCompare(TableRuleBean item, String partitions, Statement stmt, String where) throws Exception {
        //获取表的count
        logger.info("执行数据量规则:id:{}dataBase:{},tableName:{}", item.getNumber(), item.getDatabaseName(), item.getTableName());
//        int countNow = ruleRunService.getCount(item.getDatabaseName(), item.getTableName(), null
//                , stmt, partitions, DateFormatSafe.getDay(item.getCheckDay()),where);
        Long countNow = ruleRunService.getCount(item.getDatabaseName(), item.getTableName(), null
                , stmt, partitions,
                FinalVar.MONTH.equals(item.getPartitionType()) ? DateFormatSafe.getMonth(item.getCheckDay()) : DateFormatSafe.getDay(item.getCheckDay()),
                where, item.getPartitionType());
        String name = getProName(item.getTid());
        WarnResultBean warnResultBean = new WarnResultBean();
        warnResultBean.setValue(String.valueOf(countNow));
        warnResultBean.setFlag(true);
        warnResultBean.setProject(name);
        Map<String, String> dataMap = new HashMap<>();
        //环比
        if (!StringUtils.isEmpty(item.gethCompare())) {
            Long dayCount = compareResult(item, dataMap, stmt, partitions, 1 + item.getCheckDay(), where);
            double devition = countNow - dayCount;
            Double value = devition / (dayCount == 0 ? 1 : dayCount);
            String wave = String.format("%.2f", value * 100);
            logger.info("count环比:id:{},value:{},valueCompare:{},scope:{},wave:{}dataBase:{},tableName:{}",
                    item.getNumber(), countNow, dayCount, item.gethCompare(), wave, item.getDatabaseName(), item.getTableName());
            if (!PatternRule.isRule(item.gethCompare(), wave)) {
                warnResultBean.setFlag(false);
                switch (item.getPartitionType()) {
                    case FinalVar.MONTH:
                        warnResultBean.setError("数据量月环比");
                        break;
                    case FinalVar.DAY:
                        warnResultBean.setError("数据量日环比");
                        break;
                    default:
                        warnResultBean.setError("数据量日环比");
                }
                warnResultBean.setCompareValue(TypeConvert.LongConvertString(dayCount));
                warnResultBean.setScope(item.gethCompare());
                excutingRuleService.warning(item.getAlarmUniqueId(), item.getDatabaseName(), item.getTableName(), item.getContent()
                        , null, warnResultBean.getError(), TypeConvert.LongConvertString(countNow)
                        , TypeConvert.LongConvertString(dayCount), item.gethCompare(), item.getNumber(), name, wave + "%", item.getCheckDay(), item.getPartitionType(), item.getTowner(), item.getPriority());
                return warnResultBean;
            }

        }
        //同比
        if (!StringUtils.isEmpty(item.gettCompare())) {
            Long dayCount = compareResult(item, dataMap, stmt, partitions, 7 + item.getCheckDay(), where);
            double devition = countNow - dayCount;
            Double value = devition / (dayCount == 0 ? 1 : dayCount);
            String wave = String.format("%.2f", value * 100);
            logger.info("count同比:id:{},value:{},valueCompare:{},scope:{},wave:{},dataBase:{},tableName:{}",
                    item.getNumber(), countNow, dayCount, item.gethCompare(), wave, item.getDatabaseName(), item.getTableName());
            if (!PatternRule.isRule(item.gettCompare(), wave)) {
                warnResultBean.setFlag(false);
                switch (item.getPartitionType()) {
                    case FinalVar.MONTH:
                        warnResultBean.setError("数据量月同比");
                        break;
                    case FinalVar.DAY:
                        warnResultBean.setError("数据量周同比");
                        break;
                    default:
                        warnResultBean.setError("数据量周同比");
                }
                warnResultBean.setCompareValue(TypeConvert.LongConvertString(dayCount));
                warnResultBean.setScope(item.gettCompare());
                excutingRuleService.warning(item.getAlarmUniqueId(), item.getDatabaseName(), item.getTableName(), item.getContent()
                        , null, warnResultBean.getError(), TypeConvert.LongConvertString(countNow)
                        , TypeConvert.LongConvertString(dayCount), item.gettCompare(), item.getNumber(), name, wave + "%", item.getCheckDay(), item.getPartitionType(), item.getTowner(), item.getPriority());
                return warnResultBean;
            }
        }
        //七天
        if (!StringUtils.isEmpty(item.getSevenWaveAvg())) {
            Integer dayCount = ruleRunService.selectSevenDayDate(item.getDatabaseName(), item.getTableName(), stmt, partitions, null
//                    ,DateFormatSafe.formatSign(DateFormatSafe.getDay(6+item.getCheckDay()+1))
                    , FinalVar.MONTH.equals(item.getPartitionType()) ?
                            DateFormatSafe.formatMonth(DateFormatSafe.getMonth(6 + item.getCheckDay() + 1)) :
                            DateFormatSafe.formatSign(DateFormatSafe.getDay(6 + item.getCheckDay() + 1))
                    , FinalVar.MONTH.equals(item.getPartitionType()) ?
                            DateFormatSafe.formatMonth(DateFormatSafe.getMonth(item.getCheckDay() + 1)) :
                            DateFormatSafe.formatSign(DateFormatSafe.getDay(item.getCheckDay() + 1)));
            double devition = countNow - dayCount;
            Double value = devition / (dayCount == 0 ? 1 : dayCount);
            String wave = String.format("%.2f", value * 100);
            logger.info("count7天（月）波动:id:{},value:{},valueCompare:{},scope:{},wave:{},dataBase:{},tableName:{}",
                    item.getNumber(), countNow, dayCount, item.gethCompare(), wave, item.getDatabaseName(), item.getTableName());
            if (!PatternRule.isRule(item.getSevenWaveAvg(), wave)) {
                excutingRuleService.warning(item.getAlarmUniqueId(), item.getDatabaseName(), item.getTableName(), item.getContent()
                        , null, "数据量七天比", TypeConvert.LongConvertString(countNow)
                        , TypeConvert.IntegerConvertString(dayCount), item.getSevenWaveAvg(), item.getNumber(), name, wave + "%", item.getCheckDay(), item.getPartitionType(), item.getTowner(), item.getPriority());
                warnResultBean.setFlag(false);
                warnResultBean.setError("数据量七天比");
                warnResultBean.setCompareValue(TypeConvert.IntegerConvertString(dayCount));
                warnResultBean.setScope(item.getSevenWaveAvg());
                return warnResultBean;
            }
        }
        //self
        if (!StringUtils.isEmpty(item.getSelf()) && !PatternRule.isRule(item.getSelf(), countNow)) {
            logger.info("自身比:id:{},value:{},scope{},dataBase:{},tableName:{}", item.getNumber(), countNow, item.getSelf()
                    , item.getDatabaseName(), item.getTableName());
            excutingRuleService.warning(item.getAlarmUniqueId(), item.getDatabaseName(), item.getTableName(), item.getContent(), null
                    , "数据量自身比", TypeConvert.LongConvertString(countNow)
                    , null, item.getSelf(), item.getNumber(), name, null, item.getCheckDay(), item.getPartitionType(), item.getTowner(), item.getPriority());
            warnResultBean.setFlag(false);
            warnResultBean.setError("数据量自身比");
            warnResultBean.setCompareValue(null);
            warnResultBean.setScope(item.getSelf());
            return warnResultBean;
        }
        return warnResultBean;
    }

    private Boolean countCompareDimensionGroup(TableRuleBean item, String partitions, Statement stmt, String where, String proName) throws Exception {
        //获取表的count
        logger.info("执行维度group规则:id:{}dataBase:{},tableName:{}", item.getNumber(), item.getDatabaseName(), item.getTableName());
        boolean isPass = true;
        List<String> columnList = dimensionColumnBeanMapper.getColumnById(item.getTid());
        MultiValueMap<String, String> dimensionMap = ruleRunService.getDimensionDataGroupBy(item.getDatabaseName(), item.getTableName(), item.getColumnName()
                , stmt, partitions,
                FinalVar.MONTH.equals(item.getPartitionType()) ? DateFormatSafe.getMonth(item.getCheckDay()) : DateFormatSafe.getDay(item.getCheckDay()),
                where, item.getPartitionType(), columnList);
        WarnResultBean warnResultBean = new WarnResultBean();
        warnResultBean.setFlag(true);

        if (!StringUtils.isEmpty(item.gethCompare())) {
            boolean flag;
            MultiValueMap<String, String> dimensionMapH = compareDimensionGroupByResult(item, stmt, partitions,
                    1 + item.getCheckDay(), where, columnList, item.getColumnName());
            flag = dimensionCompare(item, dimensionMap, columnList, warnResultBean, isPass,
                    "维度环比", item.gethCompare(), dimensionMapH);
            isPass = isPass && flag;
        }

        if (!StringUtils.isEmpty(item.gettCompare())) {
            boolean flag;
            MultiValueMap<String, String> dimensionMapH = compareDimensionGroupByResult(item, stmt, partitions,
                    7 + item.getCheckDay(), where, columnList, item.getColumnName());
            flag = dimensionCompare(item, dimensionMap, columnList, warnResultBean, isPass,
                    "维度同比", item.gettCompare(), dimensionMapH);
            isPass = isPass && flag;
        }
        return isPass;
    }

    private Boolean countCompareDimension(TableRuleBean item, String partitions, Statement stmt, String where, String proName) throws Exception {
        //获取表的count
        logger.info("执行维度规则:id:{}dataBase:{},tableName:{}", item.getNumber(), item.getDatabaseName(), item.getTableName());
        boolean isPass = true;
        List<String> columnList = dimensionColumnBeanMapper.getColumnById(item.getTid());
        MultiValueMap<String, String> dimensionMap = ruleRunService.getDimensionData(item.getDatabaseName(), item.getTableName(), item.getColumnName()
                , stmt, partitions,
                FinalVar.MONTH.equals(item.getPartitionType()) ? DateFormatSafe.getMonth(item.getCheckDay()) : DateFormatSafe.getDay(item.getCheckDay()),
                where, item.getPartitionType(), columnList);
        WarnResultBean warnResultBean = new WarnResultBean();
        warnResultBean.setFlag(true);

        if (!StringUtils.isEmpty(item.gethCompare())) {
            boolean flag;
            MultiValueMap<String, String> dimensionMapH = compareDimensionResult(item, stmt, partitions,
                    1 + item.getCheckDay(), where, columnList, item.getColumnName());
            flag = dimensionCompare(item, dimensionMap, columnList, warnResultBean, isPass,
                    "维度环比", item.gethCompare(), dimensionMapH);
            isPass = isPass && flag;
        }

        if (!StringUtils.isEmpty(item.gettCompare())) {
            boolean flag;
            MultiValueMap<String, String> dimensionMapH = compareDimensionResult(item, stmt, partitions,
                    7 + item.getCheckDay(), where, columnList, item.getColumnName());
            flag = dimensionCompare(item, dimensionMap, columnList, warnResultBean, isPass,
                    "维度同比", item.gettCompare(), dimensionMapH);
            isPass = isPass && flag;
        }
        return isPass;
    }


    private boolean dimensionCompare(TableRuleBean item, MultiValueMap<String, String> dimensionMap, List<String> columnList, WarnResultBean warnResultBean, boolean isPass,
                                     String typeName, String compare, MultiValueMap<String, String> dimensionMapH) throws Exception {

        Set<String> keySet = dimensionMap.keySet();
        Set<String> keySetH = dimensionMapH.keySet();
        Set<String> keySetAll = new HashSet<>();
        keySetAll.addAll(keySet);
        keySetAll.addAll(keySetH);

        for (String column : keySetAll) {
            List<String> dimensionList = dimensionMap.get(column);
            List<String> dimensionListH = dimensionMapH.get(column);
            for (int i = 0; i < columnList.size(); i++) {
                Integer value;
                Integer valueCompare;
                if (dimensionList == null || dimensionList.isEmpty()) {
                    value = 0;
                } else {
                    value = TypeConvert.StringConvertInteger(dimensionList.get(i));
                }

                if (dimensionListH == null || dimensionListH.isEmpty()) {
                    valueCompare = 0;
                } else {
                    valueCompare = TypeConvert.StringConvertInteger(dimensionListH.get(i));
                }
                warnResultBean = getCompareResult(value, valueCompare, item, typeName, compare);
                if (isPass && !warnResultBean.isFlag()) {
                    isPass = warnResultBean.isFlag();
                }
                if (!warnResultBean.isFlag()) {
                    dimensionResultBeanMapper.insert(new DimensionResultBean(item.getTid(),
                            TypeConvert.IntegerConvertString(value), TypeConvert.IntegerConvertString(valueCompare), column, columnList.get(i), warnResultBean.getWave(), warnResultBean.getError(), compare));
                }
            }
        }
        return isPass;
    }


    public void adaptiveThresholdRuleExec(TableRuleBean item, String partitions, Statement stmt, boolean addCheckLog) throws Exception {

        WarnResultBean warnResultBean = adaptiveThresholdRule(item, partitions, stmt, item.getTcondition());

        insertResultLog(item.getTowner(), "表级别", item.getMonitorType()
                , warnResultBean.isFlag() ? FinalVar.PASS : FinalVar.NOPASS
                , warnResultBean, item.getDatabaseName(), item.getTableName(), item.getTid(), item.getContent(), item.getPartitionType(),
                warnResultBean.getError(), item.getPriority(), item.getNumber(), item.getCheckDay());

        if (addCheckLog) {
            addRuleCheck(item, FinalVar.RULE_AUTO_THRESHOLD, FinalVar.TABLECOLUMN);
        }

        logger.info("自适应阈值检测执行完成:database:{},tableName:{},itemId:{}", item.getDatabaseName(),
                item.getTableName(), item.getTid());
    }

    /**
     * 数据量自动阈值类型规则执行
     */
    private WarnResultBean adaptiveThresholdRule(TableRuleBean item, String partition, Statement stmt, String where) throws Exception {

        //获取表的count
        logger.info("执行自适应阈值类型规则:id:{}dataBase:{},tableName:{}", item.getNumber(), item.getDatabaseName(), item.getTableName());
        Long countNow = ruleRunService.getCount(item.getDatabaseName(), item.getTableName(), null
                , stmt, partition,
                FinalVar.MONTH.equals(item.getPartitionType()) ? DateFormatSafe.getMonth(item.getCheckDay()) : DateFormatSafe.getDay(item.getCheckDay()),
                where, item.getPartitionType());
        List<ItemModuleListBean> itemModuleListBeanList = itemModuleListBeanMapper.selectProByTableId(item.getTid());
        String name = getProName(item.getTid());
        WarnResultBean warnResultBean = new WarnResultBean();
        warnResultBean.setValue(String.valueOf(countNow));
        warnResultBean.setFlag(true);
        warnResultBean.setProject(name);

        //获取最近历史数据  最大30条
        List<PartitionDataCountBean> historyDataCount = partitionDataCountBeanMapper.getHistoryCount(item.getDatabaseName(), item.getTableName());
        if (historyDataCount.isEmpty()) {
            addHistoryDataCount(item.getDatabaseName(), item.getTableName(), partition, stmt);
            historyDataCount = partitionDataCountBeanMapper.getHistoryCount(item.getDatabaseName(), item.getTableName());

        }

        String partitionArr[] = partition.split("=");

        Boolean isWeekend = DateUtils.isWeekend(partitionArr[1]);
        //存储工作日或周末数据量值，后续进入处理阶段利用
        List<Long> countList = new ArrayList<>();
        for (PartitionDataCountBean historyDataCountItem : historyDataCount) {
            if (historyDataCountItem.getIsWeekend().equalsIgnoreCase(isWeekend.toString())) {
                countList.add(historyDataCountItem.getDataCount());
            }
        }

        //取30天内工作日或休息日数据平均值   去除最大最小值
        Long sum = 0L;
        Long max = 0L;
        Long min = 0L;
        int countNumber = 0;
        for (Long i : countList) {

            if (max == 0L) {
                max = i;
            }
            if (min == 0L) {
                min = i;
            }
            if (i > max) {
                max = i;
            }
            if (i < min) {
                min = i;
            }
            sum = sum + i;
            countNumber++;
        }
        if (countNumber > 2) {
            sum = sum - max - min;
            countNumber = countNumber - 2;
            countList.remove(max);
            countList.remove(min);

        }


        //获取平均值
        Long averageCount = sum / countNumber;
        Long difference = countNow - averageCount;
        Double value = difference.doubleValue() / averageCount.doubleValue();
        String wave = String.format("%.2f", value * 100);


        //获取上下界
        Double upperLimit;
        Double lowerLimit;
        Long upperLimitItem = 0L;
        Long lowerLimitItem = 0L;
        Long differ;
        int upperLimitCount = 0;
        int lowerLimitCount = 0;
        for (Long i : countList) {
            differ = i - averageCount;
            if (differ >= 0) {
                upperLimitItem += differ;
                upperLimitCount++;
            } else {
                lowerLimitItem += differ;
                lowerLimitCount++;
            }
        }
        //定义阈值范围扩展倍数
        Double expansionMultiple = 1.5;
        upperLimit = expansionMultiple * upperLimitItem / (upperLimitCount == 0 ? 1 : upperLimitCount);
        lowerLimit = expansionMultiple * lowerLimitItem / (lowerLimitCount == 0 ? 1 : lowerLimitCount);

        String lowWave = PatternRule.numberFormat.format(lowerLimit / averageCount * 100);
        String upWave = PatternRule.numberFormat.format(upperLimit / averageCount * 100);

        boolean isNormal = lowerLimit <= difference && difference <= upperLimit;
        if (!isNormal) {
            excutingRuleService.warning(item.getAlarmUniqueId(), item.getDatabaseName(), item.getTableName(), item.getContent()
                    , null, "自动阈值", TypeConvert.LongConvertString(countNow)
                    , TypeConvert.LongConvertString(averageCount), "(" + lowWave + "%" + "," + upWave + "%)", item.getNumber(), name, wave + "%", item.getCheckDay(), item.getPartitionType(), item.getTowner(), item.getPriority());


            warnResultBean.setFlag(false);
            switch (item.getPartitionType()) {
                case FinalVar.MONTH:
                    warnResultBean.setError("月数据量自动阈值");
                    break;
                case FinalVar.DAY:
                    warnResultBean.setError("数据量自动阈值");
                    break;
                default:
                    warnResultBean.setError("数据量自动阈值");
            }
            warnResultBean.setCompareValue(TypeConvert.LongConvertString(averageCount));
            warnResultBean.setScope("(" + lowWave + "%" + "," + upWave + "%)");


        }


        //添加入数据库
        addPartitionDataCount(item.getDatabaseName(), item.getTableName(), partitionArr[1], countNow, isWeekend.toString());

        return warnResultBean;
    }

    /**
     * 添加历史数据
     * 已排除当日分区
     * 查询30条
     */
    private void addHistoryDataCount(String databaseName, String tableName, String partition, Statement stmt) throws Exception {
        List<String> partitionList = ruleRunService.getPartitionList(databaseName, tableName, stmt);
        Collections.sort(partitionList);
        Collections.reverse(partitionList);
        int num = 0;
        for (String partitionItem : partitionList) {

            if (partitionItem.equals(partition)) {
                continue;
            }
            Long dataCount = ruleRunService.getCount(databaseName, tableName, null, stmt, partitionItem);
            String partitionArr[] = partitionItem.split("=");
            Boolean isWeekend = DateUtils.isWeekend(partitionArr[1]);
            addPartitionDataCount(databaseName, tableName, partitionArr[1], dataCount, isWeekend.toString());
            num++;
            if (num > 30) {
                break;
            }
        }
    }


    private Long compareResult(TableRuleBean item, Map<String, String> dataMap, Statement stmt
            , String partitions, int dayNum, String where) throws Exception {
        //获取当天day的count
        Long dayCount = ruleRunService.getCount(item.getDatabaseName(), item.getTableName()
                , null, stmt, partitions,
                FinalVar.MONTH.equals(item.getPartitionType()) ? DateFormatSafe.getMonth(dayNum) : DateFormatSafe.getDay(dayNum),
                where, item.getPartitionType());
        return dayCount;
    }


    private MultiValueMap compareDimensionGroupByResult(TableRuleBean item, Statement stmt
            , String partitions, int dayNum, String where, List<String> columnList, String columnName) throws Exception {
        //获取当天day的count
        MultiValueMap dimensionListT = ruleRunService.getDimensionDataGroupBy(item.getDatabaseName(), item.getTableName(), columnName
                , stmt, partitions,
                FinalVar.MONTH.equals(item.getPartitionType()) ? DateFormatSafe.getMonth(dayNum) : DateFormatSafe.getDay(dayNum),
                where, item.getPartitionType(), columnList);
        return dimensionListT;
    }

    private MultiValueMap compareDimensionResult(TableRuleBean item, Statement stmt
            , String partitions, int dayNum, String where, List<String> columnList, String columnName) throws Exception {
        //获取当天day的count
        MultiValueMap dimensionListT = ruleRunService.getDimensionData(item.getDatabaseName(), item.getTableName(), columnName
                , stmt, partitions,
                FinalVar.MONTH.equals(item.getPartitionType()) ? DateFormatSafe.getMonth(dayNum) : DateFormatSafe.getDay(dayNum),
                where, item.getPartitionType(), columnList);
        return dimensionListT;
    }

    private String getDifferentValue(Set<String> dimensionSet, Set<String> dimensionSetH, String compareValue) {
        if (dimensionSet == null && dimensionSetH == null) {
            return null;
        }

        if (dimensionSet == null || dimensionSet.size() == 0) {
            return "今天没有数据";
        }
        if (dimensionSetH == null || dimensionSetH.size() == 0) {
            return compareValue + "当天没有数据";
        }

        Iterator<String> columnIterator = dimensionSet.iterator();
        Iterator<String> columnIteratorH = dimensionSetH.iterator();

        while (columnIteratorH.hasNext()) {
            String column = columnIteratorH.next();
            if (!dimensionSet.contains(column)) {
                return "今天天该维度没有数据:" + column;
            }
        }

        while (columnIterator.hasNext()) {
            String column = columnIterator.next();
            if (!dimensionSetH.contains(column)) {
                return compareValue + "当天该维度没有数据:" + column;

            }
        }
        return null;
    }

    private WarnResultBean getCompareResult(int value, int valueCompare, TableRuleBean item, String compareType, String compare) {
        double devition = value - valueCompare;
        Double percent = devition / (valueCompare == 0 ? 1 : valueCompare);
        String wave = String.format("%.2f", percent * 100);
        logger.info("维度{}:id:{},value:{},valueCompare:{},scope:{},wave:{},dataBase:{},tableName:{}", compareType,
                item.getNumber(), value, valueCompare, compare, wave, item.getDatabaseName(), item.getTableName());
        WarnResultBean warnResultBean = new WarnResultBean();
        warnResultBean.setFlag(true);
        if (!PatternRule.isRule(compare, wave)) {

            warnResultBean.setFlag(false);
            switch (item.getPartitionType()) {
                case FinalVar.MONTH:
                    warnResultBean.setError(compareType + "(月)");
                    break;
                case FinalVar.DAY:
                    warnResultBean.setError(compareType + "(周)");
                    break;
                default:
                    warnResultBean.setError(compareType + "（周)");
            }
            warnResultBean.setWave(wave + "%");
        }
        return warnResultBean;
    }

    private void insertResultLog(String leader, String level, String type, String status
            , WarnResultBean bean, String dataBaseName, String tableName, String ruleId, String content, String partitionType,
                                 String error, String priority, String number, Integer checkDay) {
        RuleRunningLogBean ruleRunningLogBean = new RuleRunningLogBean();
        ruleRunningLogBean.setCreateTime(new Date());
        ruleRunningLogBean.setLeader(leader);
        ruleRunningLogBean.setType(type);
        ruleRunningLogBean.setLevelType(level);
        ruleRunningLogBean.setStatus(status);
        ruleRunningLogBean.setScope(bean.getScope());
        ruleRunningLogBean.setValue(bean.getValue());
        ruleRunningLogBean.setValueCompare(bean.getCompareValue());
        ruleRunningLogBean.setDatabaseName(dataBaseName);
        ruleRunningLogBean.setTableName(tableName);
        ruleRunningLogBean.setColumnName(FinalVar.TABLECOLUMN);
        ruleRunningLogBean.setIsWarnning(bean.getError());
        ruleRunningLogBean.setRuleId(ruleId);
        ruleRunningLogBean.setProject(bean.getProject());
        ruleRunningLogBean.setContent(content);
        ruleRunningLogBean.setPartitionType(partitionType);
        ruleRunningLogBean.setError(error);
        ruleRunningLogBean.setPriority(priority);
        ruleRunningLogBean.setNumber(number);
        ruleRunningLogBean.setCheckday(checkDay);
        ruleRunningLogBeanMapper.insert(ruleRunningLogBean);
    }

    private void insertDimesionResultLog(String leader, String level, String type, String status
            , String dataBaseName, String tableName, String ruleId, String content, String partitionType,
                                         String priority, String number, Integer checkDay, String proName) {
        RuleRunningLogBean ruleRunningLogBean = new RuleRunningLogBean();
        ruleRunningLogBean.setCreateTime(new Date());
        ruleRunningLogBean.setLeader(leader);
        ruleRunningLogBean.setType(type);
        ruleRunningLogBean.setLevelType(level);
        ruleRunningLogBean.setStatus(status);
        ruleRunningLogBean.setDatabaseName(dataBaseName);
        ruleRunningLogBean.setTableName(tableName);
        ruleRunningLogBean.setColumnName(FinalVar.TABLECOLUMN);
        ruleRunningLogBean.setRuleId(ruleId);
        ruleRunningLogBean.setProject(proName);
        ruleRunningLogBean.setContent(content);
        ruleRunningLogBean.setPartitionType(partitionType);
        ruleRunningLogBean.setError("维度结果请查看维度结果表");
        ruleRunningLogBean.setPriority(priority);
        ruleRunningLogBean.setNumber(number);
        ruleRunningLogBean.setCheckday(checkDay);
        ruleRunningLogBeanMapper.insert(ruleRunningLogBean);
    }

    private void getCountByTime() {
        DateFormatSafe.getDay(7);
    }


    private void insertDayDateCount(String dateBase, String tableName, Integer count, Integer checkDay, String condition) {
        if (!StringUtils.isEmpty(condition)) {
            return;
        }
        List<DayDataBean> list = dayDataBeanMapper.selectDayDataToday(dateBase, tableName, "YC_TABLE", checkDay);
        DayDataBean dayDataBean;
        if (list == null || list.isEmpty()) {
            dayDataBean = new DayDataBean();
            dayDataBean.setAllCount(count);
            dayDataBean.setDatabaseName(dateBase);
            dayDataBean.setTableName(tableName);
            dayDataBean.setCreateTime(DateFormatSafe.getDay(checkDay));
            dayDataBean.setColumnName("YC_TABLE");
            dayDataBeanMapper.insert(dayDataBean);
        } else {
            dayDataBean = list.get(0);
            dayDataBean.setAllCount(count);
            dayDataBeanMapper.updateByPrimaryKey(dayDataBean);
        }
    }

    /*
    private boolean isCheck(TableRuleBean item,String type, String columnName){
        List<RuleCheckBean>  logBeanList = ruleCheckBeanMapper.getRuleCheckByRuleId(item.getTid(),item.getCheckDay());
        if(logBeanList!=null&&!logBeanList.isEmpty()){
            logger.info("database:{},tableName:{}-{}已经检查-break",item.getDatabaseName()
                    ,item.getTableName(),type);
            return true;
        }else{
            RuleCheckBean  ruleCheckBean = new RuleCheckBean();
            ruleCheckBean.setDatabaseName(item.getDatabaseName());
            ruleCheckBean.setTableName(item.getTableName());
            ruleCheckBean.setColumnName(columnName);
            ruleCheckBean.setType(type);
            ruleCheckBean.setCreateTime(DateFormatSafe.getDay(item.getCheckDay()));
            ruleCheckBean.setRuleId(item.getTid());
            ruleCheckBeanMapper.insert(ruleCheckBean);
        }
        return  false;
    }*/

    private void addRuleCheck(TableRuleBean item, String type, String columnName) {
        RuleCheckBean ruleCheckBean = new RuleCheckBean();
        ruleCheckBean.setDatabaseName(item.getDatabaseName());
        ruleCheckBean.setTableName(item.getTableName());
        ruleCheckBean.setColumnName(columnName);
        ruleCheckBean.setType(type);
        ruleCheckBean.setCreateTime(DateFormatSafe.getDay(item.getCheckDay()));
        ruleCheckBean.setRuleId(item.getTid());
        ruleCheckBeanMapper.insert(ruleCheckBean);
    }

    /**
     * 当日数据量添加入数据库
     *
     * @param
     * @param partition
     * @param dataCount
     */
    private void addPartitionDataCount(String databaseName, String tableName, String partition, Long dataCount, String isWeekend) {


        //            如果存在记录则先删除
        List<PartitionDataCountBean> historyDataItem = partitionDataCountBeanMapper.getHistoryCountByPartition(databaseName, tableName, partition);
        if (!historyDataItem.isEmpty()) {
            for (PartitionDataCountBean item : historyDataItem) {
                partitionDataCountBeanMapper.deleteById(item.getId());
            }
        }

        PartitionDataCountBean partitionDataCountBean = new PartitionDataCountBean();
        partitionDataCountBean.setDatabaseName(databaseName);
        partitionDataCountBean.setPartition(partition);
        partitionDataCountBean.setTableName(tableName);
        partitionDataCountBean.setDataCount(dataCount);
        partitionDataCountBean.setIsWeekend(isWeekend);
        partitionDataCountBeanMapper.insert(partitionDataCountBean);
    }

    private boolean ruleAlreadyRuned(TableRuleBean item) {
        Date now = new Date();
        String checkDay = null;
        if ("0".equals(item.getCheckDay())) {
            checkDay = DateUtils.format(now);
        } else {
            checkDay = DateUtils.getAroundDate(now, ~(Integer.valueOf(item.getCheckDay())) + 1, DateUtils.DATE_PATTERN);
        }

        List<RuleCheckBean> list = ruleCheckBeanMapper.getHistoryByRuleidAndDate(item.getTid(), checkDay);
        if (list.isEmpty()) {
            return false;
        }
        return true;
    }

    /**
     * 查看25分钟轮询未就绪的完成时间规则 是否完成
     *
     * @param item
     * @param type
     * @param columnName
     * @return
     */
    private boolean isCheckTimeRule(TableRuleBean item, String type, String columnName) {
        List<RuleCheckBean> logBeanList = ruleCheckBeanMapper.getRuleCheckByRuleId(item.getTid(), item.getCheckDay());
        if (logBeanList != null && !logBeanList.isEmpty()) {
            logger.info("database:{},tableName:{}-{}已经检查-break", item.getDatabaseName()
                    , item.getTableName(), type);
            return true;
        }
        return false;
    }

    /**
     * 获取规则关联的模块名称
     *
     * @param tableId
     * @return
     */
    private String getProName(String tableId) {
        String name = null;
        List<ItemModuleListBean> itemModuleListBeanList = itemModuleListBeanMapper.selectProByTableId(tableId);
        if (itemModuleListBeanList != null && itemModuleListBeanList.size() > 0) {
            StringBuilder builder = new StringBuilder();
            itemModuleListBeanList.forEach(moduleItem -> {
                builder.append(moduleItem.getName());
                builder.append("  ");
            });
            name = builder.toString();
        }
        return name;
    }


    public void setTableSon(List<String> lists, String time, String fatherId) {
        lists.forEach(item -> {
            String[] dataTable = item.split("\\.");
            String dateBase = dataTable[0];
            String tableName = dataTable[1];
            List<TableRuleBean> tableRuleBeans = tableRuleBeanMapper.getTableRuleByName(dateBase, tableName, FinalVar.RULE_END_TIME, time);
            if (tableRuleBeans == null || tableRuleBeans.isEmpty()) {
                return;
            } else {
                RuleNotReadyBean ruleNotReadyBean = new RuleNotReadyBean();
                ruleNotReadyBean.setDatabaseName(dateBase);
                ruleNotReadyBean.setTableName(tableName);
                ruleNotReadyBean.setRuleId(tableRuleBeans.get(0).getTid());
                ruleNotReadyBean.setRuleTime(tableRuleBeans.get(0).getSelf());
                ruleNotReadyBean.setStatus(0);
                ruleNotReadyBean.setCreateTime(new Date());
                ruleNotReadyBean.setFaterId(fatherId);
                ruleNotReadyBeanMapper.insert(ruleNotReadyBean);
            }
        });
    }

    public boolean isTableSon(String ruleId) {

        List<RuleNotReadyBean> list = ruleNotReadyBeanMapper.getByRuleId(ruleId, 0);
        if (list != null && !list.isEmpty()) {
            logger.info("id:{}未就绪子表存在status:0", ruleId);
            return true;
        }
        return false;
    }

    public void updateTableSonStatus(String fatherId) {
        ruleNotReadyBeanMapper.updateStatusByFatherId(fatherId, 1);
    }

    private void sendDimensionWarning(TableRuleBean tableRuleBean, String proName) {
        List<DimensionResultBean> dimensionResultBeanList = dimensionResultBeanMapper.queryTodayResultByTableId(tableRuleBean.getTid());
        if (dimensionResultBeanList == null || dimensionResultBeanList.isEmpty()) {
            return;
        }
        StringBuilder errorBuilder = new StringBuilder();
        int num = 0;
        for (DimensionResultBean dimensionResultBean : dimensionResultBeanList) {
            String value = dimensionResultBean.getValue();
            String valueCompare = dimensionResultBean.getValueCompare();
            String percent = dimensionResultBean.getPercent();
            String scope = dimensionResultBean.getWave();
            String type = dimensionResultBean.getError();
            String dimension = dimensionResultBean.getDimension();
            String colunmName = dimensionResultBean.getColumnName();
            String errorMsg = String.format("<br>%s: 维度:%s,对比字段:%s,监控类型:%s,波动范围:%s,正常范围:%s,实际值:%s,对比值:%s",
                    ++num, dimension, colunmName, type, percent, scope, value, valueCompare);
            errorBuilder.append(errorMsg);

        }
        excutingRuleService.dimensionWarning(tableRuleBean.getAlarmUniqueId(), tableRuleBean.getDatabaseName(), tableRuleBean.getTableName(), tableRuleBean.getContent()
                , tableRuleBean.getColumnName(), tableRuleBean.getMonitorType(), tableRuleBean.getNumber(), proName, tableRuleBean.getCheckDay(), tableRuleBean.getPartitionType(), tableRuleBean.getTowner(), tableRuleBean.getPriority(), errorBuilder.toString());

    }
}
