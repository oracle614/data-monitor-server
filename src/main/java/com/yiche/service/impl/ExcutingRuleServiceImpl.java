package com.yiche.service.impl;

import com.yiche.bean.*;
import com.yiche.dao.*;
import com.yiche.service.ColumnRuleService;
import com.yiche.service.ExcutingRuleService;
import com.yiche.service.NotifySysService;
import com.yiche.service.TableRuleService;
import com.yiche.utils.*;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

@Service
public class ExcutingRuleServiceImpl implements ExcutingRuleService {

    private final Logger logger = LoggerFactory.getLogger(ExcutingRuleServiceImpl.class);

    private static ResultSet res;

    @Value("${alarmindex.id}")
    private String indexAlarmId;
    @Value("${alarmdatahouse.id}")
    private String dataHouseAlarmId;
    @Value("${alarmchannel.id}")
    private String channelAlarmId;
    @Value("${alarmdistributor.id}")
    private String distributorAlarmId;
    @Value("${alarmexception.id}")
    private String exceptionAlarmId;
    @Value("${globalreport.id}")
    private String globalReportAlarmId;
    @Value("${yipaireport.id}")
    private String yipaiReportAlarmId;

    @Value("${hdfs.url}")
    private String hdfsUrl;

    @Autowired
    ColumnRuleService columnRuleService;

    @Autowired
    TableRuleService tableRuleService;

    @Autowired
    RuleRunningLogBeanMapper ruleRunningLogBeanMapper;

    @Autowired
    RuleCheckBeanMapper ruleCheckBeanMapper;

    @Autowired
    NotifySysService notifySysService;

    @Autowired
    ItemCheckBeanMapper itemCheckBeanMapper;

    @Autowired
    TableRuleBeanMapper tableRuleBeanMapper;
    @Autowired
    ColumnRuleBeanMapper columnRuleBeanMapper;

    @Autowired
    ItemModuleListBeanMapper itemModuleListBeanMapper;

    @Autowired
    private AlarmHistoryDao alarmHistoryDao;

    @Override
    public void runTableRule() {
        tableRuleService.tableRuleRun();
    }

    @Override
    public void runColumnRule() {
        columnRuleService.columnRuleRun();
    }

    @Override
    public List<RuleRunningLogBean> getDataByDataBaseAndTable(String dataBaseName, String tableName, Integer index, Integer limit) {
        return ruleRunningLogBeanMapper.getDataByDataBaseAndTable(dataBaseName, tableName, index, limit);
    }

    @Override
    public Integer getResultLogAllCount() {
        return ruleRunningLogBeanMapper.getResultLogAllCount();
    }

    @Override
    public Integer getResultLogCount(String dataBaseName, String tableName) {
        return ruleRunningLogBeanMapper.getResultLogCount(dataBaseName, tableName);
    }

    @Override
    public Integer getResultLogStatusCount(String dataBaseName, String tableName, String status) {
        return ruleRunningLogBeanMapper.getResultLogStatusCount(dataBaseName, tableName, status);
    }


    @Override
    public void warning(String alarmUniqueId, String dateBase, String tableName, String content, String column, String error, String value
            , String valueCompare, String scope, String id, String project, String waveScope, Integer checkDay, String partitionType, String user, String priority) {

        logger.info("database:{},tableName:{},alarmUniqueId:{}-规则没通过  报警", dateBase, tableName, alarmUniqueId);


        notifySysService.notifyBuilder(alarmUniqueId, dateBase
                , tableName, content, column, error, value, valueCompare, scope, id, project, waveScope, checkDay, partitionType, user, priority);

        AlarmHistoryEntity alarmHistoryEntity = new AlarmHistoryEntity();
        alarmHistoryEntity.setDatabaseName(dateBase);
        alarmHistoryEntity.setTableName(tableName);
        alarmHistoryEntity.setRuleId(id);
        alarmHistoryDao.save(alarmHistoryEntity);
    }

    @Override
    public void warningWhenExecRuleException(String subject, String body) {
        String alarmUniqueId = exceptionAlarmId;
        logger.info("send alarm subject:{}, body:{}, alarmUniqueId:{}", subject, body, alarmUniqueId);
        notifySysService.notifyBuilder(alarmUniqueId, subject, body);

    }


    /*
    判断期望分区是否存在
     */
    @Override
    public boolean isPartitionReady(String expectedPartition) {
        return !expectedPartition.isEmpty();
    }


    public boolean successFileIsExist(String database, String table, Integer checkDay) {
        logger.info("判断success文件生成database:{},tableName:{}", database, table);
        String path = String.format("/bitauto/sign/%s/%s/%s/_SUCCESS", database, table
                , DateFormatSafe.formatSign(DateFormatSafe.getDay(checkDay)));
        logger.info("success文件path:{}", path);
        HttpFSFileSystem hdfs = new HttpFSFileSystem();
        URI uri;
        Long time = null;
        try {
            uri = new URI(hdfsUrl + path);
            hdfs.initialize(uri, "luozhenyu");
            FileStatus fileStatus = hdfs.getFileStatus(uri);
            return true;
        } catch (URISyntaxException e) {
            logger.error("链接hdfs解析失败", e);
            return false;
        } catch (IOException e) {
            logger.info("文件不存在", e);
            logger.info(" succes文件没有生成  table:{}", database + "." + table);
            return false;
        }
    }

    /*
    指数项目
     */
    @Override
    public void runIndexPro(String partitionType) {
        Integer id = 1;
        List<TableRuleBean> getReadyItemAll = tableRuleBeanMapper.getReadyItemAll(id, id, partitionType);
        List<RuleRunningLogBean> getReadyItemAllFinishPass = ruleRunningLogBeanMapper.getReadyItemAllFinish(id, id, 0, "通过", partitionType);
        List<RuleRunningLogBean> getReadyItemAllFinishNoPass = ruleRunningLogBeanMapper.getReadyItemAllFinish(id, id, 0, "不通过", partitionType);
        List<TableRuleBean> getReadyItemAllDnf = tableRuleBeanMapper.getReadyItemAllDnf(id, id, 0, partitionType);
        List<RuleRunningLogBean> getTimeItemNotReady = ruleRunningLogBeanMapper.getTimeRuleNotReady(id, id, 0, "未就绪", partitionType);

        List<TableRuleBean> getItemAllDnr = tableRuleBeanMapper.getItemAllDnr(id, id, partitionType);
        List<TableRuleBean> getItemAllDnrDnf = tableRuleBeanMapper.getItemAllDnrDnf(id, id, 0, partitionType);
        List<RuleRunningLogBean> getTableItemDnRFinishPass = ruleRunningLogBeanMapper.getTableItemDnRFinish(id, id, 0, "通过", partitionType);
        List<RuleRunningLogBean> getTableItemDnRFinishNoPass = ruleRunningLogBeanMapper.getTableItemDnRFinish(id, id, 0, "不通过", partitionType);

        List<ColumnRuleBean> getColumnItemAll = columnRuleBeanMapper.getColumnItemAll(id, id, partitionType);
        List<ColumnRuleBean> getColumnItemDnf = columnRuleBeanMapper.getColumnItemDnf(id, id, 0, partitionType);
        List<RuleRunningLogBean> getColumnItemFinishPass = ruleRunningLogBeanMapper.getColumnItemFinish(id, id, 0, "通过", partitionType);
        List<RuleRunningLogBean> getColumnItemFinishNoPass = ruleRunningLogBeanMapper.getColumnItemFinish(id, id, 0, "不通过", partitionType);


        int readyItemCount = getReadyItemAll.size(); //就绪总数
        int readyItemCountFinishPass = getReadyItemAllFinishPass.size(); //就绪通过
        int readyItemCountFinishNoPass = getReadyItemAllFinishNoPass.size();  //就绪未通过
        int readyItemCountDnf = getReadyItemAllDnf.size();  //就绪未执行


        int tableItemCount = getItemAllDnr.size();//table总数
        int tableItemCountDnfPass = getTableItemDnRFinishPass.size();    //table执行 通过
        int tableItemCountDnfNoPass = getTableItemDnRFinishNoPass.size();//table执行 未通过
        int tableItemCountDnf = getItemAllDnrDnf.size();//table未执行


        int columnItemCount = getColumnItemAll.size();  //column总数
        int columnItemDnfPass = getColumnItemFinishPass.size(); //column执行 通过
        int columnItemDnfNoPass = getColumnItemFinishNoPass.size();  //column执行 未通过
        int columnItemDnf = getColumnItemDnf.size();  //column未执行

        int countAll = readyItemCount + tableItemCount + columnItemCount;
        double passCount = readyItemCountFinishPass + tableItemCountDnfPass + columnItemDnfPass;
        double exceptionDevition = passCount / countAll;
        Double value = exceptionDevition * 100;
        int countDnrAll = tableItemCount + columnItemCount;
        int countDnrPass = tableItemCountDnfPass + columnItemDnfPass;
        int countDnrNoPass = tableItemCountDnfNoPass + columnItemDnfNoPass;
        int countDnrDnf = tableItemCountDnf + columnItemDnf;

        String project = FinalVar.INDEX;
        String passDevition = PatternRule.numberFormat.format(Math.floor(value)) + "%";
        String readyRule = String.format("任务数:%s,  准时就绪:%s,  未准时就绪:%s,  未执行:%s ", readyItemCount,
                readyItemCountFinishPass, readyItemCountFinishNoPass, readyItemCountDnf);
        String dnrRule = String.format("任务数:%s,  通过:%s,  未通过:%s,  监控任务异常:%s ", countDnrAll,
                countDnrPass, countDnrNoPass, countDnrDnf);
        //未就绪明细
        StringBuilder noReadyDetailBuilder = new StringBuilder();
        for (int i = 0; i < getTimeItemNotReady.size(); i++) {
            String ruleId = "null";
            if (getTimeItemNotReady.get(i).getColumnName().equals(FinalVar.TABLECOLUMN)) {
                ruleId = tableRuleBeanMapper.getTableRuleById(getTimeItemNotReady.get(i).getRuleId());
            } else {
                ruleId = columnRuleBeanMapper.getColumnRuleById(getTimeItemNotReady.get(i).getRuleId());
            }
            String dataContent = String.format("<br>%s. 规则编号:%s;  监控内容:%s;  相关库表:%s.%s;  期望时间:%s;", i + 1, ruleId, getTimeItemNotReady.get(i).getContent(),
                    getTimeItemNotReady.get(i).getDatabaseName(), getTimeItemNotReady.get(i).getTableName(), getTimeItemNotReady.get(i).getValueCompare());
            noReadyDetailBuilder.append(dataContent);
        }
        getColumnItemFinishNoPass.addAll(getTableItemDnRFinishNoPass);
        StringBuilder noPassDetailBuilder = new StringBuilder();
        for (int i = 0; i < getColumnItemFinishNoPass.size(); i++) {
            int compareValue = TypeConvert.StringConvertInteger(getColumnItemFinishNoPass.get(i).getValueCompare());
            int num = TypeConvert.StringConvertInteger(getColumnItemFinishNoPass.get(i).getValue());
            double devition = num - compareValue;
            Double waveNum = devition / (compareValue == 0 ? 1 : compareValue);
            String wave = String.format("%.2f", waveNum * 100) + "%";
            String ruleId = "null";
            if (getColumnItemFinishNoPass.get(i).getColumnName().equals(FinalVar.TABLECOLUMN)) {
                ruleId = tableRuleBeanMapper.getTableRuleById(getColumnItemFinishNoPass.get(i).getRuleId());
            } else {
                ruleId = columnRuleBeanMapper.getColumnRuleById(getColumnItemFinishNoPass.get(i).getRuleId());
            }
            String dataContent = String.format("<br>%s. 规则编号:%s;  监控内容:%s;  监控项:%s;  波动范围:%s;  影响范围:%s;", i + 1, ruleId, getColumnItemFinishNoPass.get(i).getContent(),
                    getColumnItemFinishNoPass.get(i).getIsWarnning(), wave.replace(",", "") + getColumnItemFinishNoPass.get(i).getScope(),
                    getColumnItemFinishNoPass.get(i).getProject());
            noPassDetailBuilder.append(dataContent);
        }
        List<String> dnFinishList = new ArrayList<>();
        getItemAllDnrDnf.forEach(tableDnf -> {
            List<ItemModuleListBean> itemModuleListBeanList = itemModuleListBeanMapper.selectModuleByTableId(tableDnf.getTid());
            String name = getProName(itemModuleListBeanList);
            dnFinishList.add(name);
        });
        getColumnItemDnf.forEach(tableDnf -> {
            List<ItemModuleListBean> itemModuleListBeanList = itemModuleListBeanMapper.selectModuleByColumnId(tableDnf.getId());
            String name = getProName(itemModuleListBeanList);
            dnFinishList.add(name);
        });
        StringBuilder dateDnFinishBuilder = new StringBuilder();
        for (int i = 0; i < dnFinishList.size(); i++) {
            String dateDnFinishStr = String.format("<br>%s.影响范围:%s", i + 1, dnFinishList.get(i));
            dateDnFinishBuilder.append(dateDnFinishStr);
        }
        String noReadyDetail = noReadyDetailBuilder.toString();
        String noPassDetail = noPassDetailBuilder.toString();
        IndexProMail indexProMail = new IndexProMail();
        indexProMail.setProject(project);
        indexProMail.setDnrRule(dnrRule);
        indexProMail.setNoReadyDetail(StringUtils.isEmpty(noReadyDetail) ? FinalVar.NOTHING : noReadyDetail);
        indexProMail.setNoPassDetail(StringUtils.isEmpty(noPassDetail) ? FinalVar.NOTHING : noPassDetail);
        indexProMail.setReadyRule(readyRule);
        indexProMail.setPassDevition(passDevition);
        indexProMail.setDataDnfDetail(StringUtils.isEmpty(dateDnFinishBuilder.toString()) ? FinalVar.NOTHING : dateDnFinishBuilder.toString());
        List<IndexProMail> mailList = new ArrayList<>();
        mailList.add(indexProMail);

        String alarmUniqueId = indexAlarmId;
        notifySysService.notifyBuilder(project, alarmUniqueId, mailList);
    }

    /*
    数仓项目
     */

    @Override
    public void runDataWarehourse(String partitionType) {
        Integer id = 40;
        List<TableRuleBean> getReadyItemAll = tableRuleBeanMapper.getReadyItemAll(id, id, partitionType);
        List<RuleRunningLogBean> getReadyItemAllFinishPass = ruleRunningLogBeanMapper.getReadyItemAllFinish(id, id, 0, "通过", partitionType);
        List<RuleRunningLogBean> getReadyItemAllFinishNoPass = ruleRunningLogBeanMapper.getReadyItemAllFinish(id, id, 0, "不通过", partitionType);
        List<TableRuleBean> getReadyItemAllDnf = tableRuleBeanMapper.getReadyItemAllDnf(id, id, 0, partitionType);
        List<RuleRunningLogBean> getTimeItemNotReady = ruleRunningLogBeanMapper.getTimeRuleNotReady(id, id, 0, "未就绪", partitionType);

        List<TableRuleBean> getItemAllDnr = tableRuleBeanMapper.getItemAllDnr(id, id, partitionType);
        List<TableRuleBean> getItemAllDnrDnf = tableRuleBeanMapper.getItemAllDnrDnf(id, id, 0, partitionType);
        List<RuleRunningLogBean> getTableItemDnRFinishPass = ruleRunningLogBeanMapper.getTableItemDnRFinish(id, id, 0, "通过", partitionType);
        List<RuleRunningLogBean> getTableItemDnRFinishNoPass = ruleRunningLogBeanMapper.getTableItemDnRFinish(id, id, 0, "不通过", partitionType);

        List<ColumnRuleBean> getColumnItemAll = columnRuleBeanMapper.getColumnItemAll(id, id, partitionType);
        List<ColumnRuleBean> getColumnItemDnf = columnRuleBeanMapper.getColumnItemDnf(id, id, 0, partitionType);
        List<RuleRunningLogBean> getColumnItemFinishPass = ruleRunningLogBeanMapper.getColumnItemFinish(id, id, 0, "通过", partitionType);
        List<RuleRunningLogBean> getColumnItemFinishNoPass = ruleRunningLogBeanMapper.getColumnItemFinish(id, id, 0, "不通过", partitionType);

        int readyItemCount = getReadyItemAll.size();   //就绪总数
        int readyItemCountFinishPass = getReadyItemAllFinishPass.size(); //就绪通过
        int readyItemCountFinishNoPass = getReadyItemAllFinishNoPass.size(); //就绪未通过
        int readyItemCountDnf = getReadyItemAllDnf.size();//就绪未执行

        int tableItemCount = getItemAllDnr.size(); //table总数
        int tableItemCountDnfPass = getTableItemDnRFinishPass.size(); //table执行 通过
        int tableItemCountDnfNoPass = getTableItemDnRFinishNoPass.size();  //table执行 未通过
        int tableItemCountDnf = getItemAllDnrDnf.size(); //table未执行

        int columnItemCount = getColumnItemAll.size(); //column总数
        int columnItemDnfPass = getColumnItemFinishPass.size(); //column执行 通过
        int columnItemDnfNoPass = getColumnItemFinishNoPass.size(); //column执行 未通过
        int columnItemDnf = getColumnItemDnf.size();  //column未执行

        int countAll = readyItemCount + tableItemCount + columnItemCount;
        double passCount = readyItemCountFinishPass + tableItemCountDnfPass + columnItemDnfPass;
        double exceptionDevition = passCount / countAll;
        Double value = exceptionDevition * 100;
        int countDnrAll = tableItemCount + columnItemCount;
        int countDnrPass = tableItemCountDnfPass + columnItemDnfPass;
        int countDnrNoPass = tableItemCountDnfNoPass + columnItemDnfNoPass;
        int countDnrDnf = tableItemCountDnf + columnItemDnf;

        String project = FinalVar.DATA_WAREHOUSE;
        String passDevition = PatternRule.numberFormat.format(Math.floor(value)) + "%";
        String readyRule = String.format("任务数:%s,  准时就绪:%s,  未准时就绪:%s,  未执行:%s ", readyItemCount,
                readyItemCountFinishPass, readyItemCountFinishNoPass, readyItemCountDnf);
        String dnrRule = String.format("任务数:%s,  通过:%s,  未通过:%s,  监控任务异常:%s ", countDnrAll,
                countDnrPass, countDnrNoPass, countDnrDnf);

        //未就绪明细
        StringBuilder noReadyDetailBuilder = new StringBuilder();
        for (int i = 0; i < getTimeItemNotReady.size(); i++) {
            String ruleId = "null";
            if (getTimeItemNotReady.get(i).getColumnName().equals(FinalVar.TABLECOLUMN)) {
                ruleId = tableRuleBeanMapper.getTableRuleById(getTimeItemNotReady.get(i).getRuleId());
            } else {
                ruleId = columnRuleBeanMapper.getColumnRuleById(getTimeItemNotReady.get(i).getRuleId());
            }
            String dataContent = String.format("<br>%s. 规则编号:%s;  监控内容:%s;  相关库表:%s.%s;  期望时间:%s;", i + 1, ruleId, getTimeItemNotReady.get(i).getContent(),
                    getTimeItemNotReady.get(i).getDatabaseName(), getTimeItemNotReady.get(i).getTableName(), getTimeItemNotReady.get(i).getValueCompare());
            noReadyDetailBuilder.append(dataContent);
        }
        getColumnItemFinishNoPass.addAll(getTableItemDnRFinishNoPass);
        StringBuilder noPassDetailBuilder = new StringBuilder();
        for (int i = 0; i < getColumnItemFinishNoPass.size(); i++) {
            int compareValue = TypeConvert.StringConvertInteger(getColumnItemFinishNoPass.get(i).getValueCompare());
            int num = TypeConvert.StringConvertInteger(getColumnItemFinishNoPass.get(i).getValue());
            double devition = num - compareValue;
            Double waveNum = devition / (compareValue == 0 ? 1 : compareValue);
            String wave = String.format("%.2f", waveNum * 100) + "%";
            String ruleId = "null";
            if (getColumnItemFinishNoPass.get(i).getColumnName().equals(FinalVar.TABLECOLUMN)) {
                ruleId = tableRuleBeanMapper.getTableRuleById(getColumnItemFinishNoPass.get(i).getRuleId());
            } else {
                ruleId = columnRuleBeanMapper.getColumnRuleById(getColumnItemFinishNoPass.get(i).getRuleId());
            }
            String dataContent = String.format("<br>%s. 规则编号:%s;  监控内容:%s;  监控项:%s;  波动范围:%s; %s.%s;", i + 1, ruleId, getColumnItemFinishNoPass.get(i).getContent(),
                    getColumnItemFinishNoPass.get(i).getIsWarnning(), wave.replace(",", "") + getColumnItemFinishNoPass.get(i).getScope(),
                    getColumnItemFinishNoPass.get(i).getDatabaseName(), getColumnItemFinishNoPass.get(i).getTableName());
            noPassDetailBuilder.append(dataContent);
        }
        List<String> dnFinishList = new ArrayList<>();
        getItemAllDnrDnf.forEach(tableDnf -> {
            String name = String.format("规则编号: %s；相关库表: %s.%s", tableDnf.getNumber(), tableDnf.getDatabaseName(), tableDnf.getTableName());
            dnFinishList.add(name);
        });
        getColumnItemDnf.forEach(tableDnf -> {
            String name = String.format("规则编号: %s；相关库表: %s.%s", tableDnf.getNumber(), tableDnf.getDatabaseName(), tableDnf.getTableName());
            dnFinishList.add(name);
        });
        StringBuilder dateDnFinishBuilder = new StringBuilder();
        for (int i = 0; i < dnFinishList.size(); i++) {
            String dateDnFinishStr = String.format("<br>%s.%s", i + 1, dnFinishList.get(i));
            dateDnFinishBuilder.append(dateDnFinishStr);
        }
        String noReadyDetail = noReadyDetailBuilder.toString();
        String noPassDetail = noPassDetailBuilder.toString();
        IndexProMail indexProMail = new IndexProMail();
        indexProMail.setProject(project);
        indexProMail.setDnrRule(dnrRule);
        indexProMail.setNoPassDetail(StringUtils.isEmpty(noPassDetail) ? FinalVar.NOTHING : noPassDetail);
        indexProMail.setReadyRule(readyRule);
        indexProMail.setPassDevition(passDevition);
        indexProMail.setNoReadyDetail(StringUtils.isEmpty(noReadyDetail) ? FinalVar.NOTHING : noReadyDetail);
        indexProMail.setDataDnfDetail(StringUtils.isEmpty(dateDnFinishBuilder.toString()) ? FinalVar.NOTHING : dateDnFinishBuilder.toString());
        List<IndexProMail> mailList = new ArrayList<>();
        mailList.add(indexProMail);
        String alarmUniqueId = dataHouseAlarmId;
        notifySysService.notifyBuilder(project, alarmUniqueId, mailList);


    }

    /*
    渠道线索
     */
    @Override
    public void runPro(String partitionType) {
        Integer id = 42;
        List<TableRuleBean> getReadyItemAll = tableRuleBeanMapper.getReadyItemAll(id, id, partitionType);
        List<RuleRunningLogBean> getReadyItemAllFinishPass = ruleRunningLogBeanMapper.getReadyItemAllFinish(id, id, 0, "通过", partitionType);
        List<RuleRunningLogBean> getReadyItemAllFinishNoPass = ruleRunningLogBeanMapper.getReadyItemAllFinish(id, id, 0, "不通过", partitionType);
        List<TableRuleBean> getReadyItemAllDnf = tableRuleBeanMapper.getReadyItemAllDnf(id, id, 0, partitionType);
        List<RuleRunningLogBean> getTimeItemNotReady = ruleRunningLogBeanMapper.getTimeRuleNotReady(id, id, 0, "未就绪", partitionType);

        List<TableRuleBean> getItemAllDnr = tableRuleBeanMapper.getItemAllDnr(id, id, partitionType);
        List<TableRuleBean> getItemAllDnrDnf = tableRuleBeanMapper.getItemAllDnrDnf(id, id, 0, partitionType);
        List<RuleRunningLogBean> getTableItemDnRFinishPass = ruleRunningLogBeanMapper.getTableItemDnRFinish(id, id, 0, "通过", partitionType);
        List<RuleRunningLogBean> getTableItemDnRFinishNoPass = ruleRunningLogBeanMapper.getTableItemDnRFinish(id, id, 0, "不通过", partitionType);

        List<ColumnRuleBean> getColumnItemAll = columnRuleBeanMapper.getColumnItemAll(id, id, partitionType);
        List<ColumnRuleBean> getColumnItemDnf = columnRuleBeanMapper.getColumnItemDnf(id, id, 0, partitionType);
        List<RuleRunningLogBean> getColumnItemFinishPass = ruleRunningLogBeanMapper.getColumnItemFinish(id, id, 0, "通过", partitionType);
        List<RuleRunningLogBean> getColumnItemFinishNoPass = ruleRunningLogBeanMapper.getColumnItemFinish(id, id, 0, "不通过", partitionType);

        int readyItemCount = getReadyItemAll.size();   //就绪总数
        int readyItemCountFinishPass = getReadyItemAllFinishPass.size(); //就绪通过
        int readyItemCountFinishNoPass = getReadyItemAllFinishNoPass.size(); //就绪未通过
        int readyItemCountDnf = getReadyItemAllDnf.size();//就绪未执行

        int tableItemCount = getItemAllDnr.size(); //table总数
        int tableItemCountDnfPass = getTableItemDnRFinishPass.size(); //table执行 通过
        int tableItemCountDnfNoPass = getTableItemDnRFinishNoPass.size();  //table执行 未通过
        int tableItemCountDnf = getItemAllDnrDnf.size(); //table未执行

        int columnItemCount = getColumnItemAll.size(); //column总数
        int columnItemDnfPass = getColumnItemFinishPass.size(); //column执行 通过
        int columnItemDnfNoPass = getColumnItemFinishNoPass.size(); //column执行 未通过
        int columnItemDnf = getColumnItemDnf.size();  //column未执行


        int countAll = readyItemCount + tableItemCount + columnItemCount;
        double passCount = readyItemCountFinishPass + tableItemCountDnfPass + columnItemDnfPass;
        double exceptionDevition = passCount / countAll;
        Double value = exceptionDevition * 100;
        int countDnrAll = tableItemCount + columnItemCount;
        int countDnrPass = tableItemCountDnfPass + columnItemDnfPass;
        int countDnrNoPass = tableItemCountDnfNoPass + columnItemDnfNoPass;
        int countDnrDnf = tableItemCountDnf + columnItemDnf;

        String project = FinalVar.PLATFORM_INDEX;
        String passDevition = PatternRule.numberFormat.format(Math.floor(value)) + "%";
        String readyRule = String.format("任务数:%s,  准时就绪:%s,  未准时就绪:%s,  未执行:%s ", readyItemCount,
                readyItemCountFinishPass, readyItemCountFinishNoPass, readyItemCountDnf);
        String dnrRule = String.format("任务数:%s,  通过:%s,  未通过:%s,  监控任务异常:%s ", countDnrAll,
                countDnrPass, countDnrNoPass, countDnrDnf);

        //未就绪明细
        StringBuilder noReadyDetailBuilder = new StringBuilder();
        for (int i = 0; i < getTimeItemNotReady.size(); i++) {
            String ruleId = "null";
            if (getTimeItemNotReady.get(i).getColumnName().equals(FinalVar.TABLECOLUMN)) {
                ruleId = tableRuleBeanMapper.getTableRuleById(getTimeItemNotReady.get(i).getRuleId());
            } else {
                ruleId = columnRuleBeanMapper.getColumnRuleById(getTimeItemNotReady.get(i).getRuleId());
            }
            String dataContent = String.format("<br>%s. 规则编号:%s;  监控内容:%s;  相关库表:%s.%s;  期望时间:%s;", i + 1, ruleId, getTimeItemNotReady.get(i).getContent(),
                    getTimeItemNotReady.get(i).getDatabaseName(), getTimeItemNotReady.get(i).getTableName(), getTimeItemNotReady.get(i).getValueCompare());
            noReadyDetailBuilder.append(dataContent);
        }
        getColumnItemFinishNoPass.addAll(getTableItemDnRFinishNoPass);
        StringBuilder noPassDetailBuilder = new StringBuilder();
        for (int i = 0; i < getColumnItemFinishNoPass.size(); i++) {
            int compareValue = TypeConvert.StringConvertInteger(getColumnItemFinishNoPass.get(i).getValueCompare());
            int num = TypeConvert.StringConvertInteger(getColumnItemFinishNoPass.get(i).getValue());
            double devition = num - compareValue;
            Double waveNum = devition / (compareValue == 0 ? 1 : compareValue);
            String wave = String.format("%.2f", waveNum * 100) + "%";
            String ruleId = "null";
            if (getColumnItemFinishNoPass.get(i).getColumnName().equals(FinalVar.TABLECOLUMN)) {
                ruleId = tableRuleBeanMapper.getTableRuleById(getColumnItemFinishNoPass.get(i).getRuleId());
            } else {
                ruleId = columnRuleBeanMapper.getColumnRuleById(getColumnItemFinishNoPass.get(i).getRuleId());
            }
            String dataContent = String.format("<br>%s. 规则编号:%s;  监控内容:%s;  监控项:%s;  波动范围:%s; %s.%s;", i + 1, ruleId, getColumnItemFinishNoPass.get(i).getContent(),
                    getColumnItemFinishNoPass.get(i).getIsWarnning(), wave.replace(",", "") + getColumnItemFinishNoPass.get(i).getScope(),
                    getColumnItemFinishNoPass.get(i).getDatabaseName(), getColumnItemFinishNoPass.get(i).getTableName());
            noPassDetailBuilder.append(dataContent);
        }
        List<String> dnFinishList = new ArrayList<>();
        getItemAllDnrDnf.forEach(tableDnf -> {
            String name = String.format("规则编号: %s；相关库表: %s.%s", tableDnf.getNumber(), tableDnf.getDatabaseName(), tableDnf.getTableName());
            dnFinishList.add(name);
        });
        getColumnItemDnf.forEach(tableDnf -> {
            String name = String.format("规则编号: %s；相关库表: %s.%s", tableDnf.getNumber(), tableDnf.getDatabaseName(), tableDnf.getTableName());
            dnFinishList.add(name);
        });
        StringBuilder dateDnFinishBuilder = new StringBuilder();
        for (int i = 0; i < dnFinishList.size(); i++) {
            String dateDnFinishStr = String.format("<br>%s.%s", i + 1, dnFinishList.get(i));
            dateDnFinishBuilder.append(dateDnFinishStr);
        }
        String noReadyDetail = noReadyDetailBuilder.toString();
        String noPassDetail = noPassDetailBuilder.toString();
        IndexProMail indexProMail = new IndexProMail();
        indexProMail.setProject(project);
        indexProMail.setDnrRule(dnrRule);
        indexProMail.setNoReadyDetail(StringUtils.isEmpty(noReadyDetail) ? FinalVar.NOTHING : noReadyDetail);
        indexProMail.setNoPassDetail(StringUtils.isEmpty(noPassDetail) ? FinalVar.NOTHING : noPassDetail);
        indexProMail.setReadyRule(readyRule);
        indexProMail.setPassDevition(passDevition);
        indexProMail.setDataDnfDetail(StringUtils.isEmpty(dateDnFinishBuilder.toString()) ? FinalVar.NOTHING : dateDnFinishBuilder.toString());
        List<IndexProMail> mailList = new ArrayList<>();
        mailList.add(indexProMail);
        String alarmUniqueId = channelAlarmId;
        notifySysService.notifyBuilder(project, alarmUniqueId, mailList);

    }

    /*
    经销商数据中心
     */
    @Override
    public void runDistributorPro(String partitionType) {
        Integer id = 41;
        List<TableRuleBean> getReadyItemAll = tableRuleBeanMapper.getReadyItemAll(id, id, partitionType);
        List<RuleRunningLogBean> getReadyItemAllFinishPass = ruleRunningLogBeanMapper.getReadyItemAllFinish(id, id, 0, "通过", partitionType);
        List<RuleRunningLogBean> getReadyItemAllFinishNoPass = ruleRunningLogBeanMapper.getReadyItemAllFinish(id, id, 0, "不通过", partitionType);
        List<TableRuleBean> getReadyItemAllDnf = tableRuleBeanMapper.getReadyItemAllDnf(id, id, 0, partitionType);
        List<RuleRunningLogBean> getTimeItemNotReady = ruleRunningLogBeanMapper.getTimeRuleNotReady(id, id, 0, "未就绪", partitionType);

        List<TableRuleBean> getItemAllDnr = tableRuleBeanMapper.getItemAllDnr(id, id, partitionType);
        List<TableRuleBean> getItemAllDnrDnf = tableRuleBeanMapper.getItemAllDnrDnf(id, id, 0, partitionType);
        List<RuleRunningLogBean> getTableItemDnRFinishPass = ruleRunningLogBeanMapper.getTableItemDnRFinish(id, id, 0, "通过", partitionType);
        List<RuleRunningLogBean> getTableItemDnRFinishNoPass = ruleRunningLogBeanMapper.getTableItemDnRFinish(id, id, 0, "不通过", partitionType);

        List<ColumnRuleBean> getColumnItemAll = columnRuleBeanMapper.getColumnItemAll(id, id, partitionType);
        List<ColumnRuleBean> getColumnItemDnf = columnRuleBeanMapper.getColumnItemDnf(id, id, 0, partitionType);
        List<RuleRunningLogBean> getColumnItemFinishPass = ruleRunningLogBeanMapper.getColumnItemFinish(id, id, 0, "通过", partitionType);
        List<RuleRunningLogBean> getColumnItemFinishNoPass = ruleRunningLogBeanMapper.getColumnItemFinish(id, id, 0, "不通过", partitionType);

        int readyItemCount = getReadyItemAll.size();   //就绪总数
        int readyItemCountFinishPass = getReadyItemAllFinishPass.size(); //就绪通过
        int readyItemCountFinishNoPass = getReadyItemAllFinishNoPass.size(); //就绪未通过
        int readyItemCountDnf = getReadyItemAllDnf.size();//就绪未执行

        int tableItemCount = getItemAllDnr.size(); //table总数
        int tableItemCountDnfPass = getTableItemDnRFinishPass.size(); //table执行 通过
        int tableItemCountDnfNoPass = getTableItemDnRFinishNoPass.size();  //table执行 未通过
        int tableItemCountDnf = getItemAllDnrDnf.size(); //table未执行

        int columnItemCount = getColumnItemAll.size(); //column总数
        int columnItemDnfPass = getColumnItemFinishPass.size(); //column执行 通过
        int columnItemDnfNoPass = getColumnItemFinishNoPass.size(); //column执行 未通过
        int columnItemDnf = getColumnItemDnf.size();  //column未执行

        int countAll = readyItemCount + tableItemCount + columnItemCount;
        double passCount = readyItemCountFinishPass + tableItemCountDnfPass + columnItemDnfPass;
        double exceptionDevition = passCount / countAll;
        Double value = exceptionDevition * 100;
        int countDnrAll = tableItemCount + columnItemCount;
        int countDnrPass = tableItemCountDnfPass + columnItemDnfPass;
        int countDnrNoPass = tableItemCountDnfNoPass + columnItemDnfNoPass;
        int countDnrDnf = tableItemCountDnf + columnItemDnf;

        String project = FinalVar.DISTRIBUTOR_DATA_CENTER;
        String passDevition = PatternRule.numberFormat.format(Math.floor(value)) + "%";
        String readyRule = String.format("任务数:%s,  准时就绪:%s,  未准时就绪:%s,  未执行:%s ", readyItemCount,
                readyItemCountFinishPass, readyItemCountFinishNoPass, readyItemCountDnf);
        String dnrRule = String.format("任务数:%s,  通过:%s,  未通过:%s,  监控任务异常:%s ", countDnrAll,
                countDnrPass, countDnrNoPass, countDnrDnf);

        //未就绪明细
        StringBuilder noReadyDetailBuilder = new StringBuilder();
        for (int i = 0; i < getTimeItemNotReady.size(); i++) {
            String ruleId = "null";
            if (getTimeItemNotReady.get(i).getColumnName().equals(FinalVar.TABLECOLUMN)) {
                ruleId = tableRuleBeanMapper.getTableRuleById(getTimeItemNotReady.get(i).getRuleId());
            } else {
                ruleId = columnRuleBeanMapper.getColumnRuleById(getTimeItemNotReady.get(i).getRuleId());
            }
            String dataContent = String.format("<br>%s. 规则编号:%s;  监控内容:%s;  相关库表:%s.%s;  期望时间:%s;", i + 1, ruleId, getTimeItemNotReady.get(i).getContent(),
                    getTimeItemNotReady.get(i).getDatabaseName(), getTimeItemNotReady.get(i).getTableName(), getTimeItemNotReady.get(i).getValueCompare());
            noReadyDetailBuilder.append(dataContent);
        }
        getColumnItemFinishNoPass.addAll(getTableItemDnRFinishNoPass);
        StringBuilder noPassDetailBuilder = new StringBuilder();
        for (int i = 0; i < getColumnItemFinishNoPass.size(); i++) {
            int compareValue = TypeConvert.StringConvertInteger(getColumnItemFinishNoPass.get(i).getValueCompare());
            int num = TypeConvert.StringConvertInteger(getColumnItemFinishNoPass.get(i).getValue());
            double devition = num - compareValue;
            Double waveNum = devition / (compareValue == 0 ? 1 : compareValue);
            String wave = String.format("%.2f", waveNum * 100) + "%";
            String ruleId = "null";
            if (getColumnItemFinishNoPass.get(i).getColumnName().equals(FinalVar.TABLECOLUMN)) {
                ruleId = tableRuleBeanMapper.getTableRuleById(getColumnItemFinishNoPass.get(i).getRuleId());
            } else {
                ruleId = columnRuleBeanMapper.getColumnRuleById(getColumnItemFinishNoPass.get(i).getRuleId());
            }
            String dataContent = String.format("<br>%s. 规则编号:%s;  监控内容:%s;  监控项:%s;  波动范围:%s; %s.%s;", i + 1, ruleId, getColumnItemFinishNoPass.get(i).getContent(),
                    getColumnItemFinishNoPass.get(i).getIsWarnning(), wave.replace(",", "") + getColumnItemFinishNoPass.get(i).getScope(),
                    getColumnItemFinishNoPass.get(i).getDatabaseName(), getColumnItemFinishNoPass.get(i).getTableName());
            noPassDetailBuilder.append(dataContent);
        }
        List<String> dnFinishList = new ArrayList<>();
        getItemAllDnrDnf.forEach(tableDnf -> {

            String name = String.format("规则编号: %s；相关库表: %s.%s", tableDnf.getNumber(), tableDnf.getDatabaseName(), tableDnf.getTableName());
            dnFinishList.add(name);
        });
        getColumnItemDnf.forEach(tableDnf -> {

            String name = String.format("规则编号: %s；相关库表: %s.%s", tableDnf.getNumber(), tableDnf.getDatabaseName(), tableDnf.getTableName());
            dnFinishList.add(name);
        });
        StringBuilder dateDnFinishBuilder = new StringBuilder();
        for (int i = 0; i < dnFinishList.size(); i++) {
            String dateDnFinishStr = String.format("<br>%s.%s", i + 1, dnFinishList.get(i));
            dateDnFinishBuilder.append(dateDnFinishStr);
        }
        String noReadyDetail = noReadyDetailBuilder.toString();
        String noPassDetail = noPassDetailBuilder.toString();
        IndexProMail indexProMail = new IndexProMail();
        indexProMail.setProject(project);
        indexProMail.setDnrRule(dnrRule);
        indexProMail.setNoReadyDetail(StringUtils.isEmpty(noReadyDetail) ? FinalVar.NOTHING : noReadyDetail);
        indexProMail.setNoPassDetail(StringUtils.isEmpty(noPassDetail) ? FinalVar.NOTHING : noPassDetail);
        indexProMail.setReadyRule(readyRule);
        indexProMail.setPassDevition(passDevition);
        indexProMail.setDataDnfDetail(StringUtils.isEmpty(dateDnFinishBuilder.toString()) ? FinalVar.NOTHING : dateDnFinishBuilder.toString());
        List<IndexProMail> mailList = new ArrayList<>();
        mailList.add(indexProMail);

        String alarmUniqueId = distributorAlarmId;
        notifySysService.notifyBuilder(project, alarmUniqueId, mailList);

    }

    /*
    全局报表
     */
    @Override
    public void globalReport(String partitionType) {
        Integer id = 92;
        List<TableRuleBean> getReadyItemAll = tableRuleBeanMapper.getReadyItemAll(id, id, partitionType);
        List<RuleRunningLogBean> getReadyItemAllFinishPass = ruleRunningLogBeanMapper.getReadyItemAllFinish(id, id, 0, "通过", partitionType);
        List<RuleRunningLogBean> getReadyItemAllFinishNoPass = ruleRunningLogBeanMapper.getReadyItemAllFinish(id, id, 0, "不通过", partitionType);
        List<TableRuleBean> getReadyItemAllDnf = tableRuleBeanMapper.getReadyItemAllDnf(id, id, 0, partitionType);
        List<RuleRunningLogBean> getTimeItemNotReady = ruleRunningLogBeanMapper.getTimeRuleNotReady(id, id, 0, "未就绪", partitionType);

        List<TableRuleBean> getItemAllDnr = tableRuleBeanMapper.getItemAllDnr(id, id, partitionType);
        List<TableRuleBean> getItemAllDnrDnf = tableRuleBeanMapper.getItemAllDnrDnf(id, id, 0, partitionType);
        List<RuleRunningLogBean> getTableItemDnRFinishPass = ruleRunningLogBeanMapper.getTableItemDnRFinish(id, id, 0, "通过", partitionType);
        List<RuleRunningLogBean> getTableItemDnRFinishNoPass = ruleRunningLogBeanMapper.getTableItemDnRFinish(id, id, 0, "不通过", partitionType);

        List<ColumnRuleBean> getColumnItemAll = columnRuleBeanMapper.getColumnItemAll(id, id, partitionType);
        List<ColumnRuleBean> getColumnItemDnf = columnRuleBeanMapper.getColumnItemDnf(id, id, 0, partitionType);
        List<RuleRunningLogBean> getColumnItemFinishPass = ruleRunningLogBeanMapper.getColumnItemFinish(id, id, 0, "通过", partitionType);
        List<RuleRunningLogBean> getColumnItemFinishNoPass = ruleRunningLogBeanMapper.getColumnItemFinish(id, id, 0, "不通过", partitionType);

        int readyItemCount = getReadyItemAll.size();   //就绪总数
        int readyItemCountFinishPass = getReadyItemAllFinishPass.size(); //就绪通过
        int readyItemCountFinishNoPass = getReadyItemAllFinishNoPass.size(); //就绪未通过
        int readyItemCountDnf = getReadyItemAllDnf.size();//就绪未执行

        int tableItemCount = getItemAllDnr.size(); //table总数
        int tableItemCountDnfPass = getTableItemDnRFinishPass.size(); //table执行 通过
        int tableItemCountDnfNoPass = getTableItemDnRFinishNoPass.size();  //table执行 未通过
        int tableItemCountDnf = getItemAllDnrDnf.size(); //table未执行

        int columnItemCount = getColumnItemAll.size(); //column总数
        int columnItemDnfPass = getColumnItemFinishPass.size(); //column执行 通过
        int columnItemDnfNoPass = getColumnItemFinishNoPass.size(); //column执行 未通过
        int columnItemDnf = getColumnItemDnf.size();  //column未执行

        int countAll = readyItemCount + tableItemCount + columnItemCount;
        double passCount = readyItemCountFinishPass + tableItemCountDnfPass + columnItemDnfPass;
        double exceptionDevition = passCount / countAll;
        Double value = exceptionDevition * 100;
        int countDnrAll = tableItemCount + columnItemCount;
        int countDnrPass = tableItemCountDnfPass + columnItemDnfPass;
        int countDnrNoPass = tableItemCountDnfNoPass + columnItemDnfNoPass;
        int countDnrDnf = tableItemCountDnf + columnItemDnf;

        String project = FinalVar.GLOBAL_REPORT;
        String passDevition = PatternRule.numberFormat.format(Math.floor(value)) + "%";
        String readyRule = String.format("任务数:%s,  准时就绪:%s,  未准时就绪:%s,  未执行:%s ", readyItemCount,
                readyItemCountFinishPass, readyItemCountFinishNoPass, readyItemCountDnf);
        String dnrRule = String.format("任务数:%s,  通过:%s,  未通过:%s,  监控任务异常:%s ", countDnrAll,
                countDnrPass, countDnrNoPass, countDnrDnf);

        //未就绪明细
        StringBuilder noReadyDetailBuilder = new StringBuilder();
        for (int i = 0; i < getTimeItemNotReady.size(); i++) {
            String ruleId = "null";
            if (getTimeItemNotReady.get(i).getColumnName().equals(FinalVar.TABLECOLUMN)) {
                ruleId = tableRuleBeanMapper.getTableRuleById(getTimeItemNotReady.get(i).getRuleId());
            } else {
                ruleId = columnRuleBeanMapper.getColumnRuleById(getTimeItemNotReady.get(i).getRuleId());
            }

            String dataContent = String.format("<br>%s. 规则编号:%s;  监控内容:%s;  相关库表:%s.%s;  期望时间:%s;", i + 1, ruleId, getTimeItemNotReady.get(i).getContent(),
                    getTimeItemNotReady.get(i).getDatabaseName(), getTimeItemNotReady.get(i).getTableName(), getTimeItemNotReady.get(i).getValueCompare());
            noReadyDetailBuilder.append(dataContent);
        }
        getColumnItemFinishNoPass.addAll(getTableItemDnRFinishNoPass);
        StringBuilder noPassDetailBuilder = new StringBuilder();
        for (int i = 0; i < getColumnItemFinishNoPass.size(); i++) {
            int compareValue = TypeConvert.StringConvertInteger(getColumnItemFinishNoPass.get(i).getValueCompare());
            int num = TypeConvert.StringConvertInteger(getColumnItemFinishNoPass.get(i).getValue());
            double devition = num - compareValue;
            Double waveNum = devition / (compareValue == 0 ? 1 : compareValue);
            String wave = String.format("%.2f", waveNum * 100) + "%";
            String ruleId = "null";
            if (getColumnItemFinishNoPass.get(i).getColumnName().equals(FinalVar.TABLECOLUMN)) {
                ruleId = tableRuleBeanMapper.getTableRuleById(getColumnItemFinishNoPass.get(i).getRuleId());
            } else {
                ruleId = columnRuleBeanMapper.getColumnRuleById(getColumnItemFinishNoPass.get(i).getRuleId());
            }
            String dataContent = String.format("<br>%s. 规则编号:%s;  监控内容:%s;  监控项:%s;  波动范围:%s; %s.%s;", i + 1, ruleId, getColumnItemFinishNoPass.get(i).getContent(),
                    getColumnItemFinishNoPass.get(i).getIsWarnning(), wave.replace(",", "") + getColumnItemFinishNoPass.get(i).getScope(),
                    getColumnItemFinishNoPass.get(i).getDatabaseName(), getColumnItemFinishNoPass.get(i).getTableName());
            noPassDetailBuilder.append(dataContent);
        }
        List<String> dnFinishList = new ArrayList<>();
        getItemAllDnrDnf.forEach(tableDnf -> {

            String name = String.format("规则编号: %s；相关库表: %s.%s", tableDnf.getNumber(), tableDnf.getDatabaseName(), tableDnf.getTableName());
            dnFinishList.add(name);
        });
        getColumnItemDnf.forEach(tableDnf -> {
            String name = String.format("规则编号: %s；相关库表: %s.%s", tableDnf.getNumber(), tableDnf.getDatabaseName(), tableDnf.getTableName());
            dnFinishList.add(name);
        });
        StringBuilder dateDnFinishBuilder = new StringBuilder();
        for (int i = 0; i < dnFinishList.size(); i++) {
            String dateDnFinishStr = String.format("<br>%s.%s", i + 1, dnFinishList.get(i));
            dateDnFinishBuilder.append(dateDnFinishStr);
        }
        String noReadyDetail = noReadyDetailBuilder.toString();
        String noPassDetail = noPassDetailBuilder.toString();
        IndexProMail indexProMail = new IndexProMail();
        indexProMail.setProject(project);
        indexProMail.setDnrRule(dnrRule);
        indexProMail.setNoReadyDetail(StringUtils.isEmpty(noReadyDetail) ? FinalVar.NOTHING : noReadyDetail);
        indexProMail.setNoPassDetail(StringUtils.isEmpty(noPassDetail) ? FinalVar.NOTHING : noPassDetail);
        indexProMail.setReadyRule(readyRule);
        indexProMail.setPassDevition(passDevition);
        indexProMail.setDataDnfDetail(StringUtils.isEmpty(dateDnFinishBuilder.toString()) ? FinalVar.NOTHING : dateDnFinishBuilder.toString());
        List<IndexProMail> mailList = new ArrayList<>();
        mailList.add(indexProMail);

        String alarmUniqueId = globalReportAlarmId;
        notifySysService.notifyBuilder(project, alarmUniqueId, mailList);

    }


    /**
     * 易湃线索宽表
     *
     * @param partitionType
     */
    @Override
    public void yipai(String partitionType) {
        Integer id = 99;
        List<TableRuleBean> getReadyItemAll = tableRuleBeanMapper.getReadyItemAll(id, id, partitionType);
        List<RuleRunningLogBean> getReadyItemAllFinishPass = ruleRunningLogBeanMapper.getReadyItemAllFinish(id, id, 0, "通过", partitionType);
        List<RuleRunningLogBean> getReadyItemAllFinishNoPass = ruleRunningLogBeanMapper.getReadyItemAllFinish(id, id, 0, "不通过", partitionType);
        List<TableRuleBean> getReadyItemAllDnf = tableRuleBeanMapper.getReadyItemAllDnf(id, id, 0, partitionType);
        List<RuleRunningLogBean> getTimeItemNotReady = ruleRunningLogBeanMapper.getTimeRuleNotReady(id, id, 0, "未就绪", partitionType);

        List<TableRuleBean> getItemAllDnr = tableRuleBeanMapper.getItemAllDnr(id, id, partitionType);
        List<TableRuleBean> getItemAllDnrDnf = tableRuleBeanMapper.getItemAllDnrDnf(id, id, 0, partitionType);
        List<RuleRunningLogBean> getTableItemDnRFinishPass = ruleRunningLogBeanMapper.getTableItemDnRFinish(id, id, 0, "通过", partitionType);
        List<RuleRunningLogBean> getTableItemDnRFinishNoPass = ruleRunningLogBeanMapper.getTableItemDnRFinish(id, id, 0, "不通过", partitionType);

        List<ColumnRuleBean> getColumnItemAll = columnRuleBeanMapper.getColumnItemAll(id, id, partitionType);
        List<ColumnRuleBean> getColumnItemDnf = columnRuleBeanMapper.getColumnItemDnf(id, id, 0, partitionType);
        List<RuleRunningLogBean> getColumnItemFinishPass = ruleRunningLogBeanMapper.getColumnItemFinish(id, id, 0, "通过", partitionType);
        List<RuleRunningLogBean> getColumnItemFinishNoPass = ruleRunningLogBeanMapper.getColumnItemFinish(id, id, 0, "不通过", partitionType);

        int readyItemCount = getReadyItemAll.size();   //就绪总数
        int readyItemCountFinishPass = getReadyItemAllFinishPass.size(); //就绪通过
        int readyItemCountFinishNoPass = getReadyItemAllFinishNoPass.size(); //就绪未通过
        int readyItemCountDnf = getReadyItemAllDnf.size();//就绪未执行

        int tableItemCount = getItemAllDnr.size(); //table总数
        int tableItemCountDnfPass = getTableItemDnRFinishPass.size(); //table执行 通过
        int tableItemCountDnfNoPass = getTableItemDnRFinishNoPass.size();  //table执行 未通过
        int tableItemCountDnf = getItemAllDnrDnf.size(); //table未执行

        int columnItemCount = getColumnItemAll.size(); //column总数
        int columnItemDnfPass = getColumnItemFinishPass.size(); //column执行 通过
        int columnItemDnfNoPass = getColumnItemFinishNoPass.size(); //column执行 未通过
        int columnItemDnf = getColumnItemDnf.size();  //column未执行

        int countAll = readyItemCount + tableItemCount + columnItemCount;
        double passCount = readyItemCountFinishPass + tableItemCountDnfPass + columnItemDnfPass;
        double exceptionDevition = passCount / countAll;
        Double value = exceptionDevition * 100;
        int countDnrAll = tableItemCount + columnItemCount;
        int countDnrPass = tableItemCountDnfPass + columnItemDnfPass;
        int countDnrNoPass = tableItemCountDnfNoPass + columnItemDnfNoPass;
        int countDnrDnf = tableItemCountDnf + columnItemDnf;

        String project = FinalVar.YIPAI_REPORT;
        String passDevition = PatternRule.numberFormat.format(Math.floor(value)) + "%";
        String readyRule = String.format("任务数:%s,  准时就绪:%s,  未准时就绪:%s,  未执行:%s ", readyItemCount,
                readyItemCountFinishPass, readyItemCountFinishNoPass, readyItemCountDnf);
        String dnrRule = String.format("任务数:%s,  通过:%s,  未通过:%s,  监控任务未执行:%s ", countDnrAll,
                countDnrPass, countDnrNoPass, countDnrDnf);

        //未就绪明细
        StringBuilder noReadyDetailBuilder = new StringBuilder();
        for (int i = 0; i < getTimeItemNotReady.size(); i++) {
            String ruleId = "null";
            if (getTimeItemNotReady.get(i).getColumnName().equals(FinalVar.TABLECOLUMN)) {
                ruleId = tableRuleBeanMapper.getTableRuleById(getTimeItemNotReady.get(i).getRuleId());
            } else {
                ruleId = columnRuleBeanMapper.getColumnRuleById(getTimeItemNotReady.get(i).getRuleId());
            }

            String dataContent = String.format("<br>%s. 规则编号:%s;  监控内容:%s;  相关库表:%s.%s;  期望时间:%s;", i + 1, ruleId, getTimeItemNotReady.get(i).getContent(),
                    getTimeItemNotReady.get(i).getDatabaseName(), getTimeItemNotReady.get(i).getTableName(), getTimeItemNotReady.get(i).getValueCompare());
            noReadyDetailBuilder.append(dataContent);
        }
        getColumnItemFinishNoPass.addAll(getTableItemDnRFinishNoPass);
        StringBuilder noPassDetailBuilder = new StringBuilder();
        for (int i = 0; i < getColumnItemFinishNoPass.size(); i++) {
            int compareValue = TypeConvert.StringConvertInteger(getColumnItemFinishNoPass.get(i).getValueCompare());
            int num = TypeConvert.StringConvertInteger(getColumnItemFinishNoPass.get(i).getValue());
            double devition = num - compareValue;
            Double waveNum = devition / (compareValue == 0 ? 1 : compareValue);
            String wave = String.format("%.2f", waveNum * 100) + "%";
            String ruleId = "null";
            if (getColumnItemFinishNoPass.get(i).getColumnName().equals(FinalVar.TABLECOLUMN)) {
                ruleId = tableRuleBeanMapper.getTableRuleById(getColumnItemFinishNoPass.get(i).getRuleId());
            } else {
                ruleId = columnRuleBeanMapper.getColumnRuleById(getColumnItemFinishNoPass.get(i).getRuleId());
            }
            String dataContent = String.format("<br>%s. 规则编号:%s;  监控内容:%s;  监控项:%s;  波动范围:%s; %s.%s;", i + 1, ruleId, getColumnItemFinishNoPass.get(i).getContent(),
                    getColumnItemFinishNoPass.get(i).getIsWarnning(), wave.replace(",", "") + getColumnItemFinishNoPass.get(i).getScope(),
                    getColumnItemFinishNoPass.get(i).getDatabaseName(), getColumnItemFinishNoPass.get(i).getTableName());
            noPassDetailBuilder.append(dataContent);
        }
        List<String> dnFinishList = new ArrayList<>();
        getItemAllDnrDnf.forEach(tableDnf -> {

            String name = String.format("规则编号: %s；相关库表: %s.%s", tableDnf.getNumber(), tableDnf.getDatabaseName(), tableDnf.getTableName());
            dnFinishList.add(name);
        });
        getColumnItemDnf.forEach(tableDnf -> {
            String name = String.format("规则编号: %s；相关库表: %s.%s", tableDnf.getNumber(), tableDnf.getDatabaseName(), tableDnf.getTableName());
            dnFinishList.add(name);
        });
        StringBuilder dateDnFinishBuilder = new StringBuilder();
        for (int i = 0; i < dnFinishList.size(); i++) {
            String dateDnFinishStr = String.format("<br>%s.%s", i + 1, dnFinishList.get(i));
            dateDnFinishBuilder.append(dateDnFinishStr);
        }
        String noReadyDetail = noReadyDetailBuilder.toString();
        String noPassDetail = noPassDetailBuilder.toString();
        IndexProMail indexProMail = new IndexProMail();
        indexProMail.setProject(project);
        indexProMail.setDnrRule(dnrRule);
        indexProMail.setNoReadyDetail(StringUtils.isEmpty(noReadyDetail) ? FinalVar.NOTHING : noReadyDetail);
        indexProMail.setNoPassDetail(StringUtils.isEmpty(noPassDetail) ? FinalVar.NOTHING : noPassDetail);
        indexProMail.setReadyRule(readyRule);
        indexProMail.setPassDevition(passDevition);
        indexProMail.setDataDnfDetail(StringUtils.isEmpty(dateDnFinishBuilder.toString()) ? FinalVar.NOTHING : dateDnFinishBuilder.toString());
        List<IndexProMail> mailList = new ArrayList<>();
        mailList.add(indexProMail);

        String alarmUniqueId = yipaiReportAlarmId;
        notifySysService.notifyBuilder(project, alarmUniqueId, mailList);

    }

    @Override
    public void dimensionWarning(String alarmUniqueId, String dateBase, String tableName, String content, String column, String error,
                                 String id, String project, Integer checkDay,
                                 String partitionType, String user, String priority, String errorMsg) {
        logger.info("database:{},tableName:{},alarmUniqueId:{}-维度规则没通过  报警", dateBase, tableName, alarmUniqueId);


        notifySysService.notifyBuilder(alarmUniqueId, dateBase
                , tableName, content, column, error, id, project, checkDay, partitionType, user, priority, errorMsg);

        AlarmHistoryEntity alarmHistoryEntity = new AlarmHistoryEntity();
        alarmHistoryEntity.setDatabaseName(dateBase);
        alarmHistoryEntity.setTableName(tableName);
        alarmHistoryEntity.setRuleId(id);
        alarmHistoryDao.save(alarmHistoryEntity);
    }


    private String getProName(List<ItemModuleListBean> itemModuleListBeanList) {
        String name = null;
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

    public void getTimeRuleNoPass() {
        List<TableRuleBean> tableRuleBeanList = tableRuleBeanMapper.getTimeRuleNoPass("未就绪", 0);

        if (tableRuleBeanList == null || tableRuleBeanList.size() == 0) {
            return;
        }
        logger.info("未就绪list:{}", tableRuleBeanList.toString());
        tableRuleBeanList.forEach(item -> {
            tableRuleService.getTimeRuleNoPass(item);
        });
    }

    public void sendTodayNoPassReport() {
        List<RuleRunningLogBean> ruleRunningLogBeanList = ruleRunningLogBeanMapper.queryTodayNoPassRule();

        if (ruleRunningLogBeanList == null || ruleRunningLogBeanList.isEmpty()) {
            return;
        }
        ruleRunningLogBeanList.forEach(item -> {
            List<ItemModuleListBean> itemModuleListBeanList = itemModuleListBeanMapper.selectProByTableId(item.getRuleId());
            String proName = getProName(itemModuleListBeanList);
            String wave = null;
            if (!FinalVar.RULE_END_TIME.equals(item.getType())) {
                wave = getWave(item.getValueCompare(), item.getValue());
            }
            warning("nyweisf0", item.getDatabaseName(), item.getTableName(), item.getContent()
                    , null, item.getError(), item.getValue(), item.getValueCompare(), item.getScope(), item.getNumber(), proName, StringUtils.isEmpty(wave) ? null : wave, item.getCheckday(), item.getPartitionType(), item.getLeader(), item.getPriority());
        });
    }

    private String getWave(String valueCompare, String value) {
        int compareValue = TypeConvert.StringConvertInteger(valueCompare);
        int num = TypeConvert.StringConvertInteger(value);
        double devition = num - compareValue;
        Double waveNum = devition / (compareValue == 0 ? 1 : compareValue);
        String wave = PatternRule.numberFormat.format(waveNum * 100) + "%";
        return wave;
    }
}