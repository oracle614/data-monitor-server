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
    private String indexAlarmID;
    @Value("${alarmdatahouse.id}")
    private String datahouseAlarmID;
    @Value("${alarmchannel.id}")
    private String channelAlarmID;
    @Value("${alarmdistributor.id}")
    private String distributorAlarmID;
    @Value("${alarmexception.id}")
    private String exceptionAlarmID;

    @Value("${group.id}")
    private String groupId;

    @Value("${indexProGroup.id}")
    private String indexProGroupId;

    @Value("${sqlGroup.id}")
    private String sqlGroupId;

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
    public void warnning(String alarmUniqueId, String dateBase, String tableName, String content, String column, String error, String value
            , String valueCompare, String scope, String id, String project, String waveScope, Integer checkDay, String partitionType) {

        logger.info("database:{},tableName:{},alarmUniqueId:{}-规则没通过  报警", dateBase, tableName, alarmUniqueId);


        notifySysService.notifyBuilder(alarmUniqueId, dateBase
                , tableName, content, column, error, value, valueCompare, scope, id, project, waveScope, checkDay, partitionType);

        AlarmHistoryEntity alarmHistoryEntity = new AlarmHistoryEntity();
        alarmHistoryEntity.setDatabaseName(dateBase);
        alarmHistoryEntity.setTableName(tableName);
        alarmHistoryEntity.setRuleId(id);
        alarmHistoryDao.save(alarmHistoryEntity);
    }

//    public void sqlWarnning( String dateBase, String tableName, String content) {
//
//        logger.info("database:{},tableName:{},alarmUniqueId:{}-自定义sql", dateBase, tableName, alarmUniqueId);
//        String alarmUniqueId ="1r6lljyy";
//        notifySysService.notifyBuilder(alarmUniqueId, dateBase
//                , tableName, content);
//    }

   // alarmWhenExecRuleException
    @Override
    public void warningWhenExecRuleException(String subject, String body){
        String alarmUniqueId =exceptionAlarmID;
        logger.info("send alarm subject:{}, body:{}, alarmUniqueId:{}", subject, body, alarmUniqueId);
        notifySysService.notifyBuilder(alarmUniqueId,subject ,body);

    }



    @Override
    public boolean isPartitionReady(String partitions, Integer day, String partitionType) {
        if (StringUtils.isEmpty(partitions)) {
            return false;
        }
        String[] partitionsArr = partitions.split("=");
        if (FinalVar.MONTH.equals(partitionType)) {
            if (partitionsArr[1].compareTo(DateFormatSafe.formatMonth(DateFormatSafe.getMonth(day))) >= 0) {
                return true;
            }
        } else {
            if (partitionsArr[1].compareTo(DateFormatSafe.format(DateFormatSafe.getDay(day))) >= 0
                    || partitionsArr[1].compareTo(DateFormatSafe.formatSign(DateFormatSafe.getDay(day))) >= 0) {
                return true;
            }
        }
        return false;
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

    @Override
    public void runIndexPro(String partitionType) {

        List<TableRuleBean> getReadyItemAll = tableRuleBeanMapper.getReadyItemAll(1, 1, partitionType);
        List<RuleRunningLogBean> getReadyItemAllFinishPass = ruleRunningLogBeanMapper.getReadyItemAllFinish(1, 1, 0, "通过", partitionType);
        List<RuleRunningLogBean> getReadyItemAllFinishNoPass = ruleRunningLogBeanMapper.getReadyItemAllFinish(1, 1, 0, "不通过", partitionType);
        List<TableRuleBean> getReadyItemAllDnf = tableRuleBeanMapper.getReadyItemAllDnf(1, 1, 0, partitionType);

        List<TableRuleBean> getItemAllDnr = tableRuleBeanMapper.getItemAllDnr(1, 1, partitionType);
        List<TableRuleBean> getItemAllDnrDnf = tableRuleBeanMapper.getItemAllDnrDnf(1, 1, 0, partitionType);
        List<RuleRunningLogBean> getTableItemDnRFinishPass = ruleRunningLogBeanMapper.getTableItemDnRFinish(1, 1, 0, "通过", partitionType);
        List<RuleRunningLogBean> getTableItemDnRFinishNoPass = ruleRunningLogBeanMapper.getTableItemDnRFinish(1, 1, 0, "不通过", partitionType);

        List<ColumnRuleBean> getColumnItemAll = columnRuleBeanMapper.getColumnItemAll(1, 1, partitionType);
        List<ColumnRuleBean> getColumnItemDnf = columnRuleBeanMapper.getColumnItemDnf(1, 1, 0, partitionType);
        List<RuleRunningLogBean> getColumnItemFinishPass = ruleRunningLogBeanMapper.getColumnItemFinish(1, 1, 0, "通过", partitionType);
        List<RuleRunningLogBean> getColumnItemFinishNoPass = ruleRunningLogBeanMapper.getColumnItemFinish(1, 1, 0, "不通过", partitionType);

        //就绪总数
        int readyItemCount = getReadyItemAll.size();
        //就绪通过
        int readyItemCountFinishPass = getReadyItemAllFinishPass.size();
        //就绪未通过
        int readyItemCountFinishNoPass = getReadyItemAllFinishNoPass.size();
        //就绪未执行
        int readyItemCountDnf = getReadyItemAllDnf.size();

        //table总数
        int tableItemCount = getItemAllDnr.size();
        //table执行 通过
        int tableItemCountDnfPass = getTableItemDnRFinishPass.size();
        //table执行 未通过
        int tableItemCountDnfNoPass = getTableItemDnRFinishNoPass.size();
        //table未执行
        int tableItemCountDnf = getItemAllDnrDnf.size();

        //column总数
        int columnItemCount = getColumnItemAll.size();
        //column执行 通过
        int columnItemDnfPass = getColumnItemFinishPass.size();
        //column执行 未通过
        int columnItemDnfNoPass = getColumnItemFinishNoPass.size();
        //column未执行
        int columnItemDnf = getColumnItemDnf.size();

        int countAll = readyItemCount + tableItemCount + columnItemCount;
        double passCount = readyItemCountFinishPass + tableItemCountDnfPass + columnItemDnfPass;
        double exceptionDevition = passCount / countAll;
        Double value = exceptionDevition * 100;
        int countDnrAll = tableItemCount + columnItemCount;
        int countDnrPass = tableItemCountDnfPass + columnItemDnfPass;
        int countDnrNoPass = tableItemCountDnfNoPass + columnItemDnfNoPass;
        int countDnrDnf = tableItemCountDnf + columnItemDnf;

        String project = "指数项目";
        String passDevition = PatternRule.numberFormat.format(value) + "%";
        String readyRule = String.format("任务数:%s,  准时就绪:%s,  未准时就绪:%s,  未执行:%s ", readyItemCount,
                readyItemCountFinishPass, readyItemCountFinishNoPass, readyItemCountDnf);
        String dnrRule = String.format("任务数:%s,  通过:%s,  未通过:%s,  监控任务异常:%s ", countDnrAll,
                countDnrPass, countDnrNoPass, countDnrDnf);
        getColumnItemFinishNoPass.addAll(getTableItemDnRFinishNoPass);
        StringBuilder noPassDetailBuilder = new StringBuilder();
        for (int i = 0; i < getColumnItemFinishNoPass.size(); i++) {
            int compareValue = TypeConvert.StringConvertInteger(getColumnItemFinishNoPass.get(i).getValueCompare());
            int num = TypeConvert.StringConvertInteger(getColumnItemFinishNoPass.get(i).getValue());
            double devition = num - compareValue;
            Double waveNum = devition / (compareValue == 0 ? 1 : compareValue);
            String wave = PatternRule.numberFormat.format(waveNum * 100) + "%";
            String dataContent = String.format("\n%s.监控内容:%s;  监控项:%s;  波动范围:%s;  影响范围:%s;", i + 1, getColumnItemFinishNoPass.get(i).getContent(),
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
            String dateDnFinishStr = String.format("\n%s.影响范围:%s", i + 1, dnFinishList.get(i));
            dateDnFinishBuilder.append(dateDnFinishStr);
        }
        String noPassDetail = noPassDetailBuilder.toString();
        IndexProMail indexProMail = new IndexProMail();
        indexProMail.setProject(project);
        indexProMail.setDnrRule(dnrRule);
        indexProMail.setNoPassDetail(StringUtils.isEmpty(noPassDetail) ? FinalVar.NOTHING : noPassDetail);
        indexProMail.setReadyRule(readyRule);
        indexProMail.setPassDevition(passDevition);
        indexProMail.setDataDnfDetail(StringUtils.isEmpty(dateDnFinishBuilder.toString()) ? FinalVar.NOTHING : dateDnFinishBuilder.toString());
        List<IndexProMail> mailList = new ArrayList<>();
        mailList.add(indexProMail);

        String alarmUniqueId =indexAlarmID ;
        notifySysService.notifyBuilder(alarmUniqueId,mailList);
}

    @Override
    public void runDataWarehourse(String partitionType, Integer id, String proName) {

        List<TableRuleBean> getReadyItemAll = tableRuleBeanMapper.getReadyItemAll(id, id, partitionType);
        List<RuleRunningLogBean> getReadyItemAllFinishPass = ruleRunningLogBeanMapper.getReadyItemAllFinish(id, id, 0, "通过", partitionType);
        List<RuleRunningLogBean> getReadyItemAllFinishNoPass = ruleRunningLogBeanMapper.getReadyItemAllFinish(id, id, 0, "不通过", partitionType);
        List<TableRuleBean> getReadyItemAllDnf = tableRuleBeanMapper.getReadyItemAllDnf(id, id, 0, partitionType);

        List<TableRuleBean> getItemAllDnr = tableRuleBeanMapper.getItemAllDnr(id, id, partitionType);
        List<TableRuleBean> getItemAllDnrDnf = tableRuleBeanMapper.getItemAllDnrDnf(id, id, 0, partitionType);
        List<RuleRunningLogBean> getTableItemDnRFinishPass = ruleRunningLogBeanMapper.getTableItemDnRFinish(id, id, 0, "通过", partitionType);
        List<RuleRunningLogBean> getTableItemDnRFinishNoPass = ruleRunningLogBeanMapper.getTableItemDnRFinish(id, id, 0, "不通过", partitionType);

        List<ColumnRuleBean> getColumnItemAll = columnRuleBeanMapper.getColumnItemAll(id, id, partitionType);
        List<ColumnRuleBean> getColumnItemDnf = columnRuleBeanMapper.getColumnItemDnf(id, id, 0, partitionType);
        List<RuleRunningLogBean> getColumnItemFinishPass = ruleRunningLogBeanMapper.getColumnItemFinish(id, id, 0, "通过", partitionType);
        List<RuleRunningLogBean> getColumnItemFinishNoPass = ruleRunningLogBeanMapper.getColumnItemFinish(id, id, 0, "不通过", partitionType);
        //就绪总数
        int readyItemCount = getReadyItemAll.size();
        //就绪通过
        int readyItemCountFinishPass = getReadyItemAllFinishPass.size();
        //就绪未通过
        int readyItemCountFinishNoPass = getReadyItemAllFinishNoPass.size();
        //就绪未执行
        int readyItemCountDnf = getReadyItemAllDnf.size();

        //table总数
        int tableItemCount = getItemAllDnr.size();
        //table执行 通过
        int tableItemCountDnfPass = getTableItemDnRFinishPass.size();
        //table执行 未通过
        int tableItemCountDnfNoPass = getTableItemDnRFinishNoPass.size();
        //table未执行
        int tableItemCountDnf = getItemAllDnrDnf.size();

        //column总数
        int columnItemCount = getColumnItemAll.size();
        //column执行 通过
        int columnItemDnfPass = getColumnItemFinishPass.size();
        //column执行 未通过
        int columnItemDnfNoPass = getColumnItemFinishNoPass.size();
        //column未执行
        int columnItemDnf = getColumnItemDnf.size();

        int countAll = readyItemCount + tableItemCount + columnItemCount;
        double passCount = readyItemCountFinishPass + tableItemCountDnfPass + columnItemDnfPass;
        double exceptionDevition = passCount / countAll;
        Double value = exceptionDevition * 100;
        int countDnrAll = tableItemCount + columnItemCount;
        int countDnrPass = tableItemCountDnfPass + columnItemDnfPass;
        int countDnrNoPass = tableItemCountDnfNoPass + columnItemDnfNoPass;
        int countDnrDnf = tableItemCountDnf + columnItemDnf;

        String project = proName;
        String passDevition = PatternRule.numberFormat.format(value) + "%";
        String readyRule = String.format("任务数:%s,  准时就绪:%s,  未准时就绪:%s,  未执行:%s ", readyItemCount,
                readyItemCountFinishPass, readyItemCountFinishNoPass, readyItemCountDnf);
        String dnrRule = String.format("任务数:%s,  通过:%s,  未通过:%s,  监控任务异常:%s ", countDnrAll,
                countDnrPass, countDnrNoPass, countDnrDnf);
        getColumnItemFinishNoPass.addAll(getTableItemDnRFinishNoPass);
        StringBuilder noPassDetailBuilder = new StringBuilder();
        for (int i = 0; i < getColumnItemFinishNoPass.size(); i++) {
            int compareValue = TypeConvert.StringConvertInteger(getColumnItemFinishNoPass.get(i).getValueCompare());
            int num = TypeConvert.StringConvertInteger(getColumnItemFinishNoPass.get(i).getValue());
            double devition = num - compareValue;
            Double waveNum = devition / (compareValue == 0 ? 1 : compareValue);
            String wave = PatternRule.numberFormat.format(waveNum * 100) + "%";
            String dataContent = String.format("\n%s.监控内容:%s;  监控项:%s;  波动范围:%s; %s.%s;", i + 1, getColumnItemFinishNoPass.get(i).getContent(),
                    getColumnItemFinishNoPass.get(i).getIsWarnning(), wave.replace(",", "") + getColumnItemFinishNoPass.get(i).getScope(),
                    getColumnItemFinishNoPass.get(i).getDatabaseName(), getColumnItemFinishNoPass.get(i).getTableName());
            noPassDetailBuilder.append(dataContent);
        }
        List<String> dnFinishList = new ArrayList<>();
        getItemAllDnrDnf.forEach(tableDnf -> {
            String name = String.format("%s.%s", tableDnf.getDatabaseName(), tableDnf.getTableName());
            dnFinishList.add(name);
        });
        getColumnItemDnf.forEach(tableDnf -> {
            String name = String.format("%s.%s", tableDnf.getDatabaseName(), tableDnf.getTableName());
            dnFinishList.add(name);
        });
        StringBuilder dateDnFinishBuilder = new StringBuilder();
        for (int i = 0; i < dnFinishList.size(); i++) {
            String dateDnFinishStr = String.format("\n%s.%s", i + 1, dnFinishList.get(i));
            dateDnFinishBuilder.append(dateDnFinishStr);
        }
        String noPassDetail = noPassDetailBuilder.toString();
        IndexProMail indexProMail = new IndexProMail();
        indexProMail.setProject(project);
        indexProMail.setDnrRule(dnrRule);
        indexProMail.setNoPassDetail(StringUtils.isEmpty(noPassDetail) ? FinalVar.NOTHING : noPassDetail);
        indexProMail.setReadyRule(readyRule);
        indexProMail.setPassDevition(passDevition);
        indexProMail.setDataDnfDetail(StringUtils.isEmpty(dateDnFinishBuilder.toString()) ? FinalVar.NOTHING : dateDnFinishBuilder.toString());
        List<IndexProMail> mailList = new ArrayList<>();
        mailList.add(indexProMail);
String alarmUniqueId =datahouseAlarmID;
        notifySysService.notifyBuilder(alarmUniqueId, mailList);


    }

    @Override
    public void runPro(String partitionType, Integer id, String proName) {

        List<TableRuleBean> getReadyItemAll = tableRuleBeanMapper.getReadyItemAll(id, id, partitionType);
        List<RuleRunningLogBean> getReadyItemAllFinishPass = ruleRunningLogBeanMapper.getReadyItemAllFinish(id, id, 0, "通过", partitionType);
        List<RuleRunningLogBean> getReadyItemAllFinishNoPass = ruleRunningLogBeanMapper.getReadyItemAllFinish(id, id, 0, "不通过", partitionType);
        List<TableRuleBean> getReadyItemAllDnf = tableRuleBeanMapper.getReadyItemAllDnf(id, id, 0, partitionType);

        List<TableRuleBean> getItemAllDnr = tableRuleBeanMapper.getItemAllDnr(id, id, partitionType);
        List<TableRuleBean> getItemAllDnrDnf = tableRuleBeanMapper.getItemAllDnrDnf(id, id, 0, partitionType);
        List<RuleRunningLogBean> getTableItemDnRFinishPass = ruleRunningLogBeanMapper.getTableItemDnRFinish(id, id, 0, "通过", partitionType);
        List<RuleRunningLogBean> getTableItemDnRFinishNoPass = ruleRunningLogBeanMapper.getTableItemDnRFinish(id, id, 0, "不通过", partitionType);

        List<ColumnRuleBean> getColumnItemAll = columnRuleBeanMapper.getColumnItemAll(id, id, partitionType);
        List<ColumnRuleBean> getColumnItemDnf = columnRuleBeanMapper.getColumnItemDnf(id, id, 0, partitionType);
        List<RuleRunningLogBean> getColumnItemFinishPass = ruleRunningLogBeanMapper.getColumnItemFinish(id, id, 0, "通过", partitionType);
        List<RuleRunningLogBean> getColumnItemFinishNoPass = ruleRunningLogBeanMapper.getColumnItemFinish(id, id, 0, "不通过", partitionType);
        //就绪总数
        int readyItemCount = getReadyItemAll.size();
        //就绪通过
        int readyItemCountFinishPass = getReadyItemAllFinishPass.size();
        //就绪未通过
        int readyItemCountFinishNoPass = getReadyItemAllFinishNoPass.size();
        //就绪未执行
        int readyItemCountDnf = getReadyItemAllDnf.size();

        //table总数
        int tableItemCount = getItemAllDnr.size();
        //table执行 通过
        int tableItemCountDnfPass = getTableItemDnRFinishPass.size();
        //table执行 未通过
        int tableItemCountDnfNoPass = getTableItemDnRFinishNoPass.size();
        //table未执行
        int tableItemCountDnf = getItemAllDnrDnf.size();

        //column总数
        int columnItemCount = getColumnItemAll.size();
        //column执行 通过
        int columnItemDnfPass = getColumnItemFinishPass.size();
        //column执行 未通过
        int columnItemDnfNoPass = getColumnItemFinishNoPass.size();
        //column未执行
        int columnItemDnf = getColumnItemDnf.size();

        int countAll = readyItemCount + tableItemCount + columnItemCount;
        double passCount = readyItemCountFinishPass + tableItemCountDnfPass + columnItemDnfPass;
        double exceptionDevition = passCount / countAll;
        Double value = exceptionDevition * 100;
        int countDnrAll = tableItemCount + columnItemCount;
        int countDnrPass = tableItemCountDnfPass + columnItemDnfPass;
        int countDnrNoPass = tableItemCountDnfNoPass + columnItemDnfNoPass;
        int countDnrDnf = tableItemCountDnf + columnItemDnf;

        String project = proName;
        String passDevition = PatternRule.numberFormat.format(value) + "%";
        String readyRule = String.format("任务数:%s,  准时就绪:%s,  未准时就绪:%s,  未执行:%s ", readyItemCount,
                readyItemCountFinishPass, readyItemCountFinishNoPass, readyItemCountDnf);
        String dnrRule = String.format("任务数:%s,  通过:%s,  未通过:%s,  监控任务异常:%s ", countDnrAll,
                countDnrPass, countDnrNoPass, countDnrDnf);
        getColumnItemFinishNoPass.addAll(getTableItemDnRFinishNoPass);
        StringBuilder noPassDetailBuilder = new StringBuilder();
        for (int i = 0; i < getColumnItemFinishNoPass.size(); i++) {
            int compareValue = TypeConvert.StringConvertInteger(getColumnItemFinishNoPass.get(i).getValueCompare());
            int num = TypeConvert.StringConvertInteger(getColumnItemFinishNoPass.get(i).getValue());
            double devition = num - compareValue;
            Double waveNum = devition / (compareValue == 0 ? 1 : compareValue);
            String wave = PatternRule.numberFormat.format(waveNum * 100) + "%";
            String dataContent = String.format("\n%s.监控内容:%s;  监控项:%s;  波动范围:%s; %s.%s;", i + 1, getColumnItemFinishNoPass.get(i).getContent(),
                    getColumnItemFinishNoPass.get(i).getIsWarnning(), wave.replace(",", "") + getColumnItemFinishNoPass.get(i).getScope(),
                    getColumnItemFinishNoPass.get(i).getDatabaseName(), getColumnItemFinishNoPass.get(i).getTableName());
            noPassDetailBuilder.append(dataContent);
        }
        List<String> dnFinishList = new ArrayList<>();
        getItemAllDnrDnf.forEach(tableDnf -> {
            String name = String.format("%s.%s", tableDnf.getDatabaseName(), tableDnf.getTableName());
            dnFinishList.add(name);
        });
        getColumnItemDnf.forEach(tableDnf -> {
            String name = String.format("%s.%s", tableDnf.getDatabaseName(), tableDnf.getTableName());
            dnFinishList.add(name);
        });
        StringBuilder dateDnFinishBuilder = new StringBuilder();
        for (int i = 0; i < dnFinishList.size(); i++) {
            String dateDnFinishStr = String.format("\n%s.%s", i + 1, dnFinishList.get(i));
            dateDnFinishBuilder.append(dateDnFinishStr);
        }
        String noPassDetail = noPassDetailBuilder.toString();
        IndexProMail indexProMail = new IndexProMail();
        indexProMail.setProject(project);
        indexProMail.setDnrRule(dnrRule);
        indexProMail.setNoPassDetail(StringUtils.isEmpty(noPassDetail) ? FinalVar.NOTHING : noPassDetail);
        indexProMail.setReadyRule(readyRule);
        indexProMail.setPassDevition(passDevition);
        indexProMail.setDataDnfDetail(StringUtils.isEmpty(dateDnFinishBuilder.toString()) ? FinalVar.NOTHING : dateDnFinishBuilder.toString());
        List<IndexProMail> mailList = new ArrayList<>();
        mailList.add(indexProMail);
String alarmUniqueId = channelAlarmID;

        notifySysService.notifyBuilder(alarmUniqueId, mailList);

    }


    @Override
    //经销商数据中心
    public void runDistributorPro(String partitionType, Integer id, String proName) {

        List<TableRuleBean> getReadyItemAll = tableRuleBeanMapper.getReadyItemAll(id, id, partitionType);
        List<RuleRunningLogBean> getReadyItemAllFinishPass = ruleRunningLogBeanMapper.getReadyItemAllFinish(id, id, 0, "通过", partitionType);
        List<RuleRunningLogBean> getReadyItemAllFinishNoPass = ruleRunningLogBeanMapper.getReadyItemAllFinish(id, id, 0, "不通过", partitionType);
        List<TableRuleBean> getReadyItemAllDnf = tableRuleBeanMapper.getReadyItemAllDnf(id, id, 0, partitionType);

        List<TableRuleBean> getItemAllDnr = tableRuleBeanMapper.getItemAllDnr(id, id, partitionType);
        List<TableRuleBean> getItemAllDnrDnf = tableRuleBeanMapper.getItemAllDnrDnf(id, id, 0, partitionType);
        List<RuleRunningLogBean> getTableItemDnRFinishPass = ruleRunningLogBeanMapper.getTableItemDnRFinish(id, id, 0, "通过", partitionType);
        List<RuleRunningLogBean> getTableItemDnRFinishNoPass = ruleRunningLogBeanMapper.getTableItemDnRFinish(id, id, 0, "不通过", partitionType);

        List<ColumnRuleBean> getColumnItemAll = columnRuleBeanMapper.getColumnItemAll(id, id, partitionType);
        List<ColumnRuleBean> getColumnItemDnf = columnRuleBeanMapper.getColumnItemDnf(id, id, 0, partitionType);
        List<RuleRunningLogBean> getColumnItemFinishPass = ruleRunningLogBeanMapper.getColumnItemFinish(id, id, 0, "通过", partitionType);
        List<RuleRunningLogBean> getColumnItemFinishNoPass = ruleRunningLogBeanMapper.getColumnItemFinish(id, id, 0, "不通过", partitionType);
        //就绪总数
        int readyItemCount = getReadyItemAll.size();
        //就绪通过
        int readyItemCountFinishPass = getReadyItemAllFinishPass.size();
        //就绪未通过
        int readyItemCountFinishNoPass = getReadyItemAllFinishNoPass.size();
        //就绪未执行
        int readyItemCountDnf = getReadyItemAllDnf.size();

        //table总数
        int tableItemCount = getItemAllDnr.size();
        //table执行 通过
        int tableItemCountDnfPass = getTableItemDnRFinishPass.size();
        //table执行 未通过
        int tableItemCountDnfNoPass = getTableItemDnRFinishNoPass.size();
        //table未执行
        int tableItemCountDnf = getItemAllDnrDnf.size();

        //column总数
        int columnItemCount = getColumnItemAll.size();
        //column执行 通过
        int columnItemDnfPass = getColumnItemFinishPass.size();
        //column执行 未通过
        int columnItemDnfNoPass = getColumnItemFinishNoPass.size();
        //column未执行
        int columnItemDnf = getColumnItemDnf.size();

        int countAll = readyItemCount + tableItemCount + columnItemCount;
        double passCount = readyItemCountFinishPass + tableItemCountDnfPass + columnItemDnfPass;
        double exceptionDevition = passCount / countAll;
        Double value = exceptionDevition * 100;
        int countDnrAll = tableItemCount + columnItemCount;
        int countDnrPass = tableItemCountDnfPass + columnItemDnfPass;
        int countDnrNoPass = tableItemCountDnfNoPass + columnItemDnfNoPass;
        int countDnrDnf = tableItemCountDnf + columnItemDnf;

        String project = proName;
        String passDevition = PatternRule.numberFormat.format(value) + "%";
        String readyRule = String.format("任务数:%s,  准时就绪:%s,  未准时就绪:%s,  未执行:%s ", readyItemCount,
                readyItemCountFinishPass, readyItemCountFinishNoPass, readyItemCountDnf);
        String dnrRule = String.format("任务数:%s,  通过:%s,  未通过:%s,  监控任务异常:%s ", countDnrAll,
                countDnrPass, countDnrNoPass, countDnrDnf);
        getColumnItemFinishNoPass.addAll(getTableItemDnRFinishNoPass);
        StringBuilder noPassDetailBuilder = new StringBuilder();
        for (int i = 0; i < getColumnItemFinishNoPass.size(); i++) {
            int compareValue = TypeConvert.StringConvertInteger(getColumnItemFinishNoPass.get(i).getValueCompare());
            int num = TypeConvert.StringConvertInteger(getColumnItemFinishNoPass.get(i).getValue());
            double devition = num - compareValue;
            Double waveNum = devition / (compareValue == 0 ? 1 : compareValue);
            String wave = PatternRule.numberFormat.format(waveNum * 100) + "%";
            String dataContent = String.format("\n%s.监控内容:%s;  监控项:%s;  波动范围:%s; %s.%s;", i + 1, getColumnItemFinishNoPass.get(i).getContent(),
                    getColumnItemFinishNoPass.get(i).getIsWarnning(), wave.replace(",", "") + getColumnItemFinishNoPass.get(i).getScope(),
                    getColumnItemFinishNoPass.get(i).getDatabaseName(), getColumnItemFinishNoPass.get(i).getTableName());
            noPassDetailBuilder.append(dataContent);
        }
        List<String> dnFinishList = new ArrayList<>();
        getItemAllDnrDnf.forEach(tableDnf -> {
            String name = String.format("%s.%s", tableDnf.getDatabaseName(), tableDnf.getTableName());
            dnFinishList.add(name);
        });
        getColumnItemDnf.forEach(tableDnf -> {
            String name = String.format("%s.%s", tableDnf.getDatabaseName(), tableDnf.getTableName());
            dnFinishList.add(name);
        });
        StringBuilder dateDnFinishBuilder = new StringBuilder();
        for (int i = 0; i < dnFinishList.size(); i++) {
            String dateDnFinishStr = String.format("\n%s.%s", i + 1, dnFinishList.get(i));
            dateDnFinishBuilder.append(dateDnFinishStr);
        }
        String noPassDetail = noPassDetailBuilder.toString();
        IndexProMail indexProMail = new IndexProMail();
        indexProMail.setProject(project);
        indexProMail.setDnrRule(dnrRule);
        indexProMail.setNoPassDetail(StringUtils.isEmpty(noPassDetail) ? FinalVar.NOTHING : noPassDetail);
        indexProMail.setReadyRule(readyRule);
        indexProMail.setPassDevition(passDevition);
        indexProMail.setDataDnfDetail(StringUtils.isEmpty(dateDnFinishBuilder.toString()) ? FinalVar.NOTHING : dateDnFinishBuilder.toString());
        List<IndexProMail> mailList = new ArrayList<>();
        mailList.add(indexProMail);

String alarmUniqueId=distributorAlarmID;
System.out.println(distributorAlarmID);
        notifySysService.notifyBuilder(alarmUniqueId, mailList);

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
}
