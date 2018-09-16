package com.yiche.service.impl;

import com.yiche.bean.*;
import com.yiche.bussiness.PlatformBussiness;
import com.yiche.dao.*;
import com.yiche.db.ConnectFactory;
import com.yiche.em.DbType;
import com.yiche.service.ExcutingRuleService;
import com.yiche.service.NotifySysService;
import com.yiche.service.SyncRuleExecTimeService;
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
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


@Service
public class TableRuleServiceImpl implements TableRuleService {


    private final Logger logger = LoggerFactory.getLogger(TableRuleServiceImpl.class);

    @Autowired
    TableRuleBeanMapper tableRuleBeanMapper;

    @Autowired
    RuleRunServiceImpl ruleRunService;

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

    @Value("${hdfs.url}")
    private  String hdfsUrl;



    @Override
    public Vector<TableRuleBean> getTableRule() {
        Vector<TableRuleBean> vector = new Vector<>();
        String nowTime = DateUtils.format(new Date(), DateUtils.DATE_TIME_PATTERN);
        List<TableRuleBean> ruleList = tableRuleBeanMapper.queryNeedRunList(nowTime);
        filterTableRulesHasBeenRun(ruleList);
        filterTableRulesNotNeedRun(ruleList);
        if(ruleList.isEmpty()) {
            return vector;
        }
        vector.addAll(ruleList);
        return vector;
    }

    public void filterTableRulesNotNeedRun(List<TableRuleBean> tableRuleList) {
        if(tableRuleList.isEmpty()) {
            return;
        }
        Iterator<TableRuleBean> iterator = tableRuleList.iterator();
        while (iterator.hasNext()) {
            TableRuleBean item = iterator.next();
            if(!partionTypeMonthIsNeedRun(item)) {
                iterator.remove();
            }
        }
    }

    public boolean partionTypeMonthIsNeedRun(TableRuleBean item){
        if(FinalVar.MONTH.equals(item.getPartitionType()) && !DateFormatSafe.isFirstDay()){
            return false;
        }
        return true;
    }


    public void filterTableRulesHasBeenRun(List<TableRuleBean> tableRuleList) {
        if(tableRuleList.isEmpty()) {
            return;
        }

        Iterator<TableRuleBean> iterator = tableRuleList.iterator();
        while (iterator.hasNext()) {
            TableRuleBean item = iterator.next();
            if(ruleAlreadyRuned(item)) {
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
                logger.info( "{}:运行,连接总数:!{}!",Thread.currentThread().getName(),FinalVar.dbConnCount);
                Statement stmt= null;
                Connection con= null;
                try {
                    con = connectFactory.createConnect(DbType.Presto);
                    stmt = con.createStatement();

                    String partitions = ruleRunService.getNewestPartition(item.getDatabaseName(), item.getTableName(), stmt);
                    logger.info("获取最新分区:itemId:{},dataBase:{},tableName:{},patitions:{}", item.getTid(), item.getDatabaseName(),item.getTableName(),partitions);

                    if(!excutingRuleService.isPartitionReady(partitions,item.getCheckDay(),item.getPartitionType())){
                        logger.info("分区没有生成:itemId:{},dataBase:{},tableName:{}", item.getTid(), item.getDatabaseName(),item.getTableName());

                        if(item.getMonitorType().equals(FinalVar.RULE_END_TIME)){
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
                        case FinalVar.RULE_END_TIME:
                            tableEndTimeRuleExec(item, true);
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
//        AlarmChannelService.mailAlarm("表规则执行异常(id:" + id + ")", msg, users);
    }

    public void closeAfterQuery(Statement stmt, Connection con) {
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e) {
                logger.error("stmt关闭失败",e);
            }
        }
        if (con != null) {
            try {
                con.close();
                FinalVar.dbConnCount--;
                logger.info("hiveConnect关闭");
            } catch (SQLException e) {
                FinalVar.dbConnCount++;
                logger.error("hiveConnect关闭失败",e);
            }
        }
    }



    public String tableRuleRun(TableRuleBean  item) {
        logger.info( "{}:运行,hive连接总数:!{}!",Thread.currentThread().getName(),FinalVar.dbConnCount);

        Statement stmt= null;
        Connection con=null;
        try {
            con = connectFactory.createConnect(DbType.Presto);
            stmt = con.createStatement();

            String partitions = ruleRunService.getNewestPartition(item.getDatabaseName(), item.getTableName(), stmt);
            logger.info("获取最新分区:itemId:{},dataBase:{},tableName:{},patitions:{}", item.getTid(), item.getDatabaseName(),item.getTableName(),partitions);

            if(!excutingRuleService.isPartitionReady(partitions,item.getCheckDay(),item.getPartitionType())){
                logger.info("分区没有生成:itemId:{},dataBase:{},tableName:{}", item.getTid(), item.getDatabaseName(),item.getTableName());

                if(FinalVar.RULE_END_TIME.equals(item.getMonitorType())){
                    tableEndTimeRuleExecWhenPartitionNotReady(item, false);
                    return "规则执行完成";
                }
                return "日分区未生成";
            }

            switch (item.getMonitorType()) {
                case FinalVar.RULE_TABLE_COUNT:
                    tableDataCountRuleExec(item, partitions, stmt,false);
                    break;
                case "执行时长":
                    logger.error("没有处理执行时长,itemId:{}", item.getTid());
                    break;
                case FinalVar.RULE_END_TIME:
                    tableEndTimeRuleExec(item, false);
                    break;
                default:
                    logger.error("没有此种类型规则,itemId:{}", item.getTid());
                    break;
            }
        } catch (Exception e) {
            logger.error("执行规则异常",e);
            alarmWhenExecRuleException(item.getId(), e.getMessage());
        } finally {
            closeAfterQuery(stmt, con);
        }
        return  "规则执行完成";
    }




   @Override
    public void getTimeRuleNoPass(TableRuleBean item) {
                logger.info( "开始执行未通过规则，{}:运行,hive连接总数:!{}!",Thread.currentThread().getName(),FinalVar.dbConnCount);
                Statement stmt= null;
                Connection con=null;
                try {
                    con = connectFactory.createConnect(DbType.Presto);
                    stmt = con.createStatement();
                    //判断分区和数据是否生成

                    String partitions = ruleRunService.getNewestPartition(item.getDatabaseName(), item.getTableName(), stmt);
//                    String partitions="pt=2018-06-12";
                    if(!excutingRuleService.isPartitionReady(partitions,item.getCheckDay(),item.getPartitionType())){
                        logger.info("当天分区没有生成:dataBase:{},tableName{},patitions:{},type:{},checkDay:{},partitionType:{}",
                                item.getDatabaseName(),item.getTableName(),partitions,item.getMonitorType(),item.getCheckDay(),item.getPartitionType());
                        List<ItemModuleListBean> itemModuleListBeanList=itemModuleListBeanMapper.selectProByTableId(item.getTid());
                        String name=getProName(itemModuleListBeanList);
                       //报警;
                        excutingRuleService.warnning(item.getAlarmUniqueId(), item.getDatabaseName(), item.getTableName(),item.getContent()
                                ,null,"完成时间","未就绪"
                                ,item.getSelf(),null,item.getNumber(),name,null,item.getCheckDay(),item.getPartitionType());
                        return;
                    }else {
                        logger.info("未通过规则检测,分区已经生成,tid:{},partitions:{}", item.getTid(), partitions);
                        WarnResultBean  warnResultBean = finishTimeCompare(item);
                        //报警
                        excutingRuleService.warnning(item.getAlarmUniqueId(), item.getDatabaseName(), item.getTableName(),item.getContent()
                                ,null,"完成时间",warnResultBean.getValue()
                                ,item.getSelf(),null,item.getNumber(),warnResultBean.getProject(),
                                warnResultBean.getScope(),item.getCheckDay(),item.getPartitionType());
                        //修改log
                        ruleRunningLogBeanMapper.updateStatusByRuleId(warnResultBean.getValue(),item.getTid(),
                               0);
                    }
                } catch (Exception e) {
                    logger.error("执行规则异常",e);
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
        WarnResultBean warnResultBean = countCompare(item, partitions, stmt, item.getTcondition());

        insertResultLog(item.getTowner(), "表级别", item.getMonitorType()
                , warnResultBean.isFlag() ? FinalVar.PASS : FinalVar.NOPASS
                , warnResultBean,item.getDatabaseName(),item.getTableName(),item.getTid(),item.getContent(),item.getPartitionType());

        if(addCheckLog) {
            addRuleCheck(item, FinalVar.RULE_TABLE_COUNT, FinalVar.TABLECOLUMN);
        }

        logger.info("表规则数据量检测执行完成:database:{},tableName:{},itemId:{}",item.getDatabaseName(),
                item.getTableName(), item.getTid());
    }


    public void tableEndTimeRuleExec(TableRuleBean item, boolean addCheckLog) throws Exception{
        WarnResultBean warnResultBean = finishTimeCompare(item);
        insertResultLog(item.getTowner(), "表级别", item.getMonitorType()
                , warnResultBean.isFlag() ? FinalVar.PASS : FinalVar.NOPASS
                , warnResultBean,item.getDatabaseName(),item.getTableName(),item.getTid(),item.getContent(),item.getPartitionType());


        if(addCheckLog){
            addRuleCheck(item, FinalVar.RULE_END_TIME, FinalVar.TABLECOLUMN);
        }

        logger.info("success完成规则执行完成:database:{},tableName:{},itemtId:{}",item.getDatabaseName(),
                item.getTableName(),item.getTid());
    }


    private  void  sqlRule(TableRuleBean item,String partitions,Statement stmt){
        /*if(isCheck(item, FinalVar.RULE_SQL,FinalVar.TABLECOLUMN)){
            return;
        }*/
        WarnResultBean warnResultBean = sqlCompare(item,partitions,stmt);
        insertResultLog(item.getTowner(), "表级别", item.getMonitorType()
                , FinalVar.PASS
                , warnResultBean,item.getDatabaseName(),item.getTableName(),item.getTid(),item.getContent(),item.getPartitionType());
        logger.info("database:{},tableName:{}-sql自定义规则执行完成",item.getDatabaseName()
                ,item.getTableName());
    }


    private void tableEndTimeRuleExecWhenPartitionNotReady(TableRuleBean item, boolean addCheckLog) throws Exception{
        WarnResultBean warnResultBean = finishTimeCompareNoPar(item);
        if(warnResultBean.isFlag()){
            return;
        }

        insertResultLog(item.getTowner(), "表级别", item.getMonitorType()
                ,  FinalVar.NOPASS, warnResultBean,item.getDatabaseName(),item.getTableName(),item.getTid(),item.getContent(),item.getPartitionType());

        if(addCheckLog) {
            addRuleCheck(item, FinalVar.RULE_END_TIME, FinalVar.TABLECOLUMN);
        }

        logger.info("success完成规则执行完成,database:{},tableName:{},itemId:{}",item.getDatabaseName()
                ,item.getTableName(), item.getTid());
    }


    private WarnResultBean finishTimeCompareNoPar(TableRuleBean item) throws Exception{
        logger.info("执行finishTimeNoPar规则:dataBase:{},tableName:{}",item.getDatabaseName(),item.getTableName());
        Calendar ruleCalendar = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date  valueTime=null;
        try {
            valueTime= sdf.parse(DateFormatSafe.formatSign(new Date())+" "+item.getSelf());
            ruleCalendar.setTime(valueTime);
        } catch (ParseException e) {
            e.printStackTrace();
            throw new Exception("规则时间解析失败");
        }

        String ruleTime=sdf.format(valueTime);
        List<ItemModuleListBean> itemModuleListBeanList=itemModuleListBeanMapper.selectProByTableId(item.getTid());
        //获取规则关联的模块名称
        String name=getProName(itemModuleListBeanList);
        String nowTime=sdf.format(new Date());
        WarnResultBean warnResultBean = new WarnResultBean();
        warnResultBean.setFlag(true);
        logger.info("当前时间:{},     规则时间:{}",nowTime,ruleTime);
        if(nowTime.compareTo(ruleTime)>0){
            excutingRuleService.warnning(item.getAlarmUniqueId(), item.getDatabaseName(), item.getTableName(),item.getContent()
                    ,null,"完成时间","未就绪",ruleTime,null,item.getNumber(),name,null,
                    item.getCheckDay(),item.getPartitionType());
            warnResultBean.setProject(name);
            warnResultBean.setValue("未就绪");
            warnResultBean.setCompareValue(ruleTime);
            warnResultBean.setError("完成时间");
            warnResultBean.setFlag(false);
            return warnResultBean;
        }

        return warnResultBean;
    }
    private WarnResultBean finishTimeCompare(TableRuleBean item) throws Exception{
        logger.info("执行finishTime规则:id:{},dataBase:{},tableName:{}",item.getNumber(),item.getDatabaseName(),item.getTableName());
       String path= String.format("/bitauto/sign/%s/%s/%s/_SUCCESS",item.getDatabaseName(),item.getTableName()
       ,DateFormatSafe.formatSign(DateFormatSafe.getDay(item.getCheckDay())));
        logger.info("success文件path:{}",path);
        Calendar  ruleCalendar=Calendar.getInstance();
        SimpleDateFormat  sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date  valueTime=null;
        try {
            valueTime= sdf.parse(DateFormatSafe.formatSign(new Date())+" "+item.getSelf());
            ruleCalendar.setTime(valueTime);
        } catch (ParseException e) {
            logger.info("value:{}",item.getSelf());
            logger.error("规则时间解析失败",e);
        }
        String ruleTime=sdf.format(valueTime);
        HttpFSFileSystem hdfs= new HttpFSFileSystem();
        List<ItemModuleListBean> itemModuleListBeanList=itemModuleListBeanMapper.selectProByTableId(item.getTid());
        String name=getProName(itemModuleListBeanList);
        WarnResultBean warnResultBean = new WarnResultBean();
        warnResultBean.setProject(name);
        warnResultBean.setFlag(true);
        warnResultBean.setCompareValue(ruleTime);
        warnResultBean.setError("success完成时间");
        URI uri;
        Long  time=null;
        try {
            uri = new URI(hdfsUrl+path);
            hdfs.initialize(uri,"luozhenyu");
            FileStatus fileStatus=hdfs.getFileStatus(uri);
            time=fileStatus.getModificationTime();
        } catch (URISyntaxException e) {
            logger.error("链接hdfs解析失败",e);
            throw new Exception("链接hdfs解析失败");
//            updateTableSonStatus(item.getTid());
        } catch (IOException e) {
            logger.info("文件不存在",e);
            warnResultBean.setValue("success文件未生成");
            logger.info(" succes文件没有生成  table:{}", item.getDatabaseName()+"."+item.getTableName());
//            updateTableSonStatus(item.getTid());
            return  warnResultBean;
        }
        String modifyTime=sdf.format(time);
        warnResultBean.setValue(modifyTime);
        long t=valueTime.getTime()>time?valueTime.getTime()-time:time-valueTime.getTime();
        String diffTime=getTime(t);
        logger.info("修改时间:{}, 规则时间:{},时间差:{}   ",modifyTime,ruleTime,diffTime );
        warnResultBean.setScope(diffTime);
//        updateTableSonStatus(item.getTid());
        return warnResultBean;
    }

    private String  getTime(long diff){
        long days = diff / (1000 * 60 * 60 * 24);
        long hours = (diff-days*(1000 * 60 * 60 * 24))/(1000* 60 * 60);
        long minutes = (diff-days*(1000 * 60 * 60 * 24)-hours*(1000* 60 * 60))/(1000* 60);
        return  ""+days+"天"+hours+"小时"+minutes+"分";
    }

    private WarnResultBean   sqlCompare(TableRuleBean item, String partitions, Statement stmt){
        logger.info("执行数据量规则:id:{}dataBase:{},tableName:{}",item.getNumber(),item.getDatabaseName(),item.getTableName());
//      String  result = ruleRunService.getSql(item.getColumnName(),stmt,partitions,DateFormatSafe.getDay(item.getCheckDay()),item.getSql());
//            excutingRuleService.sqlWarnning(item.getAlarmUniqueId(),item.getDatabaseName(),item.getTableName(),result);
        WarnResultBean warnResultBean = new WarnResultBean();
        return   warnResultBean;
    }
    private WarnResultBean countCompare(TableRuleBean item, String partitions, Statement stmt,String where) throws Exception{
        //获取表的count
        logger.info("执行数据量规则:id:{}dataBase:{},tableName:{}",item.getNumber(),item.getDatabaseName(),item.getTableName());
//        int countNow = ruleRunService.getCount(item.getDatabaseName(), item.getTableName(), null
//                , stmt, partitions, DateFormatSafe.getDay(item.getCheckDay()),where);
        int countNow = ruleRunService.getCount(item.getDatabaseName(), item.getTableName(), null
                , stmt, partitions,
                FinalVar.MONTH.equals(item.getPartitionType())? DateFormatSafe.getMonth(item.getCheckDay()):DateFormatSafe.getDay(item.getCheckDay()),
                where,item.getPartitionType());
        List<ItemModuleListBean> itemModuleListBeanList=itemModuleListBeanMapper.selectProByTableId(item.getTid());
        String name=getProName(itemModuleListBeanList);
        WarnResultBean warnResultBean = new WarnResultBean();
        warnResultBean.setValue(String.valueOf(countNow));
        warnResultBean.setFlag(true);
        warnResultBean.setProject(name);
        Map<String, String> dataMap = new HashMap<>();
        //环比
        if (!StringUtils.isEmpty(item.gethCompare()) ){
            int dayCount = compareResult(item, dataMap, stmt, partitions, 1+item.getCheckDay(),where);
            double devition =countNow - dayCount;
            Double value =devition/(dayCount==0?1:dayCount);
            String  wave=PatternRule.numberFormat.format(value*100);
            logger.info("count环比:id:{},value:{},valueCompare:{},scope:{},wave:{}dataBase:{},tableName:{}",
                    item.getNumber(),countNow,dayCount,item.gethCompare(),wave,item.getDatabaseName(),item.getTableName());
            if (!PatternRule.isRule(item.gethCompare(), wave) ){
                warnResultBean.setFlag(false);
                switch (item.getPartitionType()){
                    case FinalVar.MONTH:
                        warnResultBean.setError("数据量月环比"+"-"+item.getPartitionType());
                        break;
                    case FinalVar.DAY:
                        warnResultBean.setError("数据量日环比"+"-"+item.getPartitionType());
                        break;
                    default:
                        warnResultBean.setError("数据量日环比"+"-"+item.getPartitionType());
                }
                warnResultBean.setCompareValue(TypeConvert.IntegerConvertString(dayCount));
                warnResultBean.setScope(item.gethCompare());
                excutingRuleService.warnning(item.getAlarmUniqueId(), item.getDatabaseName(), item.getTableName(),item.getContent()
                        ,null,warnResultBean.getError(),TypeConvert.IntegerConvertString(countNow)
                        ,TypeConvert.IntegerConvertString(dayCount),item.gethCompare(),item.getNumber(),name,wave+"%",item.getCheckDay(),item.getPartitionType());
                return warnResultBean;
            }

        }
        //同比
        if (!StringUtils.isEmpty(item.gettCompare()) ) {
            int dayCount = compareResult(item, dataMap, stmt, partitions, 7+item.getCheckDay(),where);
            double devition =countNow - dayCount;
            Double value = devition/(dayCount==0?1:dayCount);
            String  wave=PatternRule.numberFormat.format(value*100);
            logger.info("count同比:id:{},value:{},valueCompare:{},scope:{},wave:{},dataBase:{},tableName:{}",
                    item.getNumber(),countNow,dayCount,item.gethCompare(),wave,item.getDatabaseName(),item.getTableName());
            if (!PatternRule.isRule(item.gettCompare(), wave)) {
                warnResultBean.setFlag(false);
                switch (item.getPartitionType()){
                    case FinalVar.MONTH:
                        warnResultBean.setError("数据量月同比"+"-"+item.getPartitionType());
                        break;
                    case FinalVar.DAY:
                        warnResultBean.setError("数据量周同比"+"-"+item.getPartitionType());
                        break;
                    default:
                        warnResultBean.setError("数据量周同比"+"-"+item.getPartitionType());
                }
                warnResultBean.setCompareValue(TypeConvert.IntegerConvertString(dayCount));
                warnResultBean.setScope(item.gettCompare());
                excutingRuleService.warnning(item.getAlarmUniqueId(), item.getDatabaseName(), item.getTableName(),item.getContent()
                        ,null,warnResultBean.getError(),TypeConvert.IntegerConvertString(countNow)
                        ,TypeConvert.IntegerConvertString(dayCount),item.gettCompare(),item.getNumber(),name,wave+"%",item.getCheckDay(),item.getPartitionType());
                return warnResultBean;
            }
        }
        //七天
        if (!StringUtils.isEmpty(item.getSevenWaveAvg()) ) {
            Integer  dayCount=ruleRunService.selectSevenDayDate(item.getDatabaseName(),item.getTableName(),stmt,partitions,null
//                    ,DateFormatSafe.formatSign(DateFormatSafe.getDay(6+item.getCheckDay()+1))
                    ,FinalVar.MONTH.equals(item.getPartitionType())?
                            DateFormatSafe.formatMonth(DateFormatSafe.getMonth(6+item.getCheckDay()+1)) :
                            DateFormatSafe.formatSign(DateFormatSafe.getDay(6+item.getCheckDay()+1))
                    ,FinalVar.MONTH.equals(item.getPartitionType())?
                            DateFormatSafe.formatMonth(DateFormatSafe.getMonth(item.getCheckDay()+1)) :
                            DateFormatSafe.formatSign(DateFormatSafe.getDay(item.getCheckDay()+1)));
            double devition =countNow - dayCount;
            Double value = devition/(dayCount==0?1:dayCount);
            String  wave=PatternRule.numberFormat.format(value*100);
            logger.info("count7天（月）波动:id:{},value:{},valueCompare:{},scope:{},wave:{},dataBase:{},tableName:{}",
                    item.getNumber(),countNow,dayCount,item.gethCompare(),wave,item.getDatabaseName(),item.getTableName());
            if(!PatternRule.isRule(item.getSevenWaveAvg(),wave)){
                excutingRuleService.warnning(item.getAlarmUniqueId(),item.getDatabaseName(),item.getTableName(),item.getContent()
                        ,null,"数据量七天比"+"-"+item.getPartitionType(),TypeConvert.IntegerConvertString(countNow)
                        ,TypeConvert.IntegerConvertString(dayCount),item.getSevenWaveAvg(),item.getNumber(),name,wave+"%",item.getCheckDay(),item.getPartitionType());
                warnResultBean.setFlag(false);
                warnResultBean.setError("数据量七天比"+"-"+item.getPartitionType());
                warnResultBean.setCompareValue(TypeConvert.IntegerConvertString(dayCount));
                warnResultBean.setScope(item.getSevenWaveAvg());
                return  warnResultBean;
            }
        }
        //self
        if (!StringUtils.isEmpty(item.getSelf()) && !PatternRule.isRule(item.getSelf(), countNow)) {
            logger.info("自身比:id:{},value:{},scope{},dataBase:{},tableName:{}",item.getNumber(),countNow,item.getSelf()
                    ,item.getDatabaseName(),item.getTableName());
            excutingRuleService.warnning(item.getAlarmUniqueId(), item.getDatabaseName(), item.getTableName(),item.getContent(),null
                    ,"数据量自身比"+"-"+item.getPartitionType(),TypeConvert.IntegerConvertString(countNow)
                    ,null,item.getSelf(),item.getNumber(),name,null,item.getCheckDay(),item.getPartitionType());
            warnResultBean.setFlag(false);
            warnResultBean.setError("数据量自身比"+"-"+item.getPartitionType());
            warnResultBean.setCompareValue(null);
            warnResultBean.setScope(item.getSelf());
            return warnResultBean;
        }
        return warnResultBean;
    }

    private Integer compareResult(TableRuleBean item, Map<String, String> dataMap, Statement stmt
            , String partitions, int dayNum,String where) throws Exception{
        //获取当天day的count
        Integer dayCount = ruleRunService.getCount(item.getDatabaseName(), item.getTableName()
                , null, stmt, partitions,
                FinalVar.MONTH.equals(item.getPartitionType())? DateFormatSafe.getMonth(dayNum):DateFormatSafe.getDay(dayNum),
                where,item.getPartitionType());
        return dayCount;
    }


    private void insertResultLog(String leader, String level, String type, String status
            , WarnResultBean bean, String dataBaseName,String tableName,String ruleId,String content,String partitionType) {
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
        ruleRunningLogBeanMapper.insert(ruleRunningLogBean);
    }

    private  void  getCountByTime(){
        DateFormatSafe.getDay(7);
    }



    private void insertDayDateCount(String dateBase, String tableName, Integer count,Integer checkDay,String condition) {
        if(!StringUtils.isEmpty(condition)){
            return;
        }
        List<DayDataBean> list = dayDataBeanMapper.selectDayDataToday(dateBase, tableName, "YC_TABLE",checkDay);
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

    private boolean ruleAlreadyRuned(TableRuleBean item) {
        Date now = new Date();
        String checkDay = null;
        if("0".equals(item.getCheckDay())) {
            checkDay = DateUtils.format(now);
        } else {
            checkDay = DateUtils.getAroundDate(now, ~(Integer.valueOf(item.getCheckDay())) + 1, DateUtils.DATE_PATTERN);
        }

        List<RuleCheckBean> list = ruleCheckBeanMapper.getHistoryByRuleidAndDate(item.getTid(), checkDay);
        if(list.isEmpty()) {
            return false;
        }
        return true;
    }

    /**
     * 查看25分钟轮询未就绪的完成时间规则 是否完成
     * @param item
     * @param type
     * @param columnName
     * @return
     */
    private boolean isCheckTimeRule(TableRuleBean item,String type,String columnName){
        List<RuleCheckBean>  logBeanList = ruleCheckBeanMapper.getRuleCheckByRuleId(item.getTid(),item.getCheckDay());
        if(logBeanList!=null&&!logBeanList.isEmpty()){
            logger.info("database:{},tableName:{}-{}已经检查-break",item.getDatabaseName()
                    ,item.getTableName(),type);
            return true;
        }
        return  false;
    }

    /**
     * 获取规则关联的模块名称
     * @param itemModuleListBeanList
     * @return
     */
    private String getProName(List<ItemModuleListBean> itemModuleListBeanList){
        String name =null;
        if(itemModuleListBeanList!=null&&itemModuleListBeanList.size()>0){
            StringBuilder builder = new StringBuilder();
            itemModuleListBeanList.forEach(moduleItem->{
                builder.append(moduleItem.getName());
                builder.append("  ");
            });
            name=builder.toString();
        }
        return  name;
    }


    public void setTableSon(List<String> lists,String time,String fatherId){
        lists.forEach(item->{
            String [] dataTable=item.split("\\.");
            String dateBase=dataTable[0];
            String tableName=dataTable[1];
            List<TableRuleBean> tableRuleBeans=tableRuleBeanMapper.getTableRuleByName(dateBase,tableName,FinalVar.RULE_END_TIME,time);
            if (tableRuleBeans==null||tableRuleBeans.isEmpty()){
                return;
            }else{
                RuleNotReadyBean  ruleNotReadyBean = new RuleNotReadyBean();
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

    public boolean isTableSon(String ruleId){

       List<RuleNotReadyBean> list= ruleNotReadyBeanMapper.getByRuleId(ruleId,0);
       if(list!=null&&!list.isEmpty()){
           logger.info("id:{}未就绪子表存在status:0",ruleId);
           return  true;
       }
       return  false;
    }

    public void updateTableSonStatus(String fatherId){
        ruleNotReadyBeanMapper.updateStatusByFatherId(fatherId,1);
    }
}
