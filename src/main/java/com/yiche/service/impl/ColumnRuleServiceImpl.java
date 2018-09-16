package com.yiche.service.impl;

import com.yiche.bean.*;
import com.yiche.dao.*;
import com.yiche.db.ConnectFactory;
import com.yiche.em.DbType;
import com.yiche.service.*;
import com.yiche.utils.*;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Service
public class ColumnRuleServiceImpl implements ColumnRuleService {


    private final Logger logger = LoggerFactory.getLogger(ColumnRuleServiceImpl.class);

    @Autowired
    ColumnRuleBeanMapper columnRuleBeanMapper;

    @Autowired
    RuleRunService ruleRunService;

    @Autowired
    DayDataBeanMapper dayDataBeanMapper;

    @Autowired
    TableRuleService tableRuleService;

    @Autowired
    RuleRunningLogBeanMapper ruleRunningLogBeanMapper;

    @Autowired
    NotifySysService notifySysService;

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
    private SyncRuleExecTimeService syncRuleExecTimeService;

    @Override
    public Vector<ColumnRuleBean> getColumnRule() {
        Vector<ColumnRuleBean> vector = new Vector<>();
        String nowTime = DateUtils.format(new Date(), DateUtils.DATE_TIME_PATTERN);
        List<ColumnRuleBean> list = columnRuleBeanMapper.queryNeedRunList(nowTime);
        filterColumnRulesHasBeenRun(list);
        vector.addAll(list);
        return  vector;
    }

    public void filterColumnRulesHasBeenRun(List<ColumnRuleBean> columnRuleList) {
        if(columnRuleList.isEmpty()) {
            return;
        }
        Iterator<ColumnRuleBean> iterator = columnRuleList.iterator();
        while (iterator.hasNext()) {
            ColumnRuleBean item = iterator.next();
            if(ruleAlreadyRuned(item)) {
                iterator.remove();
            }
        }
    }

    @Override
    public void columnRuleRun() {
        Vector<ColumnRuleBean>  vector= getColumnRule();
        if (vector == null || vector.size() == 0) {
            logger.info("No need run column rules");
            return;
        }

        ExecutorService cachedThreadPool = Executors.newFixedThreadPool(5);

        vector.forEach(item->{
            cachedThreadPool.execute(()->{
                logger.info( "{}:运行,连接总数:!{}!",Thread.currentThread().getName(),FinalVar.dbConnCount);
                Statement stmt= null;
                Connection con=null;
                try {
                    con = connectFactory.createConnect(DbType.Presto);
                    stmt = con.createStatement();

                    if(FinalVar.MONTH.equals(item.getPartitionType()) && !DateFormatSafe.isFirstDay()){
                        return;
                    }

                    String partitions = ruleRunService.getNewestPartition(item.getDatabaseName(), item.getTableName(), stmt);
                    logger.info("查到的最新分区:id:{},dataBase:{},tableName:{},patitions:{}", item.getId(), item.getDatabaseName(),item.getTableName(),partitions);

                    if(!excutingRuleService.isPartitionReady(partitions, item.getCheckDay(), item.getPartitionType())){
                        logger.info("分区没有生成:id:{},dataBase:{},tableName{},patitions:{}", item.getId(), item.getDatabaseName(),item.getTableName(),partitions);
                        return;
                    }

                    String columnType ="";
                    calTypeCompare(item.getMonitorType(), columnType, stmt, partitions, item.getColumnName(), item);
                } catch (Exception e) {
                    logger.error("执行规则失败----id:{},库表:{}",item.getNumber(),item.getDatabaseName()+"."+item.getTableName(),e);
                    alarmWhenExecRuleException(item.getCid(), e.getMessage());
                } finally {
                    closeAfterQuery(stmt, con);
                }
            });
        });
        cachedThreadPool.shutdown();
    }


    private void alarmWhenExecRuleException(int id, String msg) {


        excutingRuleService.warningWhenExecRuleException("表规则执行异常(id:" + id + ")", msg);
//        ArrayList<String> users = new ArrayList<>();
//        users.add("weiyx@yiche.com");
//        AlarmChannelService.mailAlarm("字段规则执行异常(id:" + id + ")", msg, users);
    }

    private void closeAfterQuery(Statement stmt, Connection con) {
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
                logger.info("connect关闭");
                FinalVar.dbConnCount--;
            } catch (SQLException e) {
                FinalVar.dbConnCount++;
                logger.error("connect关闭失败",e);
            }
        }
    }

    @Override
    public String columnRuleRun(ColumnRuleBean item) {
                logger.info( "{}:运行,连接总数:!{}!",Thread.currentThread().getName(),FinalVar.dbConnCount);
                Statement stmt= null;
                Connection con=null;
                try {
                    con = connectFactory.createConnect(DbType.Presto);
                    stmt = con.createStatement();
                    //判断分区和数据是否生成
//                    String partitions = "pt=2018-06-05";
                    if(FinalVar.MONTH.equals(item.getPartitionType())&&!DateFormatSafe.isFirstDay()){
                        return "月分区未生成";
                    }
                    String partitions = ruleRunService.getNewestPartition(item.getDatabaseName(), item.getTableName(), stmt);
                    logger.info("查到的最新分区:id:{},dataBase:{},tableName:{},patitions:{},type:{},checkDay:{},partitionType:{}",
                            item.getNumber(), item.getDatabaseName(),item.getTableName(),partitions,item.getMonitorType(),item.getCheckDay(),item.getPartitionType());
                    if(!excutingRuleService.isPartitionReady(partitions,item.getCheckDay(),item.getPartitionType())){
                        logger.info("分区没有生成:id:{},dataBase:{},tableName{},patitions:{},type:{},checkDay:{},partitionType:{}",
                                item.getNumber(), item.getDatabaseName(),item.getTableName(),partitions,item.getMonitorType(),item.getCheckDay(),item.getPartitionType());
                        return "日分区未生成";
                    }
                    String columnType ="";
                    calTypeCompare(item.getMonitorType(), columnType, stmt, partitions, item.getColumnName(), item);
                } catch (Exception e) {
                    logger.error("执行规则失败----id:{},库表:{}",item.getNumber(),item.getDatabaseName()+"."+item.getTableName(),e);
                } finally {
                    closeAfterQuery(stmt, con);

                }
        return "规则执行完成";
    }






    public  void   calTypeCompare(String monitorType,String columnType,Statement stmt,String partitions,String columnName,ColumnRuleBean item){
        switch (monitorType){
            case FinalVar.RULE_COLUMN_NULL:
                // // WarnResultBean warnResultBean= nullCompare(item,partitions,stmt,"count("+columnName+")",columnName+" is null"+"and"+item.getCcondition());
                sumRule(stmt, partitions, item.getColumnName(), item,columnType,FinalVar.RULE_COLUMN_NULL,
                        "sum("+columnName+")",columnName+" is null"+"and"+item.getCcondition());
                break;
            case FinalVar.RULE_COLUMN_MIN:
                //   warnResultBean= minNumCompare(item,partitions,stmt,columnName,item.getCcondition(),"min("+columnName+")");
                sumRule(stmt, partitions, item.getColumnName(), item,columnType,FinalVar.RULE_COLUMN_MIN,
                        "min("+columnName+")",item.getCcondition());
                break;
            case FinalVar.RULE_COLUMN_MAX:
                //warnResultBean= maxNumCompare(item,partitions,stmt,columnName,item.getCcondition(),"max("+columnName+")");
                sumRule(stmt, partitions, item.getColumnName(), item,columnType,FinalVar.RULE_COLUMN_MAX,
                        "max("+columnName+")",item.getCcondition());
                break;
            case FinalVar.RULE_COLUMN_AVG:
                // warnResultBean=   avgNumCompare(item,partitions,stmt,columnName,item.getCcondition(),"avg(length("+columnName+"))");
                sumRule(stmt, partitions, item.getColumnName(), item,columnType,FinalVar.RULE_COLUMN_AVG,
                        "avg("+columnName+")",item.getCcondition() );
                break;
            case FinalVar.RULE_COLUMN_EMPTY:
                // warnResultBean=   emptyNumCompare(item,partitions,stmt,columnName,columnName+"=0"+"and"+item.getCcondition(),"count("+columnName+")");
                sumRule(stmt, partitions, item.getColumnName(), item,columnType,FinalVar.RULE_COLUMN_EMPTY,
                        "count("+columnName+")",columnName+"=0"+"and"+item.getCcondition());
                break;
//            case FinalVar.RULE_COLUMN_SPECIAL:
//                specialRule(stmt, partitions, item.getColumnName(), item,FinalVar.RULE_COLUMN_SPECIAL);
//                break;
            case FinalVar.RULE_COLUMN_REPEAT:
                repeatRule(stmt, partitions, item.getColumnName(), item,FinalVar.RULE_COLUMN_REPEAT);
                break;
            case FinalVar.RULE_COLUMN_SUM:
                sumRule(stmt, partitions, item.getColumnName(), item,columnType,FinalVar.RULE_COLUMN_SUM,
                        "sum("+columnName+")", item.getCcondition());
                break;
            default:
                break;
        }
    }

    private void trendsCompare(String columnType,Statement stmt,String partitions,String columnName,ColumnRuleBean item){
        switch (item.getAggregateFunction()){
            case "sum":
//                if(FinalVar.AGGREGATEFUNCTION_XXHG.equals(item.getCalculateType())){
                    logger.info("执行线性回归sum规则-id:{},database:{},tablename:{},column:{}",item.getNumber(),item.getDatabaseName(),item.getTableName(),item.getColumnName());
                if(isCheck(item,FinalVar.AGGREGATEFUNCTION_XXHG+"sum",columnName)){
                    return;
                }
                    xxhgCompare(item,partitions,stmt,columnName,item.getCcondition(),"sum("+columnName+")");
//                }else{
//                    minNumCompare(item,partitions,stmt,columnName,item.getCcondition(),"min("+columnName+")");
//                }
                break;
            case "avg":
//                if(FinalVar.AGGREGATEFUNCTION_XXHG.equals(item.getCalculateType())){
                if(isCheck(item,FinalVar.AGGREGATEFUNCTION_XXHG+"avg",columnName)){
                    return;
                }
                    logger.info("执行线性回归avg规则-database:{},tablename:{},column:{}",item.getDatabaseName(),item.getTableName(),item.getColumnName());
                    xxhgCompare(item,partitions,stmt,columnName,item.getCcondition(),"avg("+columnName+")");
//                }else{
//                    minNumCompare(item,partitions,stmt,columnName,item.getCcondition(),"min("+columnName+")");
//                }
                break;
            default:
                break;
        }
    }
    private void  xxhgCompare(ColumnRuleBean item,String partitions,Statement stmt,String column,String where,String select ){
//        int countNow=   ruleRunService.selectLengthOrNum(item.getDatabaseName(),item.getTableName(),stmt,select,partitions,where
//                ,DateFormatSafe.getDay(item.getCheckDay()));
        int countNow=123123;
        //获取保存的day_data_db    count

//        insertDayDateEmpty(item.getDatabaseName(),item.gettCompare(),countNow,item.getColumnName(),item.getCheckDay());
//        List<DayDataBean>  dayDataList=dayDataBeanMapper.selectByTableInDay(item.getDatabaseName(),item.getTableName()
//                ,item.getColumnName(),65);
//        Map<String,String> dataMap= new HashMap<>();
        //list放在map  key日期  value count值
//        if(dayDataList!=null&&dayDataList.size()>0){
//            dayDataList.forEach(dataItem->{
//                dataMap.put(DateFormatSafe.formatSign(dataItem.getCreateTime())
//                        ,dataItem.getZeroNum().toString());
//            }
//        }
        List<ItemModuleListBean> itemModuleListBeanList=itemModuleListBeanMapper.selectProByColumnId(item.getId());
        String name=getProName(itemModuleListBeanList);
        boolean  flag=true;
        RegressionLine line = new RegressionLine();
        for(int i=1;i<=8;i++){
            Integer value=0;
            Date wedDate=DateFormatSafe.getWedOfThisWeek();
                  value=1234112+i;
//
            line.addDataPoint(new DataPoint(i, Float.valueOf(value.toString())));
        }
        Double dayCount= line.getR();
        logger.info("线性回归:database:{},tableName:{},scope:{},value:{},compareValue:{}"
                ,item.getDatabaseName(),item.getTableName(),item.getAggregateFunction(),countNow,dayCount);
        Double devition =countNow > dayCount ? countNow - dayCount : dayCount - countNow;
        String  wave=PatternRule.numberFormat.format(devition);
        if(!PatternRule.isRule(item.getAggregateFunction(),wave)){
            flag=false;
            excutingRuleService.warnning(item.getAlarmUniqueId(),item.getDatabaseName(),item.getTableName(),item.getContent(),
                    column,"线性回归",TypeConvert.IntegerConvertString(countNow)
                    ,TypeConvert.DoubleConvertString(dayCount),item.getAggregateFunction(),item.getNumber(),name,null,item.getCheckDay(),item.getPartitionType());
        };
        WarnResultBean warnResultBean= new WarnResultBean();
        warnResultBean.setScope(item.getAggregateFunction());
        warnResultBean.setValue(TypeConvert.IntegerConvertString(countNow));
        warnResultBean.setCompareValue(TypeConvert.DoubleConvertString(dayCount));
        warnResultBean.setFlag(flag);
        warnResultBean.setProject(name);
        warnResultBean.setError("线性回归"+"-"+item.getPartitionType());
        insertResultLog(item.getCowner(),item.getColumnName(),item.getCalculateType(), flag?
                FinalVar.PASS:FinalVar.NOPASS,warnResultBean,item.getDatabaseName(),item.getTableName(),item.getColumnName(),
                item.getId(),item.getContent(),item.getPartitionType());
        logger.info("database:{},tableName:{}-线性回归规则执行完成",item.getDatabaseName()
                ,item.getTableName());
    }


    private void nullRule(Statement stmt,String partitions,String columnName,ColumnRuleBean item,String type){
        if(isCheck(item,type,columnName)){
            return;
        }
        logger.info("执行NULL值规则-id:{},database:{},tablename:{},column:{}",item.getNumber(),item.getDatabaseName(),item.getTableName(),item.getColumnName());
        WarnResultBean warnResultBean= nullCompare(item,partitions,stmt,"count("+columnName+")",columnName+" is null"+"and"+item.getCcondition());
        insertResultLog(item.getCowner(),item.getColumnName(),item.getMonitorType(), warnResultBean.isFlag()?
                FinalVar.PASS:FinalVar.NOPASS,warnResultBean,item.getDatabaseName(),item.getTableName(),item.getColumnName(),item.getId(),item.getContent(),item.getPartitionType());
        logger.info("id:{},database:{},tableName:{}-NULL值规则执行完成",item.getNumber(),item.getDatabaseName()
                ,item.getTableName());
    }
    private void avgRule(Statement stmt,String partitions,String columnName,ColumnRuleBean item,String columnType,String type){
        WarnResultBean warnResultBean;
        if(isCheck(item,type,columnName)){
            return;
        }
        if("string".equals(columnType)){
            logger.info("执行平均长度值规则-id:{},database:{},tablename:{},column:{},columnType:{}",item.getNumber(),item.getDatabaseName(),item.getTableName(),item.getColumnName(),columnType);
            warnResultBean=   avgNumCompare(item,partitions,stmt,columnName,item.getCcondition(),"avg(length("+columnName+"))");
        }else{
            logger.info("执行平均值规则-id:{},database:{},tablename:{},column:{},columnType:{}",item.getNumber(),item.getDatabaseName(),item.getTableName(),item.getColumnName(),columnType);
            warnResultBean=   avgNumCompare(item,partitions,stmt,columnName,item.getCcondition(),"avg("+columnName+")");
        }
        insertResultLog(item.getCowner(),item.getColumnName(),item.getMonitorType(),warnResultBean.isFlag()?FinalVar.PASS:FinalVar.NOPASS
                ,warnResultBean,item.getDatabaseName(),item.getTableName(),item.getColumnName(),item.getId(),item.getContent(),item.getPartitionType());
        logger.info("id:{},database:{},tableName:{}-平均值规则执行完成",item.getNumber(),item.getDatabaseName()
                ,item.getTableName());
    }
    private void minRule(Statement stmt,String partitions,String columnName,ColumnRuleBean item,String columnType,String type){
        WarnResultBean warnResultBean;
        if(isCheck(item,type,columnName)){
            return;
        }
//        if("string".equals(columnType)){
//            logger.info("执行最小长度规则-database:{},tableName:{},columnName:{}:",item.getDatabaseName(),item.getTableName(),item.getColumnName(),columnType);
//            warnResultBean= minNumCompare(item,partitions,stmt,columnName,item.getCcondition(),"min(length("+columnName+"))");
//        }else{
            logger.info("执行最小值规则-id:{},database:{},tableName:{},columnName:{}:",item.getNumber(),item.getDatabaseName(),item.getTableName(),item.getColumnName(),columnType);
            warnResultBean= minNumCompare(item,partitions,stmt,columnName,item.getCcondition(),"min("+columnName+")");
//        }
        insertResultLog(item.getCowner(),item.getColumnName(),item.getMonitorType(), warnResultBean.isFlag()?
                FinalVar.PASS:FinalVar.NOPASS,warnResultBean,item.getDatabaseName(),item.getTableName(),item.getColumnName(),item.getId(),item.getContent(),item.getPartitionType());
        logger.info("id:{},database:{},tableName:{}-最小值规则执行完成",item.getNumber(),item.getDatabaseName()
                ,item.getTableName());
    }

    private void maxRule(Statement stmt,String partitions,String columnName,ColumnRuleBean item,String columnType,String type){
        WarnResultBean warnResultBean;
        if(isCheck(item,type,columnName)){
            return;
        }
//        if("string".equals(columnType)){
//            logger.info("执行最大长度规则-database:{},tableName:{},columnName:{}:",item.getDatabaseName(),item.getTableName(),item.getColumnName(),columnType);
//            warnResultBean= maxNumCompare(item,partitions,stmt,columnName,item.getCcondition(),"max(length("+columnName+"))");
//        }else{
            logger.info("id:{},执行最大值规则-database:{},tableName:{},columnName:{}",item.getNumber(),item.getDatabaseName(),item.getTableName(),item.getColumnName(),columnType);
            warnResultBean= maxNumCompare(item,partitions,stmt,columnName,item.getCcondition(),"max("+columnName+")");
//        }
        insertResultLog(item.getCowner(),item.getColumnName(),item.getMonitorType(), warnResultBean.isFlag()?
                FinalVar.PASS:FinalVar.NOPASS,warnResultBean,item.getDatabaseName(),item.getTableName(),item.getColumnName(),item.getId(),item.getContent(),item.getPartitionType());
        logger.info("id:{},database:{},tableName:{}-最大规则执行完成",item.getNumber(),item.getDatabaseName()
                ,item.getTableName());
    }

    private void sumRule(Statement stmt,String partitions,String columnName,ColumnRuleBean item,String columnType,String type,
                         String select,String where){
        WarnResultBean warnResultBean;
        logger.info("{}规则-id:{},database:{},tableName:{},columnName:{}",type,item.getNumber(),item.getDatabaseName(),item.getTableName(),item.getColumnName(),columnType);
        warnResultBean= sumNumCompare(item,partitions,stmt,columnName,where,select,type);

        insertResultLog(item.getCowner(),item.getColumnName(),item.getMonitorType(), warnResultBean.isFlag()?
                FinalVar.PASS:FinalVar.NOPASS,warnResultBean,item.getDatabaseName(),item.getTableName(),item.getColumnName(),item.getId(),item.getContent(),item.getPartitionType());

        addRuleCheck(item, type, columnName);

        logger.info("id:{},database:{},tableName:{}-{}规则执行完成",item.getNumber(),item.getDatabaseName()
                ,item.getTableName(),type);
    }

    private void repeatRule(Statement stmt,String partitions,String columnName,ColumnRuleBean item,String type){
        logger.info("执行重复值规则-id:{},database:{},tableName:{},columnName:{}:",item.getNumber(),item.getDatabaseName(),item.getTableName(),item.getColumnName());

        WarnResultBean warnResultBean=repeatNumCompare(item,partitions,stmt,columnName,item.getCcondition());
        insertResultLog(item.getCowner(),item.getColumnName(),item.getMonitorType(),warnResultBean.isFlag()?FinalVar.PASS:FinalVar.NOPASS
                ,warnResultBean,item.getDatabaseName(),item.getTableName(),item.getColumnName(),item.getId(),item.getContent(),item.getPartitionType());
        logger.info("id:{},database:{},tableName:{}-重复值规则执行完成",item.getNumber(),item.getDatabaseName()
                ,item.getTableName());
    }
    private void emptyRule(Statement stmt,String partitions,String columnName,ColumnRuleBean item,String columnType,String type){
        WarnResultBean warnResultBean;
        if(isCheck(item,type,columnName)){
            return;
        }
        logger.info("执行0值规则-id:{},database:{},tablename:{},column:{},columnType:{}",item.getNumber(),item.getDatabaseName(),item.getTableName(),item.getColumnName(),columnType);
        warnResultBean=   emptyNumCompare(item,partitions,stmt,columnName,columnName+"=0"+"and"+item.getCcondition(),"count("+columnName+")");
        insertResultLog(item.getCowner(), item.getColumnName(),item.getMonitorType()
                , warnResultBean.isFlag()?FinalVar.PASS:FinalVar.NOPASS,warnResultBean,item.getDatabaseName()
                ,item.getTableName(),item.getColumnName(),item.getId(),item.getContent(),item.getPartitionType());
        logger.info("database:{},tableName:{}-空值规则执行完成",item.getDatabaseName()
                ,item.getTableName());
    }
    private void specialRule(Statement stmt,String partitions,String columnName,ColumnRuleBean item,String type){
        logger.info("执行特殊值规则-id:{},database:{},tablename:{},column:{}",item.getNumber(),item.getDatabaseName(),item.getTableName(),item.getColumnName());

        WarnResultBean warnResultBean=likeNumCompare(item,partitions,stmt,columnName,item.getCcondition());
        insertResultLog(item.getCowner(),item.getColumnName(),item.getMonitorType(), warnResultBean.isFlag()?
                        FinalVar.PASS:FinalVar.NOPASS,warnResultBean,item.getDatabaseName()
                ,item.getTableName(),item.getColumnName(),item.getId(),item.getContent(),item.getPartitionType());
        logger.info("id:{},database:{},tableName:{}-特殊值规则执行完成",item.getNumber(),item.getDatabaseName()
                ,item.getTableName());
    }

    private WarnResultBean  nullCompare(ColumnRuleBean item,String partitions,Statement stmt,String column,String where ){

        int countNow=   ruleRunService.selectLengthOrNum(item.getDatabaseName(),item.getTableName(),stmt,column,partitions,where
                ,FinalVar.MONTH.equals(item.getPartitionType())? DateFormatSafe.getMonth(item.getCheckDay()):DateFormatSafe.getDay(item.getCheckDay()),
                item.getPartitionType());
        List<ItemModuleListBean> itemModuleListBeanList=itemModuleListBeanMapper.selectProByColumnId(item.getId());
        String name=getProName(itemModuleListBeanList);
        WarnResultBean warnResultBean = new WarnResultBean();
        warnResultBean.setValue(String.valueOf(countNow));
        warnResultBean.setFlag(true);
        warnResultBean.setProject(name);

        Map<String,String> dataMap= new HashMap<>();
        //环比
        if(!StringUtils.isEmpty(item.gethCompare()) ){
            int dayCount=compareNullResult(item,dataMap,stmt,partitions,1+item.getCheckDay(),column,where);
            Double value = TypeConvert.IntegerConvertDouble((countNow - dayCount))/(dayCount==0?1:dayCount);
            String  wave=PatternRule.numberFormat.format(value*100);
            logger.info("null值环比:id:{},database:{},tableName:{},column:{},scope:{},value:{},compareValue:{},wave:{}",
                    item.getNumber(),item.getDatabaseName(),item.getTableName(),column,item.gethCompare(),countNow,dayCount,wave);
            if(!PatternRule.isRule(item.gethCompare(),wave)){
                excutingRuleService.warnning(item.getAlarmUniqueId(),item.getDatabaseName(),item.getTableName(),item.getContent()
                        ,column,"null环比"+"-"+item.getPartitionType(),TypeConvert.IntegerConvertString(countNow),TypeConvert.IntegerConvertString(dayCount),
                        item.gethCompare(),item.getNumber(),name,wave+"%",item.getCheckDay(),item.getPartitionType());
                warnResultBean.setFlag(false);
                warnResultBean.setError("null环比"+"-"+item.getPartitionType());
                warnResultBean.setCompareValue(TypeConvert.IntegerConvertString(dayCount));
                warnResultBean.setScope(item.gethCompare());
                return  warnResultBean;
            };
        }
        //同比
        if(!StringUtils.isEmpty(item.gettCompare()) ){
            int dayCount=compareNullResult(item,dataMap,stmt,partitions,7+item.getCheckDay(),column,where);
            Double value = TypeConvert.IntegerConvertDouble((countNow - dayCount))/(dayCount==0?1:dayCount);
            String  wave=PatternRule.numberFormat.format(value*100);
            logger.info("null值同比:id:{},database:{},tableName:{},column:{},scope:{},value{},compareValue{}",
                    item.getNumber(),item.getDatabaseName(),item.getTableName(),column,item.gettCompare(),countNow,dayCount);
            if(!PatternRule.isRule(item.gettCompare(),wave)){
                excutingRuleService.warnning(item.getAlarmUniqueId(),item.getDatabaseName(),item.getTableName(),item.getContent(),column
                        ,"null同比"+"-"+item.getPartitionType(),TypeConvert.IntegerConvertString(countNow),TypeConvert.IntegerConvertString(dayCount),
                        item.gettCompare(),item.getNumber(),name,wave+"%",item.getCheckDay(),item.getPartitionType());
                warnResultBean.setFlag(false);
                warnResultBean.setError("null同比"+"-"+item.getPartitionType());
                warnResultBean.setCompareValue(TypeConvert.IntegerConvertString(dayCount));
                warnResultBean.setScope(item.gettCompare());
                return  warnResultBean;
            };
        }
        //self
        if(!StringUtils.isEmpty(item.getSelf())){
            logger.info("null自身比:id:{},database:{},tableName:{},column:{},scope:{},value:{}",
                    item.getNumber(),item.getDatabaseName(),item.getTableName(),column,item.getSelf(),countNow);
            if(!PatternRule.isRule(item.getSelf(),countNow)){
                excutingRuleService.warnning(item.getAlarmUniqueId(),item.getDatabaseName(),item.getTableName(),item.getContent(),column
                        ,"null自身比"+"-"+item.getPartitionType(),TypeConvert.IntegerConvertString(countNow),null,
                        item.getSelf(),item.getNumber(),name,null,item.getCheckDay(),item.getPartitionType());
                warnResultBean.setFlag(false);
                warnResultBean.setError("null自身比"+"-"+item.getPartitionType());
                warnResultBean.setCompareValue(null);
                warnResultBean.setScope(item.getSelf());
                return  warnResultBean;
            }
        }
        //插入记录
        return  warnResultBean;
    }
    private Integer compareNullResult(ColumnRuleBean item,Map<String,String> dataMap,Statement stmt
            ,String partitions,int dayNum,String column,String where){
        //获取当天day的count
        Integer dayCount;
            dayCount =  ruleRunService.selectLengthOrNum(item.getDatabaseName(),item.getTableName(),stmt,column,partitions,
                    where,  FinalVar.MONTH.equals(item.getPartitionType())? DateFormatSafe.getMonth(dayNum):DateFormatSafe.getDay(dayNum),item.getPartitionType());
        return  dayCount;
    }
    private void insertDayDateNull(String dateBase,String tableName,Integer count,String columnName,Integer checkDay,String condition){
        if(!StringUtils.isEmpty(condition)){
            return;
        }
        List<DayDataBean> list=dayDataBeanMapper.selectDayDataToday(dateBase,tableName,columnName,checkDay);
        DayDataBean dayDataBean;
        if(list==null||list.isEmpty()) {
            dayDataBean = new DayDataBean();
            dayDataBean.setNullNum(count);
            dayDataBean.setDatabaseName(dateBase);
            dayDataBean.setTableName(tableName);
            dayDataBean.setCreateTime(DateFormatSafe.getDay(checkDay));
            dayDataBean.setColumnName(columnName);
            dayDataBeanMapper.insert(dayDataBean);
        }else{
            dayDataBean=list.get(0);
            dayDataBean.setNullNum(count);
            dayDataBeanMapper.updateByPrimaryKey(dayDataBean);
        }
    }

    private WarnResultBean  maxNumCompare(ColumnRuleBean item,String partitions,Statement stmt,String column,String where,String select){
        int countNow=    ruleRunService.selectLengthOrNum(item.getDatabaseName(),item.getTableName(),stmt,select,partitions,where
                ,FinalVar.MONTH.equals(item.getPartitionType())? DateFormatSafe.getMonth(item.getCheckDay()):DateFormatSafe.getDay(item.getCheckDay()),item.getPartitionType());
        //获取保存的day_data_db    count
        List<ItemModuleListBean> itemModuleListBeanList=itemModuleListBeanMapper.selectProByColumnId(item.getId());
        String name=getProName(itemModuleListBeanList);
        WarnResultBean warnResultBean= new WarnResultBean();
        warnResultBean.setValue(String.valueOf(countNow));
        warnResultBean.setFlag(true);
        warnResultBean.setProject(name);
        Map<String,String> dataMap= new HashMap<>();
        //环比
        if(!StringUtils.isEmpty(item.gethCompare()) ){
            int dayCount=compareMaxNumResult(item,dataMap,stmt,partitions,1+item.getCheckDay(),column,select,where);
            Double value = TypeConvert.IntegerConvertDouble((countNow - dayCount))/(dayCount==0?1:dayCount);
            String  wave=PatternRule.numberFormat.format(value*100);
            logger.info("最大值环比:id:{},database:{},tableName:{},column:{},scope:{},value:{},compareValue:{},wave:{}",
                    item.getNumber(),item.getDatabaseName(),item.getTableName(),column,item.gethCompare(),countNow,dayCount,wave);
            if(!PatternRule.isRule(item.gethCompare(),wave)){
                excutingRuleService.warnning(item.getAlarmUniqueId(),item.getDatabaseName(),item.getTableName(),item.getContent(),
                        column,"最大值环比"+"-"+item.getPartitionType(),TypeConvert.IntegerConvertString(countNow),TypeConvert.IntegerConvertString(dayCount),
                        item.gethCompare(),item.getNumber(),name,wave+"%",item.getCheckDay(),item.getPartitionType());
                warnResultBean.setFlag(false);
                warnResultBean.setError("最大值环比"+"-"+item.getPartitionType());
                warnResultBean.setCompareValue(TypeConvert.IntegerConvertString(dayCount));
                warnResultBean.setScope(item.gethCompare());
                return  warnResultBean;
            };
        }
        //同比
        if(!StringUtils.isEmpty(item.gettCompare()) ){
            int dayCount=compareMaxNumResult(item,dataMap,stmt,partitions,7+item.getCheckDay(),column,select,where);
            Double value = TypeConvert.IntegerConvertDouble((countNow - dayCount))/(dayCount==0?1:dayCount);
            String  wave=PatternRule.numberFormat.format(value*100);
            logger.info("最大值同比:id:{},database:{},tableName:{},column:{},scope:{},value:{},compareValue:{},wave:{}",
                    item.getNumber(),item.getDatabaseName(),item.getTableName(),column,item.gettCompare(),countNow,dayCount,wave);
            if(!PatternRule.isRule(item.gettCompare(), wave)){
                excutingRuleService.warnning(item.getAlarmUniqueId(),item.getDatabaseName(),item.getTableName(),item.getContent(),column,
                        "最大值同比"+"-"+item.getPartitionType(),TypeConvert.IntegerConvertString(countNow),TypeConvert.IntegerConvertString(dayCount),
                        item.gettCompare(),item.getNumber(),name,wave+"%",item.getCheckDay(),item.getPartitionType());
                warnResultBean.setFlag(false);
                warnResultBean.setError("最大值同比"+"-"+item.getPartitionType());
                warnResultBean.setCompareValue(TypeConvert.IntegerConvertString(dayCount));
                warnResultBean.setScope(item.gettCompare());
                return  warnResultBean;
            };
        }
        if(!StringUtils.isEmpty(item.getSelf())){
            logger.info("最大值自身比:id:{},database:{},tableName:{},column:{},scope:{},value:{}",
                    item.getNumber(),item.getDatabaseName(),item.getTableName(),column,item.getSelf(),countNow);
            if(!PatternRule.isRule(item.getSelf(),countNow)){
                excutingRuleService.warnning(item.getAlarmUniqueId(),item.getDatabaseName(),item.getTableName(),item.getContent(),column,
                        "最大值自身比"+"-"+item.getPartitionType(),TypeConvert.IntegerConvertString(countNow),null,
                        item.getSelf(),item.getNumber(),name,null,item.getCheckDay(),item.getPartitionType());
                warnResultBean.setFlag(false);
                warnResultBean.setError("最大值自身比"+"-"+item.getPartitionType());
                warnResultBean.setCompareValue(null);
                warnResultBean.setScope(item.getSelf());
                return  warnResultBean;
            };
        }
        return  warnResultBean;
    }
    private WarnResultBean  sumNumCompare(ColumnRuleBean item,String partitions,Statement stmt,String column,String where,String select,String type){
        int countNow=    ruleRunService.selectLengthOrNum(item.getDatabaseName(),item.getTableName(),stmt,select,partitions,where
                ,FinalVar.MONTH.equals(item.getPartitionType())? DateFormatSafe.getMonth(item.getCheckDay()):DateFormatSafe.getDay(item.getCheckDay()),item.getPartitionType());
        List<ItemModuleListBean> itemModuleListBeanList=itemModuleListBeanMapper.selectProByColumnId(item.getId());
        String name=getProName(itemModuleListBeanList);
        WarnResultBean warnResultBean= new WarnResultBean();
        warnResultBean.setValue(String.valueOf(countNow));
        warnResultBean.setFlag(true);
        warnResultBean.setProject(name);
        //环比
        if(!StringUtils.isEmpty(item.gethCompare()) ){
            int dayCount=compareSumNumResult(item,stmt,partitions,1+item.getCheckDay(),select,where);
            Double value = TypeConvert.IntegerConvertDouble((countNow - dayCount))/(dayCount==0?1:dayCount);
            String  wave=PatternRule.numberFormat.format(value*100);
            logger.info("{}环比:id:{},database:{},tableName:{},column:{},scope:{},value:{},compareValue:{},wave:{}",
                    type,item.getNumber(),item.getDatabaseName(),item.getTableName(),column,item.gethCompare(),countNow,dayCount,wave);
            if(!PatternRule.isRule(item.gethCompare(),wave)){

                warnResultBean.setFlag(false);
                switch (item.getPartitionType()){
                    case FinalVar.MONTH:
                        warnResultBean.setError(type+"月环比"+"-"+item.getPartitionType());
                        break;
                    case FinalVar.DAY:
                        warnResultBean.setError(type+"日环比"+"-"+item.getPartitionType());
                        break;
                    default:
                        warnResultBean.setError(type+"日环比"+"-"+item.getPartitionType());
                }
                warnResultBean.setCompareValue(TypeConvert.IntegerConvertString(dayCount));
                warnResultBean.setScope(item.gethCompare());
                excutingRuleService.warnning(item.getAlarmUniqueId(),item.getDatabaseName(),item.getTableName(),item.getContent(),
                        column,warnResultBean.getError(),TypeConvert.IntegerConvertString(countNow),TypeConvert.IntegerConvertString(dayCount),
                        item.gethCompare(),item.getNumber(),name,wave+"%",item.getCheckDay(),item.getPartitionType());
                return  warnResultBean;
            };
        }
        //同比
        if(!StringUtils.isEmpty(item.gettCompare()) ){
            int dayCount=compareSumNumResult(item,stmt,partitions,7+item.getCheckDay(),select,where);
            Double value = TypeConvert.IntegerConvertDouble((countNow - dayCount))/(dayCount==0?1:dayCount);
            String  wave=PatternRule.numberFormat.format(value*100);
            logger.info("{}同比:id:{},database:{},tableName:{},column:{},scope:{},value:{},compareValue:{},wave:{}",
                    type,item.getNumber(),item.getDatabaseName(),item.getTableName(),column,item.gettCompare(),countNow,dayCount,wave);
            if(!PatternRule.isRule(item.gettCompare(), wave)){

                warnResultBean.setFlag(false);
                switch (item.getPartitionType()){
                    case FinalVar.MONTH:
                        warnResultBean.setError(type+"月同比"+"-"+item.getPartitionType());
                        break;
                    case FinalVar.DAY:
                        warnResultBean.setError(type+"周同比"+"-"+item.getPartitionType());
                        break;
                    default:
                        warnResultBean.setError(type+"周同比"+"-"+item.getPartitionType());
                }
                warnResultBean.setCompareValue(TypeConvert.IntegerConvertString(dayCount));
                warnResultBean.setScope(item.gettCompare());
                excutingRuleService.warnning(item.getAlarmUniqueId(),item.getDatabaseName(),item.getTableName(),item.getContent(),column,
                        warnResultBean.getError(),TypeConvert.IntegerConvertString(countNow),TypeConvert.IntegerConvertString(dayCount),
                        item.gettCompare(),item.getNumber(),name,wave+"%",item.getCheckDay(),item.getPartitionType());
                return  warnResultBean;
            };
        }
        if(!StringUtils.isEmpty(item.getSelf())){
            logger.info("{}自身比:id:{},database:{},tableName:{},column:{},scope:{},value:{}",
                    type,item.getNumber(),item.getDatabaseName(),item.getTableName(),column,item.getSelf(),countNow);
            if(!PatternRule.isRule(item.getSelf(),countNow)){
                excutingRuleService.warnning(item.getAlarmUniqueId(),item.getDatabaseName(),item.getTableName(),item.getContent(),column,
                        type+"自身比"+"-"+item.getPartitionType(),TypeConvert.IntegerConvertString(countNow),null,
                        item.getSelf(),item.getNumber(),name,null,item.getCheckDay(),item.getPartitionType());
                warnResultBean.setFlag(false);
                warnResultBean.setError(type+"自身比"+"-"+item.getPartitionType());
                warnResultBean.setCompareValue(null);
                warnResultBean.setScope(item.getSelf());
                return  warnResultBean;
            };
        }
        return  warnResultBean;
    }
    private Integer compareMaxNumResult(ColumnRuleBean item,Map<String,String> dataMap,Statement stmt
            ,String partitions,int dayNum,String column,String select,String where ){
        //获取当天day的count
        Integer dayCount;
            dayCount =  ruleRunService.selectLengthOrNum(item.getDatabaseName(),item.getTableName(),stmt,select,partitions,where,
                    FinalVar.MONTH.equals(item.getPartitionType())? DateFormatSafe.getMonth(dayNum):DateFormatSafe.getDay(dayNum),item.getPartitionType());
        return  dayCount;
    }

    private Integer compareSumNumResult(ColumnRuleBean item,Statement stmt,String partitions,int dayNum,String select,String where ){
        Integer dayCount =  ruleRunService.selectLengthOrNum(item.getDatabaseName(),item.getTableName(),stmt,select,partitions,where,
                FinalVar.MONTH.equals(item.getPartitionType())? DateFormatSafe.getMonth(dayNum):DateFormatSafe.getDay(dayNum),item.getPartitionType());
        return  dayCount;
    }
    private void insertDayDateMax(String dateBase,String tableName,Integer count,String columnName,Integer checkDay,String condition){
        if(!StringUtils.isEmpty(condition)){
            return;
        }
        List<DayDataBean> list=dayDataBeanMapper.selectDayDataToday(dateBase,tableName,columnName,checkDay);
        DayDataBean dayDataBean;
        if(list==null||list.isEmpty()) {
            dayDataBean = new DayDataBean();
            dayDataBean.setMaxLenthNum(count);
            dayDataBean.setDatabaseName(dateBase);
            dayDataBean.setTableName(tableName);
            dayDataBean.setCreateTime(DateFormatSafe.getDay(checkDay));
            dayDataBean.setColumnName(columnName);
            dayDataBeanMapper.insert(dayDataBean);
        }else{
            dayDataBean=list.get(0);
            dayDataBean.setMaxLenthNum(count);
            dayDataBeanMapper.updateByPrimaryKey(dayDataBean);
        }
    }



    private WarnResultBean  minNumCompare(ColumnRuleBean item,String partitions,Statement stmt,String column,String where,String select){
        int countNow=    ruleRunService.selectLengthOrNum(item.getDatabaseName(),item.getTableName(),stmt,select,partitions,where,
                FinalVar.MONTH.equals(item.getPartitionType())? DateFormatSafe.getMonth(item.getCheckDay()):DateFormatSafe.getDay(item.getCheckDay()),item.getPartitionType());
        //获取保存的day_data_db    count
        List<ItemModuleListBean> itemModuleListBeanList=itemModuleListBeanMapper.selectProByColumnId(item.getId());
        String name=getProName(itemModuleListBeanList);
        WarnResultBean warnResultBean= new WarnResultBean();
        warnResultBean.setValue(String.valueOf(countNow));
        warnResultBean.setFlag(true);
        warnResultBean.setProject(name);
        Map<String,String> dataMap= new HashMap<>();
        //环比
        if(!StringUtils.isEmpty(item.gethCompare()) ){
            int dayCount=compareMinNumResult(item,dataMap,stmt,partitions,1+item.getCheckDay(),column,select,where);
            Double value = TypeConvert.IntegerConvertDouble((countNow - dayCount))/(dayCount==0?1:dayCount);
            String  wave=PatternRule.numberFormat.format(value*100);
            logger.info("最小值环比:id:{},database:{},tableName:{},column:{},scope:{},value:{},compareValue:{},wave:{}",
                    item.getNumber(),item.getDatabaseName(),item.getTableName(),column,item.gethCompare(),countNow,dayCount,wave);
            if(!PatternRule.isRule(item.gethCompare(), wave)){
                excutingRuleService.warnning(item.getAlarmUniqueId(),item.getDatabaseName(),item.getTableName(),item.getContent(),column,
                        "最小值环比"+"-"+item.getPartitionType(),TypeConvert.IntegerConvertString(countNow),TypeConvert.IntegerConvertString(dayCount),
                        item.gethCompare(),item.getNumber(),name,wave+"%",item.getCheckDay(),item.getPartitionType());
                warnResultBean.setFlag(false);
                warnResultBean.setError("最小值环比"+"-"+item.getPartitionType());
                warnResultBean.setCompareValue(TypeConvert.IntegerConvertString(dayCount));
                warnResultBean.setScope(item.gethCompare());
                return  warnResultBean;
            };
        }
        //同比
        if(!StringUtils.isEmpty(item.gettCompare()) ){
            int dayCount=compareMinNumResult(item,dataMap,stmt,partitions,7+item.getCheckDay(),column,select,where);
            Double value = TypeConvert.IntegerConvertDouble((countNow - dayCount))/(dayCount==0?1:dayCount);
            String  wave=PatternRule.numberFormat.format(value*100);
            logger.info("最小值同比:id:{},database:{},tableName:{},column:{},scope:{},value:{},compareValue:{},wave:{}",
                    item.getNumber(),item.getDatabaseName(),item.getTableName(),column,item.gettCompare(),countNow,dayCount,wave);
            if(!PatternRule.isRule(item.gettCompare(),wave)){
                excutingRuleService.warnning(item.getAlarmUniqueId(),item.getDatabaseName(),item.getTableName(),item.getContent(),
                        column,"最小值同比"+"-"+item.getPartitionType(),TypeConvert.IntegerConvertString(countNow),TypeConvert.IntegerConvertString(dayCount),
                        item.gettCompare(),item.getNumber(),name,wave+"%",item.getCheckDay(),item.getPartitionType());
                warnResultBean.setFlag(false);
                warnResultBean.setError("最小值同比"+"-"+item.getPartitionType());
                warnResultBean.setCompareValue(TypeConvert.IntegerConvertString(dayCount));
                warnResultBean.setScope(item.gettCompare());
                return  warnResultBean;
            };
        }
        if(!StringUtils.isEmpty(item.getSelf())){
            logger.info("最小值自身比:id:{},database:{},tableName:{},column:{},scope:{},value:{}",
                    item.getNumber(),item.getDatabaseName(),item.getTableName(),column,item.getSelf(),countNow);
            if(!PatternRule.isRule(item.getSelf(),countNow)){
                excutingRuleService.warnning(item.getAlarmUniqueId(),item.getDatabaseName(),item.getTableName(),item.getContent(),
                        column,"最小值自身比"+"-"+item.getPartitionType(),TypeConvert.IntegerConvertString(countNow),null,
                        item.getSelf(),item.getNumber(),name,null,item.getCheckDay(),item.getPartitionType());
                warnResultBean.setFlag(false);
                warnResultBean.setError("最小值自身比"+"-"+item.getPartitionType());
                warnResultBean.setCompareValue(null);
                warnResultBean.setScope(item.getSelf());
                return  warnResultBean;
            };
        }
        return  warnResultBean;
    }
    private Integer compareMinNumResult(ColumnRuleBean item,Map<String,String> dataMap,Statement stmt
            ,String partitions,int dayNum,String column,String select,String where){
        //获取当天day的count
        Integer dayCount;
            dayCount =  ruleRunService.selectLengthOrNum(item.getDatabaseName(),item.getTableName(),stmt,select,partitions,where,
                    FinalVar.MONTH.equals(item.getPartitionType())? DateFormatSafe.getMonth(dayNum):DateFormatSafe.getDay(dayNum),item.getPartitionType());
        return  dayCount;
    }
    private void insertDayDateMin(String dateBase,String tableName,Integer count,String columnName,Integer checkDay,String condition){
        if(!StringUtils.isEmpty(condition)){
            return;
        }
        List<DayDataBean> list=dayDataBeanMapper.selectDayDataToday(dateBase,tableName,columnName,checkDay);
        DayDataBean dayDataBean;
        if(list==null||list.isEmpty()) {
            dayDataBean = new DayDataBean();
            dayDataBean.setMinLenthNum(count);
            dayDataBean.setDatabaseName(dateBase);
            dayDataBean.setTableName(tableName);
            dayDataBean.setCreateTime(DateFormatSafe.getDay(checkDay));
            dayDataBean.setColumnName(columnName);
            dayDataBeanMapper.insert(dayDataBean);
        }else{
            dayDataBean=list.get(0);
            dayDataBean.setMinLenthNum(count);
            dayDataBeanMapper.updateByPrimaryKey(dayDataBean);
        }
    }

    private WarnResultBean  avgNumCompare(ColumnRuleBean item,String partitions,Statement stmt,String column,String where,String select){
        int countNow=      ruleRunService.selectLengthOrNum(item.getDatabaseName(),item.getTableName(),stmt,select,partitions,where
                ,FinalVar.MONTH.equals(item.getPartitionType())? DateFormatSafe.getMonth(item.getCheckDay()):DateFormatSafe.getDay(item.getCheckDay()),item.getPartitionType());
        //获取保存的day_data_db    count
//        insertDayDateAvg(item.getDatabaseName(),item.getTableName(),countNow,item.getColumnName(),item.getCheckDay(),item.getCcondition());
        List<ItemModuleListBean> itemModuleListBeanList=itemModuleListBeanMapper.selectProByColumnId(item.getId());
        String name=getProName(itemModuleListBeanList);
        WarnResultBean warnResultBean= new WarnResultBean();
        warnResultBean.setValue(String.valueOf(countNow));
        warnResultBean.setFlag(true);
        warnResultBean.setProject(name);
        Map<String,String> dataMap= new HashMap<>();
        //环比
        if(!StringUtils.isEmpty(item.gethCompare()) ){
            int dayCount=compareAvgNumResult(item,dataMap,stmt,partitions,1+item.getCheckDay(),column,select,where);
            Double value = TypeConvert.IntegerConvertDouble((countNow - dayCount))/(dayCount==0?1:dayCount);
            String  wave=PatternRule.numberFormat.format(value*100);
            logger.info("平均值环比:id:{},database:{},tableName:{},column:{},scope:{},value:{},compareValue:{},wave:{}",
                    item.getNumber(),item.getDatabaseName(),item.getTableName(),column,item.gethCompare(),countNow,dayCount,wave);
            if(!PatternRule.isRule(item.gethCompare(), wave)){
                excutingRuleService.warnning(item.getAlarmUniqueId(),item.getDatabaseName(),item.getTableName(),item.getContent(),
                        column,"平均值环比"+"-"+item.getPartitionType(),TypeConvert.IntegerConvertString(countNow),TypeConvert.IntegerConvertString(dayCount),
                        item.gethCompare(),item.getNumber(),name,wave+"%",item.getCheckDay(),item.getPartitionType());
                warnResultBean.setFlag(false);
                warnResultBean.setError("平均值环比"+"-"+item.getPartitionType());
                warnResultBean.setCompareValue(TypeConvert.IntegerConvertString(dayCount));
                warnResultBean.setScope(item.gethCompare());
                return warnResultBean;
            };
        }
        //同比
        if(!StringUtils.isEmpty(item.gettCompare()) ){
            int dayCount=compareAvgNumResult(item,dataMap,stmt,partitions,7+item.getCheckDay(),column,select,where);
            Double value = TypeConvert.IntegerConvertDouble((countNow - dayCount))/(dayCount==0?1:dayCount);
            String  wave=PatternRule.numberFormat.format(value*100);
            logger.info("平均值同比:id:{},database:{},tableName:{},column:{},scope:{},value:{},compareValue:{},wave:{}",
                    item.getNumber(),item.getDatabaseName(),item.getTableName(),column,item.gettCompare(),countNow,dayCount,wave);
            if(!PatternRule.isRule(item.gettCompare(), wave)){
                excutingRuleService.warnning(item.getAlarmUniqueId(),item.getDatabaseName(),item.getTableName(),item.getContent(),
                        column,"平均值同比"+"-"+item.getPartitionType(),TypeConvert.IntegerConvertString(countNow),TypeConvert.IntegerConvertString(dayCount),
                        item.gettCompare(),item.getNumber(),name,wave+"%",item.getCheckDay(),item.getPartitionType());
                warnResultBean.setFlag(false);
                warnResultBean.setError("平均值同比"+"-"+item.getPartitionType());
                warnResultBean.setCompareValue(TypeConvert.IntegerConvertString(dayCount));
                warnResultBean.setScope(item.gettCompare());
                return warnResultBean;
            };
        }
        //self
        if(!StringUtils.isEmpty(item.getSelf())){
            logger.info("平均值自身比:id:{},database:{},tableName:{},column:{},scope:{},value:{},compareValue:{}",
                    item.getNumber(),item.getDatabaseName(),item.getTableName(),column,item.getSelf(),countNow);
            if(!PatternRule.isRule(item.getSelf(),countNow)){
                excutingRuleService.warnning(item.getAlarmUniqueId(),item.getDatabaseName(),item.getTableName(),item.getContent()
                        ,column,"平均值自身比"+"-"+item.getPartitionType(),TypeConvert.IntegerConvertString(countNow),null,
                        item.getSelf(),item.getNumber(),name,null,item.getCheckDay(),item.getPartitionType());
                warnResultBean.setFlag(false);
                warnResultBean.setError("平均值自身比"+"-"+item.getPartitionType());
                warnResultBean.setCompareValue(null);
                warnResultBean.setScope(item.getSelf());
                return warnResultBean;
            };
        }
        return  warnResultBean;
    }
    private Integer compareAvgNumResult(ColumnRuleBean item,Map<String,String> dataMap,Statement stmt
            ,String partitions,int dayNum,String column,String select,String where){
        Integer dayCount=null;
            dayCount = ruleRunService.selectLengthOrNum(item.getDatabaseName(),item.getTableName(),stmt,select,partitions,
                    where,FinalVar.MONTH.equals(item.getPartitionType())? DateFormatSafe.getMonth(dayNum):DateFormatSafe.getDay(dayNum),item.getPartitionType());
        return  dayCount;
    }
    private void insertDayDateAvg(String dateBase,String tableName,Integer count,String columnName,Integer checkDay,String condition){
        if(!StringUtils.isEmpty(condition)){
            return;
        }
        List<DayDataBean> list=dayDataBeanMapper.selectDayDataToday(dateBase,tableName,columnName,checkDay);
        DayDataBean dayDataBean;
        if(list==null||list.isEmpty()) {
            dayDataBean = new DayDataBean();
            dayDataBean.setAvgLenthNum(count);
            dayDataBean.setDatabaseName(dateBase);
            dayDataBean.setTableName(tableName);
            dayDataBean.setCreateTime(DateFormatSafe.getDay(checkDay));
            dayDataBean.setColumnName(columnName);
            dayDataBeanMapper.insert(dayDataBean);
        }else{
            dayDataBean=list.get(0);
            dayDataBean.setAvgLenthNum(count);
            dayDataBeanMapper.updateByPrimaryKey(dayDataBean);
        }
    }


    private WarnResultBean  emptyNumCompare(ColumnRuleBean item,String partitions,Statement stmt,String column,String where,String select){
        int countNow=      ruleRunService.selectLengthOrNum(item.getDatabaseName(),item.getTableName(),stmt,select,partitions,where,
                FinalVar.MONTH.equals(item.getPartitionType())? DateFormatSafe.getMonth(item.getCheckDay()):DateFormatSafe.getDay(item.getCheckDay()),item.getPartitionType());
        //获取保存的day_data_db    count
//        insertDayDateEmpty(item.getDatabaseName(),item.getTableName(),countNow,item.getColumnName(),item.getCheckDay(),item.getCcondition());
        List<ItemModuleListBean> itemModuleListBeanList=itemModuleListBeanMapper.selectProByColumnId(item.getId());
        String name=getProName(itemModuleListBeanList);
        WarnResultBean warnResultBean= new WarnResultBean();
        warnResultBean.setValue(String.valueOf(countNow));
        warnResultBean.setFlag(true);
        warnResultBean.setProject(name);
        Map<String,String> dataMap= new HashMap<>();
        //list放在map  key日期  value count值
        //环比
        if(!StringUtils.isEmpty(item.gethCompare()) ){
            int dayCount=compareEmptyNumResult(item,dataMap,stmt,partitions,1+item.getCheckDay(),select,where);
            Double value = TypeConvert.IntegerConvertDouble((countNow - dayCount))/(dayCount==0?1:dayCount);
            String  wave=PatternRule.numberFormat.format(value*100);
            logger.info("空值环比:id:{},database:{},tableName:{},column:{},scope:{},value:{},compareValue:{},wave:{}",
                    item.getNumber(),item.getDatabaseName(),item.getTableName(),column,item.gethCompare(),countNow,dayCount,wave);
            if(!PatternRule.isRule(item.gethCompare(), wave)){
                excutingRuleService.warnning(item.getAlarmUniqueId(),item.getDatabaseName(),item.getTableName(),item.getContent(),
                        column,"空值环比"+"-"+item.getPartitionType(),TypeConvert.IntegerConvertString(countNow),TypeConvert.IntegerConvertString(dayCount),
                        item.gethCompare(),item.getNumber(),name,wave+"%",item.getCheckDay(),item.getPartitionType());
                warnResultBean.setFlag(false);
                warnResultBean.setError("空值环比"+"-"+item.getPartitionType());
                warnResultBean.setCompareValue(TypeConvert.IntegerConvertString(dayCount));
                warnResultBean.setScope(item.gethCompare());
                return  warnResultBean;
            };
        }
        //同比
        if(!StringUtils.isEmpty(item.gettCompare()) ){
            int dayCount=compareEmptyNumResult(item,dataMap,stmt,partitions,7+item.getCheckDay(),select,where);
            Double value = TypeConvert.IntegerConvertDouble((countNow - dayCount))/(dayCount==0?1:dayCount);
            String  wave=PatternRule.numberFormat.format(value*100);
            logger.info("空值同比:id:{},database:{},tableName:{},column:{},scope:{},value:{},compareValue:{},wave:{}",
                    item.getNumber(),item.getDatabaseName(),item.getTableName(),column,item.gettCompare(),countNow,dayCount,wave);
            if(!PatternRule.isRule(item.gettCompare(),wave)){
                excutingRuleService.warnning(item.getAlarmUniqueId(),item.getDatabaseName(),item.getTableName(),item.getContent(),
                        column,"空值同比"+"-"+item.getPartitionType(),TypeConvert.IntegerConvertString(countNow),TypeConvert.IntegerConvertString(dayCount),
                        item.gettCompare(),item.getNumber(),name,wave+"%",item.getCheckDay(),item.getPartitionType());
                warnResultBean.setFlag(false);
                warnResultBean.setError("空值同比"+"-"+item.getPartitionType());
                warnResultBean.setCompareValue(TypeConvert.IntegerConvertString(dayCount));
                warnResultBean.setScope(item.gettCompare());
                return  warnResultBean;
            };
        }
        //self
        if(!StringUtils.isEmpty(item.getSelf())){
            logger.info("空值自身:id:{},database:{},tableName:{},column:{},scope:{},value{},compareValue{}",
                    item.getNumber(),item.getDatabaseName(),item.getTableName(),column,item.getSelf(),countNow);
            if(!PatternRule.isRule(item.getSelf(),countNow)){
                excutingRuleService.warnning(item.getAlarmUniqueId(),item.getDatabaseName(),item.getTableName(),item.getContent(),
                        column,"空值自身比"+"-"+item.getPartitionType(),TypeConvert.IntegerConvertString(countNow),null,
                        item.getSelf(),item.getNumber(),name,null,item.getCheckDay(),item.getPartitionType());
                warnResultBean.setFlag(false);
                warnResultBean.setError("空值自身比"+"-"+item.getPartitionType());
                warnResultBean.setCompareValue(null);
                warnResultBean.setScope(item.getSelf());
                return  warnResultBean;
            };
        }
        return  warnResultBean;
    }
    private Integer compareEmptyNumResult(ColumnRuleBean item,Map<String,String> dataMap,Statement stmt
            ,String partitions,int dayNum,String select,String where){
        Integer dayCount;
            dayCount = ruleRunService.selectLengthOrNum(item.getDatabaseName(),item.getTableName(),stmt,select,partitions,where,
                    FinalVar.MONTH.equals(item.getPartitionType())? DateFormatSafe.getMonth(dayNum):DateFormatSafe.getDay(dayNum),item.getPartitionType());
        return  dayCount;
    }

    private void insertDayDateEmpty(String dateBase,String tableName,Integer count,String columnName,Integer checkDay,String condition){
        if(!StringUtils.isEmpty(condition)){
            return;
        }
        List<DayDataBean> list=dayDataBeanMapper.selectDayDataToday(dateBase,tableName,columnName,checkDay);
        DayDataBean dayDataBean;
        if(list==null||list.isEmpty()) {
            dayDataBean = new DayDataBean();
            dayDataBean.setZeroNum(count);
            dayDataBean.setDatabaseName(dateBase);
            dayDataBean.setTableName(tableName);
            dayDataBean.setCreateTime(DateFormatSafe.getDay(checkDay));
            dayDataBean.setColumnName(columnName);
            dayDataBeanMapper.insert(dayDataBean);
        }else{
            dayDataBean=list.get(0);
            dayDataBean.setZeroNum(count);
            dayDataBeanMapper.updateByPrimaryKey(dayDataBean);
        }
    }


    private WarnResultBean  likeNumCompare(ColumnRuleBean item,String partitions,Statement stmt,String column,String where){
        int countNow=      ruleRunService.selectLike(item.getDatabaseName(),item.getTableName(),stmt,column,partitions,where
                , FinalVar.MONTH.equals(item.getPartitionType())? DateFormatSafe.getMonth(item.getCheckDay()):DateFormatSafe.getDay(item.getCheckDay()),item.getPartitionType());
        //获取保存的day_data_db    count
        List<ItemModuleListBean> itemModuleListBeanList=itemModuleListBeanMapper.selectProByColumnId(item.getId());
        String name=getProName(itemModuleListBeanList);
        WarnResultBean warnResultBean= new WarnResultBean();
        warnResultBean.setValue(String.valueOf(countNow));
        warnResultBean.setFlag(true);
        warnResultBean.setProject(name);
        Map<String,String> dataMap= new HashMap<>();
        //list放在map  key日期  value count值
        //环比
        if(!StringUtils.isEmpty(item.gethCompare()) ){
            int dayCount=compareLikeNumResult(item,dataMap,stmt,partitions,1+item.getCheckDay(),column,where);
            Double value = TypeConvert.IntegerConvertDouble((countNow - dayCount))/(dayCount==0?1:dayCount);
            String  wave=PatternRule.numberFormat.format(value*100);
            logger.info("特殊字符环比:id:{},database:{},tableName:{},column:{},scope:{},value:{},compareValue:{},wave:{}",
                    item.getNumber(),item.getDatabaseName(),item.getTableName(),column,item.gethCompare(),countNow,dayCount,wave);
            if(!PatternRule.isRule(item.gethCompare(),wave)){
                excutingRuleService.warnning(item.getAlarmUniqueId(),item.getDatabaseName(),item.getTableName(),item.getContent(),
                        column,"特殊字符环比"+"-"+item.getPartitionType(),TypeConvert.IntegerConvertString(countNow),TypeConvert.IntegerConvertString(dayCount),
                        item.gethCompare(),item.getNumber(),name,wave+"%",item.getCheckDay(),item.getPartitionType());
                warnResultBean.setFlag(false);
                warnResultBean.setError("特殊字符环比"+"-"+item.getPartitionType());
                warnResultBean.setCompareValue(TypeConvert.IntegerConvertString(dayCount));
                warnResultBean.setScope(item.gethCompare());
                return  warnResultBean;
            };
        }
        //同比
        if(!StringUtils.isEmpty(item.gettCompare()) ){
            int dayCount=compareLikeNumResult(item,dataMap,stmt,partitions,7+item.getCheckDay(),column,where);
            Double value = TypeConvert.IntegerConvertDouble((countNow - dayCount))/(dayCount==0?1:dayCount);
            String  wave=PatternRule.numberFormat.format(value*100);
            logger.info("特殊字符同比:id:{},database:{},tableName:{},column:{},scope:{},value:{},compareValue:{},wave:{}",
                    item.getNumber(),item.getDatabaseName(),item.getTableName(),column,item.gettCompare(),countNow,dayCount,wave);
            if(!PatternRule.isRule(item.gettCompare(), wave)){
                excutingRuleService.warnning(item.getAlarmUniqueId(),item.getDatabaseName(),item.getTableName(),item.getContent(),
                        column,"特殊字符同比"+"-"+item.getPartitionType(),TypeConvert.IntegerConvertString(countNow),TypeConvert.IntegerConvertString(dayCount),
                        item.gettCompare(),item.getNumber(),name,wave+"%",item.getCheckDay(),item.getPartitionType());
                warnResultBean.setFlag(false);
                warnResultBean.setError("特殊字符同比"+"-"+item.getPartitionType());
                warnResultBean.setCompareValue(TypeConvert.IntegerConvertString(dayCount));
                warnResultBean.setScope(item.gettCompare());
                return  warnResultBean;
            };
        }
        //self
        if(!StringUtils.isEmpty(item.getSelf())){
            logger.info("特殊字符自身比:id:{},database:{},tableName:{},column:{},scope:{},value:{},compareValue:{}",
                    item.getNumber(),item.getDatabaseName(),item.getTableName(),column,item.getSelf(),countNow);
            if(!PatternRule.isRule(item.getSelf(),countNow)){
                excutingRuleService.warnning(item.getAlarmUniqueId(),item.getDatabaseName(),item.getTableName(),item.getContent(),
                        column,"特殊字符自身比"+"-"+item.getPartitionType(),TypeConvert.IntegerConvertString(countNow),null,
                        item.getSelf(),item.getNumber(),name,null,item.getCheckDay(),item.getPartitionType());
                warnResultBean.setFlag(false);
                warnResultBean.setError("特殊字符自身比"+"-"+item.getPartitionType());
                warnResultBean.setCompareValue(null);
                warnResultBean.setScope(item.getSelf());
                return  warnResultBean;
            };
        }
        return  warnResultBean;
    }
    private Integer compareLikeNumResult(ColumnRuleBean item,Map<String,String> dataMap,Statement stmt
            ,String partitions,int dayNum,String column,String where){
        //获取当天day的count
        Integer dayCount;
            dayCount = ruleRunService.selectLike(item.getDatabaseName(),item.getTableName(),stmt,column,partitions,where,
                    FinalVar.MONTH.equals(item.getPartitionType())? DateFormatSafe.getMonth(dayNum):DateFormatSafe.getDay(dayNum),item.getPartitionType());
        return  dayCount;
    }

    private void insertDayDateLike(String dateBase,String tableName,Integer count,String columnName,Integer checkDay,String condition){
        if(!StringUtils.isEmpty(condition)){
            return;
        }
        List<DayDataBean> list=dayDataBeanMapper.selectDayDataToday(dateBase,tableName,columnName,checkDay);
        DayDataBean dayDataBean;
        if(list==null||list.isEmpty()) {
            dayDataBean = new DayDataBean();
            dayDataBean.setSpecialNum(count);
            dayDataBean.setDatabaseName(dateBase);
            dayDataBean.setTableName(tableName);
            dayDataBean.setCreateTime(DateFormatSafe.getDay(checkDay));
            dayDataBean.setColumnName(columnName);
            dayDataBeanMapper.insert(dayDataBean);
        }else{
            dayDataBean=list.get(0);
            dayDataBean.setSpecialNum(count);
            dayDataBeanMapper.updateByPrimaryKey(dayDataBean);
        }
    }

    private WarnResultBean  repeatNumCompare(ColumnRuleBean item,String partitions,Statement stmt,String column,String where){
        int countNow=    ruleRunService.selectRepeatNum(item.getDatabaseName(),item.getTableName(),stmt,column,partitions,where
                , FinalVar.MONTH.equals(item.getPartitionType())? DateFormatSafe.getMonth(item.getCheckDay()):DateFormatSafe.getDay(item.getCheckDay()),item.getPartitionType());
        List<ItemModuleListBean> itemModuleListBeanList=itemModuleListBeanMapper.selectProByColumnId(item.getId());
        String name=getProName(itemModuleListBeanList);
        WarnResultBean warnResultBean= new WarnResultBean();
        warnResultBean.setValue(String.valueOf(countNow));
        warnResultBean.setFlag(true);
        warnResultBean.setProject(name);
        Map<String,String> dataMap= new HashMap<>();
        //环比
        if(!StringUtils.isEmpty(item.gethCompare())){
            int dayCount=compareRepeatNumResult(item,dataMap,stmt,partitions,1+item.getCheckDay(),column,where);
            Double value = TypeConvert.IntegerConvertDouble((countNow - dayCount))/(dayCount==0?1:dayCount);
            String  wave=PatternRule.numberFormat.format(value*100);
            logger.info("重复值环比:id:{},database:{},tableName:{},column:{},scope:{},value:{},compareValue:{},wave:{}",
                    item.getNumber(),item.getDatabaseName(),item.getTableName(),column,item.gethCompare(),countNow,dayCount,wave);
            if(!PatternRule.isRule(item.gethCompare(), wave)){
                excutingRuleService.warnning(item.getAlarmUniqueId(),item.getDatabaseName(),item.getTableName(),item.getContent(),
                        column,"重复值环比"+"-"+item.getPartitionType(),TypeConvert.IntegerConvertString(countNow),TypeConvert.IntegerConvertString(dayCount),
                        item.gethCompare(),item.getNumber(),name,wave+"%",item.getCheckDay(),item.getPartitionType());
                warnResultBean.setFlag(false);
                warnResultBean.setError("重复值环比"+"-"+item.getPartitionType());
                warnResultBean.setCompareValue(TypeConvert.IntegerConvertString(dayCount));
                warnResultBean.setScope(item.gethCompare());
                return warnResultBean;
            };
        }
        //同比
        if(!StringUtils.isEmpty(item.gettCompare()) ){
            int dayCount=compareRepeatNumResult(item,dataMap,stmt,partitions,7+item.getCheckDay(),column,where);
            Double value = TypeConvert.IntegerConvertDouble((countNow - dayCount))/(dayCount==0?1:dayCount);
            String  wave=PatternRule.numberFormat.format(value*100);
            logger.info("重复值同比:id:{},database:{},tableName:{},column:{},scope:{},value:{},compareValue:{},wave:{}",
                    item.getNumber(),item.getDatabaseName(),item.getTableName(),column,item.gettCompare(),countNow,dayCount,wave);
            if(!PatternRule.isRule(item.gettCompare(),wave)){
                excutingRuleService.warnning(item.getAlarmUniqueId(),item.getDatabaseName(),item.getTableName(),item.getContent(),
                        column,"重复值同比"+"-"+item.getPartitionType(),TypeConvert.IntegerConvertString(countNow),TypeConvert.IntegerConvertString(dayCount),
                        item.gettCompare(),item.getNumber(),name,wave+"%",item.getCheckDay(),item.getPartitionType());
                warnResultBean.setFlag(false);
                warnResultBean.setError("重复值同比"+"-"+item.getPartitionType());
                warnResultBean.setCompareValue(TypeConvert.IntegerConvertString(dayCount));
                warnResultBean.setScope(item.gettCompare());
                return warnResultBean;
            };
        }
        //self
        if(!StringUtils.isEmpty(item.getSelf())){
            logger.info("重复值自身:id:{},database:{},tableName:{},column:{},scope:{},value:{},compareValue:{}",
                    item.getNumber(),item.getDatabaseName(),item.getTableName(),column,item.getSelf(),countNow);
            if(!PatternRule.isRule(item.getSelf(),countNow)){
                excutingRuleService.warnning(item.getAlarmUniqueId(),item.getDatabaseName(),item.getTableName(),item.getContent(),
                        column,"重复值自身比"+"-"+item.getPartitionType(),TypeConvert.IntegerConvertString(countNow),null,
                        item.getSelf(),item.getNumber(),name,null,item.getCheckDay(),item.getPartitionType());
                warnResultBean.setFlag(false);
                warnResultBean.setError("重复值自身比"+"-"+item.getPartitionType());
                warnResultBean.setCompareValue(null);
                warnResultBean.setScope(item.getSelf());
                return warnResultBean;
            };
        }
        return warnResultBean;
    }
    private Integer compareRepeatNumResult(ColumnRuleBean item,Map<String,String> dataMap,Statement stmt
            ,String partitions,int dayNum,String column,String where){
        Integer dayCount ;
            dayCount = ruleRunService.selectRepeatNum(item.getDatabaseName(),item.getTableName(),stmt,column,partitions,where,
                    FinalVar.MONTH.equals(item.getPartitionType())? DateFormatSafe.getMonth(dayNum):DateFormatSafe.getDay(dayNum),item.getPartitionType());
        return  dayCount;
    }

    private void insertDayDateRepeat(String dateBase,String tableName,Integer count,String columnName,Integer checkDay,String condition){
        if(!StringUtils.isEmpty(condition)){
            return;
        }
        List<DayDataBean> list=dayDataBeanMapper.selectDayDataToday(dateBase,tableName,columnName,checkDay);
        DayDataBean dayDataBean;
        if(list==null||list.isEmpty()) {
            dayDataBean = new DayDataBean();
            dayDataBean.setAllRepeat(count);
            dayDataBean.setDatabaseName(dateBase);
            dayDataBean.setTableName(tableName);
            dayDataBean.setCreateTime(DateFormatSafe.getDay(checkDay));
            dayDataBean.setColumnName(columnName);
            dayDataBeanMapper.insert(dayDataBean);
        }else{
            dayDataBean=list.get(0);
            dayDataBean.setAllRepeat(count);
            dayDataBeanMapper.updateByPrimaryKey(dayDataBean);
        }
    }

    private void insertResultLog(String leader,String level,String type,String status
            ,WarnResultBean bean,String dataBaseName,String tableName,String columnName,String id,String content,String partitionType){
        RuleRunningLogBean ruleRunningLogBean=new RuleRunningLogBean();
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
        ruleRunningLogBean.setColumnName(columnName);
        ruleRunningLogBean.setIsWarnning(bean.getError());
        ruleRunningLogBean.setRuleId(id);
        ruleRunningLogBean.setProject(bean.getProject());
        ruleRunningLogBean.setContent(content);
        ruleRunningLogBean.setPartitionType(partitionType);
        ruleRunningLogBeanMapper.insert(ruleRunningLogBean);
    }

    private boolean isCheck(ColumnRuleBean item,String type,String columnName){
        List<RuleCheckBean>  logBeanList = ruleCheckBeanMapper.getRuleCheckByRuleId(item.getId(),item.getCheckDay());
        if(logBeanList!=null&&!logBeanList.isEmpty()){
            logger.info("database:{},tableName:{},type:{}-已经检查  break",item.getDatabaseName()
                    ,item.getTableName(),type);
            return true;
        }else{
            RuleCheckBean  ruleCheckBean = new RuleCheckBean();
            ruleCheckBean.setDatabaseName(item.getDatabaseName());
            ruleCheckBean.setTableName(item.getTableName());
            ruleCheckBean.setColumnName(columnName);
            ruleCheckBean.setType(type);
            ruleCheckBean.setCreateTime(DateFormatSafe.getDay(item.getCheckDay()));
            ruleCheckBean.setRuleId(item.getId());
            ruleCheckBeanMapper.insert(ruleCheckBean);
        }
        return  false;
    }

    private void addRuleCheck(ColumnRuleBean item, String type, String columnName) {
        RuleCheckBean  ruleCheckBean = new RuleCheckBean();
        ruleCheckBean.setType(type);
        ruleCheckBean.setDatabaseName(item.getDatabaseName());
        ruleCheckBean.setTableName(item.getTableName());
        ruleCheckBean.setColumnName(columnName);
        ruleCheckBean.setCreateTime(DateFormatSafe.getDay(item.getCheckDay()));
        ruleCheckBean.setRuleId(item.getId());
        ruleCheckBeanMapper.insert(ruleCheckBean);
    }

    private boolean ruleAlreadyRuned(ColumnRuleBean item) {
        Date now = new Date();
        String checkDay = null;
        if("0".equals(item.getCheckDay())) {
            checkDay = DateUtils.format(now);
        } else {
            checkDay = DateUtils.getAroundDate(now, ~(Integer.valueOf(item.getCheckDay())) + 1, DateUtils.DATE_PATTERN);
        }

        List<RuleCheckBean> list = ruleCheckBeanMapper.getHistoryByRuleidAndDate(item.getId(), checkDay);
        if(list.isEmpty()) {
            return false;
        }
        return true;
    }

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
}