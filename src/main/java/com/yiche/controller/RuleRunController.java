package com.yiche.controller;


import com.yiche.bean.*;
import com.yiche.bussiness.MetadataBussiness;
import com.yiche.bussiness.PlatformBussiness;
import com.yiche.dao.DayDataBeanMapper;
import com.yiche.dao.ItemModuleListBeanMapper;
import com.yiche.dao.RuleRunningLogBeanMapper;
import com.yiche.quartz.QuartzTask;
import com.yiche.service.*;
import com.yiche.utils.DateFormatSafe;
import com.yiche.utils.FinalVar;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
public class RuleRunController {


    private final Logger logger = LoggerFactory.getLogger(RuleRunController.class);


    @Autowired
    RuleRunService  ruleRunService;
    @Autowired
    ExcutingRuleService excutingRuleService;

    @Autowired
    NotifySysService notifySysService;

    @Autowired
    TableRuleService tableRuleService;

    @Autowired
    ColumnRuleService columnRuleService;

    @Autowired
    DayDataBeanMapper dayDataBeanMapper;

    @Autowired
    RuleRunningLogBeanMapper ruleRunningLogBeanMapper;
    @Autowired
    PlatformBussiness platformBussiness;

    @Autowired
    MetadataBussiness metadataBussiness;

    @Autowired
    private SyncRuleExecTimeService syncRuleExecTimeService;

    /*@Autowired
    ConnectHiveService connectHiveService;*/

    @Autowired
    QuartzTask quartzTask;

    @Autowired
    ItemModuleListBeanMapper itemModuleListBeanMapper;
    @Value("${hive.driverName}")
    private  String driverName  ;
    @Value("${hive.url}")
    private  String url ;
    @Value("${hive.user}")
    private  String user ;
    @Value("${hive.password}")
    private  String password;


    @RequestMapping("/item")
    @ResponseBody
    public String item (){
        if(!DateFormatSafe.isFirstDay()) {
            logger.info("执行指数项目调度任务-日：{}", new Date());
            excutingRuleService.runIndexPro(FinalVar.DAY);
        }else {
            logger.info("执行指数项目调度任务-日和月：{}", new Date());
            excutingRuleService.runIndexPro(null);
        }
        return  "wasdwa";
    }

    @RequestMapping("/platform")
    @ResponseBody
    public String dataWarehourse (){
        try {
            quartzTask.platformIndex();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return  "wasdwa";
    }

    @RequestMapping("/distributor")
    @ResponseBody
    public String distributor (){
        try {
            quartzTask.distributorDataCenter();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return  "wasdwaa";
    }



    @RequestMapping("/datehourse")
    @ResponseBody
    public String datehourse (){
        try {
            quartzTask.dataWarehourseRule();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return  "wasdwa";
    }

    @RequestMapping("/run")
    @ResponseBody
    public void run() {
        excutingRuleService.runTableRule();
        excutingRuleService.runColumnRule();
    }


    @RequestMapping("/run_table_rule")
    @ResponseBody
    public void runTalbeRun() {
        excutingRuleService.runTableRule();
    }

    @RequestMapping("/run_column_rule")
    @ResponseBody
    public void runColumneRun() {
        excutingRuleService.runColumnRule();
    }

    @RequestMapping("/run_end_time")
    @ResponseBody
    public void runEndTime() {
        excutingRuleService.getTimeRuleNoPass();
    }

    @RequestMapping("/update_exec_time")
    @ResponseBody
    public void updateExecTime() {
        syncRuleExecTimeService.updateRuleExecTime();
    }

    @RequestMapping("/notify")
    @ResponseBody
    public String warnning(){

        String Alarm_uniqueid="22fwm8nj";
        excutingRuleService.warnning(Alarm_uniqueid,"dateBase"
                , "tableName", "content", "column", "error", "value"
                , "valueCompare", "scope","id","project","waveScope",1,"日");
//        excutingRuleService.sqlWarnning(reciever,"asd","asd","asdasd");
        return  "asd";
    }

    @RequestMapping("/resultLog")
    @ResponseBody
    public ResponseEntity<Map> getResultLog(@RequestParam("index") String index, @RequestParam("limit") String limit){
        List<RuleRunningLogBean>  list= tableRuleService.getResultLogByPage(index,limit);
        Integer count =excutingRuleService.getResultLogAllCount();
        Map<String,Object>  resultMap= new HashMap<>();
        resultMap.put("count",count);
        resultMap.put("list",list);
        resultMap.put("count",count);
        resultMap.put("list",list);
        ResponseEntity<Map> result=new ResponseEntity<Map>(resultMap, HttpStatus.OK);
        return  result;
    }

     @RequestMapping(value="/resultLog/byTable")
     @ResponseBody
    public ResponseEntity<Map> getResultLogByTable(@RequestParam("dataBase") String dataBase
             , @RequestParam("tableName") String tableName,@RequestParam("index") String index,@RequestParam("limit") String limit){
        List<RuleRunningLogBean>  list= excutingRuleService.getDataByDataBaseAndTable(dataBase,tableName
                ,Integer.valueOf(index),Integer.valueOf(limit));
        int count= excutingRuleService.getResultLogCount(dataBase,tableName);
        Map<String,Object>  resultMap= new HashMap<>();
         resultMap.put("count",count);
         resultMap.put("list",list);
        ResponseEntity<Map> result=new ResponseEntity<Map>(resultMap, HttpStatus.OK);
        return  result;
    }

    @RequestMapping(value="/run/table")
    @ResponseBody
    public ResponseEntity<Map> tableRuleRun(@RequestBody TableRuleBean tableRuleBean ){
        System.out.println("+++___++++"+tableRuleBean.getDatabaseName());
        System.out.println("+++___++++"+tableRuleBean.getCreateTime());

          String message=tableRuleService.tableRuleRun(tableRuleBean);
//        String message = "ssdd";
        Map<String,Object>  resultMap= new HashMap<>();
        resultMap.put("status","success");
        resultMap.put("message",message);
        ResponseEntity<Map> result=new ResponseEntity<Map>(resultMap, HttpStatus.OK);
        return  result;
    }

    @RequestMapping(value="/run/column")
    @ResponseBody
    public ResponseEntity<Map> columnRuleRun(@RequestBody ColumnRuleBean columnRuleBean ){
        String message=columnRuleService.columnRuleRun(columnRuleBean);
        Map<String,Object>  resultMap= new HashMap<>();
        resultMap.put("status","success");
        resultMap.put("message",message);
        ResponseEntity<Map> result=new ResponseEntity<Map>(resultMap, HttpStatus.OK);
        return  result;
    }
}