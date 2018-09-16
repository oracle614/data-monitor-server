package com.yiche;

import com.google.common.collect.Range;
import com.yiche.bean.ColumnRuleBean;
import com.yiche.bean.IndexProMail;
import com.yiche.quartz.QuartzTask;
import com.yiche.service.*;
import com.yiche.service.impl.AlarmChannelService;
import com.yiche.service.impl.ColumnRuleServiceImpl;
import com.yiche.utils.FinalVar;
import com.yiche.utils.NoticeBuilder;
import org.apache.commons.lang.StringUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.junit4.SpringRunner;


import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.text.MessageFormat;
import java.util.*;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.yiche.service.NotifySysService;


@RunWith(SpringRunner.class)
@SpringBootTest
@ActiveProfiles("dev")
public class QcApplicationTests {

    @Autowired
    ExcutingRuleService excutingRuleService;

    @Autowired
    SyncRuleExecTimeService syncRuleExecTimeService;

    @Autowired
    NotifySysService  notifySysService;

    @Autowired
    ColumnRuleService columnRuleService;

    @Test
    public void tableRuleRunTest() {
        ;
        excutingRuleService.runTableRule();
    }

    @Test
    public void columnRuleRunTest() {
        excutingRuleService.runColumnRule();
    }

    @Test
    public void syncRuleExecTimeTest() {
        //syncRuleExecTimeService.updateRuleExecTime();
        syncRuleExecTimeService.checkRuleExecTime();
    }

    @Test
    public void runDistributorProTest() {
        String reciever = "zhaoguanchen@yiche.com|15342,weiyx@yiche.com|14741";
        excutingRuleService.runDistributorPro(FinalVar.DAY, 1, FinalVar.DISTRIBUTORDATACENTER);
       // excutingRuleService.runPro(FinalVar.DAY,1,"指数");
       // excutingRuleService.runIndexPro(FinalVar.DAY);
    }

    @Test
    public void notifyBuilder() {
        NoticeBuilder noticeBuilder = NoticeBuilder.createNoticeSend();
        noticeBuilder.setGroupUniqueId("1");
        noticeBuilder.setDataContent("2");
        try {
            boolean res = noticeBuilder.sendNotice();
            System.out.println("啊啊啊啊" + res);
        } catch (Exception e) {
            System.out.println("cuole");
        }

    }


//    weixinAlarm
    @Test
    public void warnningTest() {
        excutingRuleService.warnning("22fwm8nj", "测试", "测试","测试","测试","测试","测试","测试","测试",
                "测试","测试","测试",1,"测试");
    }

//    @Test
//    public void qzTest() {
////        excutingRuleService.warnning("22fwm8nj", "211", "12","21","222","11",",","21","21",
////                "1221","完成时间","未就绪",1,"二大");
//
//        QuartzTask quartzTask = new QuartzTask();
//        try {
//            quartzTask.distributorDataCenter();
//        }catch (Exception e){
//
//        }
//
//    }
 // 4e6h2f9m

    @Test
    public void exeTest() {
    Vector<ColumnRuleBean> vector=columnRuleService.getColumnRule();
        System.out.println(vector);
        columnRuleService.columnRuleRun();

    }

}
//    xxu3zld3