package com.yiche;
import com.google.common.collect.Range;
import com.yiche.bean.IndexProMail;
import com.yiche.service.*;
import com.yiche.service.impl.AlarmChannelService;
import com.yiche.service.impl.ColumnRuleServiceImpl;
import com.yiche.utils.FinalVar;
import com.yiche.utils.NoticeBuilder;
import org.apache.commons.lang.StringUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.yiche.service.NotifySysService;

@RunWith(SpringRunner.class)
@SpringBootTest
@ActiveProfiles("dev")

public class DataReportTest {
    @Autowired
    ExcutingRuleService excutingRuleService;

    @Autowired
    SyncRuleExecTimeService syncRuleExecTimeService;

    @Autowired
    NotifySysService  notifySysService;

    @Test
    public void ReportRunTest() {




        excutingRuleService.warningWhenExecRuleException("异常报警id","内容mess");

        excutingRuleService.runIndexPro(FinalVar.DAY);

//        excutingRuleService.runDataWarehourse(FinalVar.DAY,1,"数仓");
//
//        excutingRuleService.runPro(FinalVar.DAY,1,"渠道线索");
//
//        excutingRuleService.runDistributorPro(FinalVar.DAY,1,"经销商数据中心");
    }
}
