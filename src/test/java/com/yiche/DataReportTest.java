package com.yiche;

import com.yiche.service.ExcutingRuleService;
import com.yiche.service.NotifySysService;
import com.yiche.service.SyncRuleExecTimeService;
import com.yiche.utils.FinalVar;
import com.yiche.utils.PatternRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

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




//        excutingRuleService.warningWhenExecRuleException("异常报警id","内容mess");
//
//        excutingRuleService.runIndexPro(FinalVar.DAY);
//
////        excutingRuleService.runDataWarehourse(FinalVar.DAY,1,"数仓");
////
////        excutingRuleService.runPro(FinalVar.DAY,1,"渠道线索");
////
////        excutingRuleService.runDistributorPro(FinalVar.DAY,1,"经销商数据中心");
        double countAll = 148 + 140;
        double passCount =148+139;
        double exceptionDevition = passCount / countAll;
        Double value = exceptionDevition * 100;
        System.out.println(PatternRule.numberFormat.format(Math.floor(value)));
    }
}
