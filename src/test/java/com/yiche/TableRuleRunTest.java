package com.yiche;

import com.yiche.bean.RuleCheckBean;
import com.yiche.utils.DateUtils;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by weiyongxu on 2018/7/18.
 */
public class TableRuleRunTest {
    public static void main(String[] args) {
       /* ArrayList<String> list = new ArrayList<String>();
        list.add("111");
        list.add("22");
        list.add("444");
        list.add("555");
        test1(list);
        for(String s : list) {
            System.out.println(s);
        }*/

        /*Date now = new Date();
        Long nowMillis = now.getTime() / 1000;
        String checkDayStr = DateUtils.format(now) + " " + "3:02:00";
        String needRunTimeMillis = DateUtils.date2TimeStamp(checkDayStr, DateUtils.DATE_TIME_PATTERN);
        if(StringUtils.isEmpty(needRunTimeMillis)) {
            // todo add alarm
            System.out.println("error");
            return;
        }
        if(nowMillis >= Long.valueOf(needRunTimeMillis)) {
            System.out.println("need run");
            return;
        }
        System.out.println("not need run");
        return;*/
       /* String i = "0";
        Date now = new Date();
        String checkDay = null;
        if("0".equals(i)) {
            checkDay = DateUtils.format(now);
        } else {
            checkDay = DateUtils.getAroundDate(now, ~(Integer.valueOf(i)) + 1, DateUtils.DATE_PATTERN);
        }

        System.out.println(checkDay);*/
    }

    private static void  test1(List<String> list) {
        list.remove(2);
    }
}
