package com.yiche;

import com.google.common.collect.Range;

import com.yiche.utils.DateFormatSafe;
import com.yiche.utils.FinalVar;
import com.yiche.utils.PatternRule;
import org.apache.commons.lang.StringUtils;

import com.yiche.utils.PatternRule;

import org.junit.Test;

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

    private static void test1(List<String> list) {
        list.remove(2);
    }

    @Test
    public void compare() {

//        String s = PatternRule.numberFormat.format(0.195 * 100);
//        String q = "295";
//        boolean re = PatternRule.isRule("(-20,20)", q);
//        System.out.println(s + "     " + re);
//        int dayCount = 933333;
//        int countNow = 802333;
//        double devition = countNow - dayCount;
//        Double value = devition / (dayCount == 0 ? 1 : dayCount);
//        System.out.println("a" + value);
//        Double e = value * 100;
//        String RR = String.format("%.2f", value *100);
//        System.out.println("RR" + RR);
//
//
//        boolean RERER = PatternRule.isRule("(-5,5]", RR);
//
//        System.out.println("RERE" + RERER);
//
//
//        String qw = String.format("%.2f", 99.998);
//
//        System.out.println("qq" + qw);


    }

    @Test
    public void partition() {
        String a = "";
        Integer b = 2;
        String c = "";
        int day = 0;
        System.out.println(DateFormatSafe.format(DateFormatSafe.getDay(day)));
        System.out.println( DateFormatSafe.formatSign(DateFormatSafe.getDay(day)));


        System.out.println(DateFormatSafe.formatMonth(DateFormatSafe.getMonth(day)));

        System.out.println( DateFormatSafe.getDay(day));

    }


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

//
//        String s = PatternRule.numberFormat.format(0.195 * 100);
//        String q = "295";
//        boolean re = PatternRule.isRule("(-20,20)", q);
//        System.out.println(s + "     " + re);
//        int dayCount = 933333;
//        int countNow = 802333;
//        double devition = countNow - dayCount;
//        Double value = devition / (dayCount == 0 ? 1 : dayCount);
//        System.out.println("a" + value);
//        Double e = value * 100;
//        String RR = String.format("%.2f", value *100);
//        System.out.println("RR" + RR);
//
//
//        boolean RERER = PatternRule.isRule("(-5,5]", RR);
//
//        System.out.println("RERE" + RERER);
//
//
//        String qw = String.format("%.2f", 99.998);
//
//        System.out.println("qq" + qw);


    @Test
    public void p1() {


     String A =   DateFormatSafe.format(DateFormatSafe.getDay(1));
System.out.println(A);


     String b=   DateFormatSafe.formatSign(DateFormatSafe.getDay(1));
System.out.println(b);
    }



}