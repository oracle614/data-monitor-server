package com.yiche.controller;

import com.yiche.service.ExcutingRuleService;
import com.yiche.service.impl.ExcutingRuleServiceImpl;

public class test {

    public static void main(String args[]) {
//        boolean resulot=PatternRule.isRule("!{0}","2");
//        boolean resulot2=PatternRule.isPass("!{0}",0);
//        System.out.println(resulot);
//        System.out.println(resulot2);
//        boolean  flag= PatternRule.isRule("(5,5)","9999999999999999");
//        String a="2018-06-08";
//        String b="2018-06-10";
//        String c="2018-07-01";
//
//        System.out.println(   a.compareTo(b));
//        System.out.println(   b.compareTo(a));
//        System.out.println(   a.compareTo(c));
//        System.out.println(   c.compareTo(a));
//        System.out.println(   c.compareTo(c));

        ExcutingRuleService excutingRuleService = new ExcutingRuleServiceImpl();
        boolean flag=excutingRuleService.isPartitionReady("pt=2018-06-09",1,"日");
        System.out.print(flag);
    }
}

