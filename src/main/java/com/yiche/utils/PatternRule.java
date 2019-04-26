package com.yiche.utils;

import com.google.common.collect.Range;

import java.text.NumberFormat;
import java.util.regex.Matcher;

public class PatternRule {

    public static NumberFormat numberFormat = NumberFormat.getInstance();

    static {
        numberFormat.setMaximumFractionDigits(0);
    }

    public static boolean match(String pattern, String str) {
        java.util.regex.Pattern r = java.util.regex.Pattern.compile(pattern);
        Matcher m = r.matcher(str);
        return m.matches();
    }


    public static boolean isRule(String str, Integer value) {
        str = str.replace("%", "");
        if (str.contains("&")) {
            String[] ruleStr = str.split("&");
            return isPass(ruleStr[0], value) && isPass(ruleStr[1], value);
        } else if (str.contains("|")) {
            String[] ruleStr = str.split("|");
            return isPass(ruleStr[0], value) || isPass(ruleStr[1], value);
        }
        return isPass(str, value);
    }
    public static boolean isRule(String str, Long value) {
        str = str.replace("%", "");
        if (str.contains("&")) {
            String[] ruleStr = str.split("&");
            return isPass(ruleStr[0], value) && isPass(ruleStr[1], value);
        } else if (str.contains("|")) {
            String[] ruleStr = str.split("|");
            return isPass(ruleStr[0], value) || isPass(ruleStr[1], value);
        }
        return isPass(str, value);
    }
    ;

    public static boolean isRule(String str, String value) {
        str = str.replace("%", "");
        value = value.replace(",", "");
        Double num;
        if (value.length() > 6) {
            num = Double.MAX_VALUE;
        } else {
            num = TypeConvert.StringConvertDouble(value);
        }
        if (str.contains("&")) {
            String[] ruleStr = str.split("&");
            return isPass(ruleStr[0], num) && isPass(ruleStr[1], num);
        } else if (str.contains("|")) {
            String[] ruleStr = str.split("|");
            return isPass(ruleStr[0], num) || isPass(ruleStr[1], num);
        } else {

        }
        return isPass(str, num);
    }

    ;

/*
    public static boolean isRule(String str,String value){
        str=str.replace("%","");
        value=value.replace(",","");
        Integer num;
        if(value.length()>6){
            num=Integer.MAX_VALUE;
        }else {
            num = TypeConvert.StringConvertInteger(value);
        }
        if(str.contains("&")){
            String [] ruleStr=   str.split("&");
            return isPass(ruleStr[0],num)&&isPass(ruleStr[1],num);
        }else if(str.contains("|")){
            String [] ruleStr=   str.split("|");
            return isPass(ruleStr[0],num)||isPass(ruleStr[1],num);
        }else{

        }
        return isPass(str,num);
    };

    */

    public static boolean isPass(String str, Long value) {
        int index = str.indexOf(",");
        //(,10)
        if (PatternRule.match("\\(\\,.*\\d{1,}\\)", str)) {
            return Range.lessThan(Long.valueOf(str.substring(index + 1, str.length() - 1))).contains(value);
        }
        //(,10]
        if (PatternRule.match("\\(\\,.*\\d{1,}\\]", str)) {
            return Range.atMost(Long.valueOf(str.substring(index + 1, str.length() - 1))).contains(value);
        }
        //(10,)
        if (PatternRule.match("\\(.*\\d{1,}\\,\\)", str)) {
            return Range.greaterThan(Long.valueOf(str.substring(1, index))).contains(value);
        }
        //[10,)
        if (PatternRule.match("\\[.*\\d{1,}\\,\\)", str)) {
            return Range.atLeast(Long.valueOf(str.substring(1, index))).contains(value);
        }
        //(10,10)
        if (PatternRule.match("\\(.*\\d{1,}\\,.*\\d{1,}\\)", str)) {
            return Range.open(Long.valueOf(str.substring(1, index))
                    , Long.valueOf(str.substring(index + 1, str.length() - 1))).contains(value);
        }
        //(10,10]
        if (PatternRule.match("\\(.*\\d{1,}\\,.*\\d{1,}\\]", str)) {
            return Range.openClosed(Long.valueOf(str.substring(1, index))
                    , Long.valueOf(str.substring(index + 1, str.length() - 1))).contains(value);
        }
        //[10,10]
        if (PatternRule.match("\\[.*\\d{1,}\\,.*\\d{1,}\\]", str)) {
            return Range.closed(Long.valueOf(str.substring(1, index))
                    , Long.valueOf(str.substring(index + 1, str.length() - 1))).contains(value);
        }

        //[10,10)
        if (PatternRule.match("\\[.*\\d{1,}\\,.*\\d{1,}\\)", str)) {
            return Range.closedOpen(Long.valueOf(str.substring(1, index))
                    , Long.valueOf(str.substring(index + 1, str.length() - 1))).contains(value);
        }
        //!{10}
        return PatternRule.match("\\!\\{.*\\d{1,}\\}", str)
                && !Long.valueOf(str.substring(2, str.length() - 1)).equals(value);
    }
    public static boolean isPass(String str, Integer value) {
        int index = str.indexOf(",");
        //(,10)
        if (PatternRule.match("\\(\\,.*\\d{1,}\\)", str)) {
            return Range.lessThan(Integer.valueOf(str.substring(index + 1, str.length() - 1))).contains(value);
        }
        //(,10]
        if (PatternRule.match("\\(\\,.*\\d{1,}\\]", str)) {
            return Range.atMost(Integer.valueOf(str.substring(index + 1, str.length() - 1))).contains(value);
        }
        //(10,)
        if (PatternRule.match("\\(.*\\d{1,}\\,\\)", str)) {
            return Range.greaterThan(Integer.valueOf(str.substring(1, index))).contains(value);
        }
        //[10,)
        if (PatternRule.match("\\[.*\\d{1,}\\,\\)", str)) {
            return Range.atLeast(Integer.valueOf(str.substring(1, index))).contains(value);
        }
        //(10,10)
        if (PatternRule.match("\\(.*\\d{1,}\\,.*\\d{1,}\\)", str)) {
            return Range.open(Integer.valueOf(str.substring(1, index))
                    , Integer.valueOf(str.substring(index + 1, str.length() - 1))).contains(value);
        }
        //(10,10]
        if (PatternRule.match("\\(.*\\d{1,}\\,.*\\d{1,}\\]", str)) {
            return Range.openClosed(Integer.valueOf(str.substring(1, index))
                    , Integer.valueOf(str.substring(index + 1, str.length() - 1))).contains(value);
        }
        //[10,10]
        if (PatternRule.match("\\[.*\\d{1,}\\,.*\\d{1,}\\]", str)) {
            return Range.closed(Integer.valueOf(str.substring(1, index))
                    , Integer.valueOf(str.substring(index + 1, str.length() - 1))).contains(value);
        }

        //[10,10)
        if (PatternRule.match("\\[.*\\d{1,}\\,.*\\d{1,}\\)", str)) {
            return Range.closedOpen(Integer.valueOf(str.substring(1, index))
                    , Integer.valueOf(str.substring(index + 1, str.length() - 1))).contains(value);
        }
        //!{10}
        return PatternRule.match("\\!\\{.*\\d{1,}\\}", str)
                && !Integer.valueOf(str.substring(2, str.length() - 1)).equals(value);
    }

    public static boolean isPass(String str, Double value) {
        int index = str.indexOf(",");
        //(,10)
        if (PatternRule.match("\\(\\,.*\\d{1,}\\)", str)) {
            return Range.lessThan(Double.valueOf(str.substring(index + 1, str.length() - 1))).contains(value);
        }
        //(,10]
        if (PatternRule.match("\\(\\,.*\\d{1,}\\]", str)) {
            return Range.atMost(Double.valueOf(str.substring(index + 1, str.length() - 1))).contains(value);
        }
        //(10,)
        if (PatternRule.match("\\(.*\\d{1,}\\,\\)", str)) {
            return Range.greaterThan(Double.valueOf(str.substring(1, index))).contains(value);
        }
        //[10,)
        if (PatternRule.match("\\[.*\\d{1,}\\,\\)", str)) {
            return Range.atLeast(Double.valueOf(str.substring(1, index))).contains(value);
        }
        //(10,10)
        if (PatternRule.match("\\(.*\\d{1,}\\,.*\\d{1,}\\)", str)) {
            return Range.open(Double.valueOf(str.substring(1, index))
                    , Double.valueOf(str.substring(index + 1, str.length() - 1))).contains(value);
        }
        //(10,10]
        if (PatternRule.match("\\(.*\\d{1,}\\,.*\\d{1,}\\]", str)) {
            return Range.openClosed(Double.valueOf(str.substring(1, index))
                    , Double.valueOf(str.substring(index + 1, str.length() - 1))).contains(value);
        }
        //[10,10]
        if (PatternRule.match("\\[.*\\d{1,}\\,.*\\d{1,}\\]", str)) {
            return Range.closed(Double.valueOf(str.substring(1, index))
                    , Double.valueOf(str.substring(index + 1, str.length() - 1))).contains(value);
        }

        //[10,10)
        if (PatternRule.match("\\[.*\\d{1,}\\,.*\\d{1,}\\)", str)) {
            return Range.closedOpen(Double.valueOf(str.substring(1, index))
                    , Double.valueOf(str.substring(index + 1, str.length() - 1))).contains(value);
        }
        //!{10}
        return PatternRule.match("\\!\\{.*\\d{1,}\\}", str)
                && !Double.valueOf(str.substring(2, str.length() - 1)).equals(value);
    }
}
