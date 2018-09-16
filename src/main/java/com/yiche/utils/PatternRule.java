package com.yiche.utils;

import com.google.common.collect.Range;

import java.text.NumberFormat;
import java.util.regex.Matcher;

public class PatternRule {

    public static NumberFormat numberFormat = NumberFormat.getInstance();

    static{
        numberFormat.setMaximumFractionDigits(0);
    }
    public static boolean   match(String pattern,String str){
        java.util.regex.Pattern r = java.util.regex.Pattern.compile(pattern);
        Matcher m = r.matcher(str);
       return m.matches();
    }

//
     public static boolean isRule(String str,Integer value){
         str=str.replace("%","");
        if(str.contains("&")){
         String [] ruleStr=   str.split("&");
         return isPass(ruleStr[0],value)&&isPass(ruleStr[1],value);
        }else if(str.contains("|")){
            String [] ruleStr=   str.split("|");
            return isPass(ruleStr[0],value)||isPass(ruleStr[1],value);
        }
        return isPass(str,value);
     };

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

//    public  static boolean isPass(String str,Integer value){
//        int index=  str.indexOf(",");
//        //(,10)
//        if(PatternRule.match("\\(\\,\\d{1,}\\)",str)){
//            return  Range.lessThan(Integer.valueOf(str.substring(index+1,str.length()-1))).contains(value);
//        }
//        //(,10]
//        if(PatternRule.match("\\(\\,\\d{1,}\\]",str)){
//            return  Range.atMost(Integer.valueOf(str.substring(index+1,str.length()-1))).contains(value);
//        }
//        //(10,)
//        if(PatternRule.match("\\(\\d{1,}\\,\\)",str)){
//            return  Range.greaterThan(Integer.valueOf(str.substring(1,index))).contains(value);
//        }
//        //[10,)
//        if(PatternRule.match("\\[\\d{1,}\\,\\)",str)){
//            return  Range.atLeast(Integer.valueOf(str.substring(1,index))).contains(value);
//        }
//        //(10,10)
//        if(PatternRule.match("\\(\\d{1,}\\,\\d{1,}\\)",str)){
//            return  Range.open(Integer.valueOf(str.substring(1,index))
//                    ,Integer.valueOf(str.substring(index+1,str.length()-1))).contains(value);
//        }
//        //(10,10]
//        if(PatternRule.match("\\(\\d{1,}\\,\\d{1,}\\]",str)){
//            return  Range.openClosed(Integer.valueOf(str.substring(1,index))
//                    ,Integer.valueOf(str.substring(index+1,str.length()-1))).contains(value);
//        }
//        //[10,10]
//        if(PatternRule.match("\\[\\d{1,}\\,\\d{1,}\\]",str)){
//            return  Range.closed(Integer.valueOf(str.substring(1,index))
//                    ,Integer.valueOf(str.substring(index+1,str.length()-1))).contains(value);
//        }
//
//        //[10,10)
//        if(PatternRule.match("\\[\\d{1,}\\,\\d{1,}\\)",str)){
//            return  Range.closedOpen(Integer.valueOf(str.substring(1,index))
//                    ,Integer.valueOf(str.substring(index+1,str.length()-1))).contains(value);
//        }
//        //!{10}
//        return   PatternRule.match("\\!\\{\\d{1,}\\}",str)
//                && Integer.valueOf(str.substring(2,str.length()-1)).equals(value);
//    }
public  static boolean isPass(String str,Integer value){
    int index=  str.indexOf(",");
    //(,10)
    if(PatternRule.match("\\(\\,.*\\d{1,}\\)",str)){
        return  Range.lessThan(Integer.valueOf(str.substring(index+1,str.length()-1))).contains(value);
    }
    //(,10]
    if(PatternRule.match("\\(\\,.*\\d{1,}\\]",str)){
        return  Range.atMost(Integer.valueOf(str.substring(index+1,str.length()-1))).contains(value);
    }
    //(10,)
    if(PatternRule.match("\\(.*\\d{1,}\\,\\)",str)){
        return  Range.greaterThan(Integer.valueOf(str.substring(1,index))).contains(value);
    }
    //[10,)
    if(PatternRule.match("\\[.*\\d{1,}\\,\\)",str)){
        return  Range.atLeast(Integer.valueOf(str.substring(1,index))).contains(value);
    }
    //(10,10)
    if(PatternRule.match("\\(.*\\d{1,}\\,.*\\d{1,}\\)",str)){
        return  Range.open(Integer.valueOf(str.substring(1,index))
                ,Integer.valueOf(str.substring(index+1,str.length()-1))).contains(value);
    }
    //(10,10]
    if(PatternRule.match("\\(.*\\d{1,}\\,.*\\d{1,}\\]",str)){
        return  Range.openClosed(Integer.valueOf(str.substring(1,index))
                ,Integer.valueOf(str.substring(index+1,str.length()-1))).contains(value);
    }
    //[10,10]
    if(PatternRule.match("\\[.*\\d{1,}\\,.*\\d{1,}\\]",str)){
        return  Range.closed(Integer.valueOf(str.substring(1,index))
                ,Integer.valueOf(str.substring(index+1,str.length()-1))).contains(value);
    }

    //[10,10)
    if(PatternRule.match("\\[.*\\d{1,}\\,.*\\d{1,}\\)",str)){
        return  Range.closedOpen(Integer.valueOf(str.substring(1,index))
                ,Integer.valueOf(str.substring(index+1,str.length()-1))).contains(value);
    }
    //!{10}
    return   PatternRule.match("\\!\\{.*\\d{1,}\\}",str)
            && !Integer.valueOf(str.substring(2,str.length()-1)).equals(value);
}
}
