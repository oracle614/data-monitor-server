package com.yiche.utils;

import org.apache.commons.lang.StringUtils;

public  class TypeConvert {

    public static String IntegerConvertString(Integer value){
        return value==null?null:value.toString();
    }

    public static String DoubleConvertString(Double value){
        return value==null?null:value.toString();
    }


    public static Integer StringConvertInteger(String value ){
        return StringUtils.isEmpty(value)?0:Integer.valueOf(value);
    }

    public static Double StringConvertDouble(String value){
        return StringUtils.isEmpty(value)?0:Double.valueOf(value);
    }

    public static Double IntegerConvertDouble(Integer value){
        return value==null?null:Double.valueOf(value);
    }
}
