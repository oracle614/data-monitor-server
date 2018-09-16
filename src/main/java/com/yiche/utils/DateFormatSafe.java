package com.yiche.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateFormatSafe {



    private static ThreadLocal<DateFormat> threadLocal = new ThreadLocal<DateFormat>() {
        @Override
        protected DateFormat initialValue() {
            return new SimpleDateFormat("yyyyMMdd");
        }
    };

    private static ThreadLocal<DateFormat> threadLocalSign = new ThreadLocal<DateFormat>() {
        @Override
        protected DateFormat initialValue() {
            return new SimpleDateFormat("yyyy-MM-dd");
        }
    };

    private static ThreadLocal<DateFormat> threadLocalMonth = new ThreadLocal<DateFormat>() {
        @Override
        protected DateFormat initialValue() {
            return new SimpleDateFormat("yyyy-MM");
        }
    };


    public static ThreadLocal<Boolean> FLAGPASS = new ThreadLocal<Boolean>() {
        @Override
        protected Boolean initialValue() {
            return true;
        }
    };

    public static String dateFormat(String dataFormat,Date date){
        String dateStr;
        if (dataFormat.contains("-")) {
            dateStr = DateFormatSafe.formatSign(date==null?new Date():date);
        } else {
            dateStr = DateFormatSafe.format(date==null?new Date():date);
        }
        return  dateStr;
    }

    public static String dateFormatMonth(Date date){
        return   formatMonth(date==null?new Date():date);
    }

    public static String format(Date date) {
        return threadLocal.get().format(date);
    }

    public static String formatSign(Date date) {
        return threadLocalSign.get().format(date);
    }

    public static String formatMonth(Date date) {
        return threadLocalMonth.get().format(date);
    }

    public static Date getDay(int dayNum) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date());
        cal.add(Calendar.DATE,-dayNum);
        return cal.getTime();
    }

    public static Date getMonth(int dayNum) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date());
        cal.add(Calendar.MONTH,-dayNum);
        return cal.getTime();
    }

    public static boolean isFirstDay(){
        Calendar cal = Calendar.getInstance();
        int today = cal.get(cal.DAY_OF_MONTH);
        return  1==today;
    }

    public  static void updateFlag(boolean flag){
        if(FLAGPASS.get()){
            FLAGPASS.set(flag);
        }


    }

    /**
     * 得到本周周3
     *
     * @return yyyy-MM-dd
     */
    public static Date getWedOfThisWeek() {
        Calendar c = Calendar.getInstance();
        int day_of_week = c.get(Calendar.DAY_OF_WEEK) - 1;
        if (day_of_week == 0)
            day_of_week = 7;
        c.add(Calendar.DATE, -day_of_week + 3);
        return c.getTime();
    }

    public static Date getDay(Date date,int dayNum) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.DATE,-dayNum);
        return cal.getTime();
    }

    public static String getSimpleDate(Date date){
        SimpleDateFormat  sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        return  sdf.format(date);
    }
}
