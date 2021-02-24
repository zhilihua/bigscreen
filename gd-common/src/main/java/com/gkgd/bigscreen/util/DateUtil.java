package com.gkgd.bigscreen.util;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateUtil {
    public static boolean isInTimeRange(String intoTime,String startTime,String endTime) {
        String[] str = intoTime.split(" ");
        String time = null;
        if (str.length>1) {
            time = str[1];
        }else{
            time = intoTime;
        }
        int set;
        if (time.split(":").length==3) {
            set = Integer.valueOf(time.replaceAll(":", ""));
        }else {
            set = Integer.valueOf(time.replaceAll(":", "")) * 100;
        }
        int begin;
        if(startTime.split(":").length==3) {
            begin = Integer.valueOf(startTime.replaceAll(":", ""));
        }else {
            begin = Integer.valueOf(startTime.replaceAll(":", "")) * 100;
        }
        int end;
        if(endTime.split(":").length==3) {
            end = Integer.valueOf(endTime.replaceAll(":", ""));
        }else {
            end = Integer.valueOf(endTime.replaceAll(":", "")) * 100;
        }
        if (begin > end) {
            return set < end || set > begin;
        } else {
            return set > begin && set < end;
        }
    }
    public static boolean isEffectiveDate(String intoTime, String startTimeStr, String endTimeStr) throws Exception {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
        Date nowTime = simpleDateFormat.parse(intoTime);
        Date startTime = simpleDateFormat.parse(startTimeStr);
        Date endTime = simpleDateFormat.parse(endTimeStr);
        if (nowTime.getTime() == startTime.getTime()
                || nowTime.getTime() == endTime.getTime()) {
            return true;
        }

        Calendar date = Calendar.getInstance();
        date.setTime(nowTime);

        Calendar begin = Calendar.getInstance();
        begin.setTime(startTime);

        Calendar end = Calendar.getInstance();
        end.setTime(endTime);

        if (date.after(begin) && date.before(end)) {
            return true;
        } else {
            return false;
        }
    }
    public static void main(String[] args) throws Exception {
        String timeStr = "2020-10-20 08:46:39";
        String endTime = "23:00:00";
        String startTime = "05:00";
        boolean inTimeRange = DateUtil.isInTimeRange(timeStr, startTime, endTime);
        System.out.println(inTimeRange);
//        System.out.println(isEffectiveDate(timeStr,startTime,endTime));

    }
}
