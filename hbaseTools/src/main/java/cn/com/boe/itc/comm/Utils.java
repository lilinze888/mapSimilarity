package cn.com.boe.itc.comm;

import cn.com.boe.itc.pojo.BpmapCorrelation;
import cn.hutool.core.date.DateTime;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.date.Week;
import org.apache.commons.collections.bag.SynchronizedSortedBag;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class Utils {
    private static Connection connection;

    public static final String dateFormat = "yyyyMMdd";
    public static final String monthFormat = "yyyyMM";
    public static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    /**
     * 产生从开始到结束的两个日期之间的所有日期列表，按照顺序，并且包含两端
     *
     * @param startDate
     * @param endDate
     * @return
     */
    public static List<DateTime> listDay(DateTime startDate, DateTime endDate) {
        LinkedList<DateTime> rslt = new LinkedList<>();

        for (DateTime currentDate = startDate;
             endDate.isAfterOrEquals(currentDate);
             currentDate = DateUtil.offsetDay(currentDate, 1)) {
            rslt.addLast(currentDate);
        }
        return rslt;
    }

    public static DateTime getWeekDayStart(DateTime dt) {
        Week week = dt.dayOfWeekEnum();
        int enWeekday = week.getValue(); // 从周日算一周的开始,周日->1,周一->2...周六->7
        int zhWeekdayOffset = -(enWeekday + 5) % 7;
        return DateUtil.offsetDay(dt, zhWeekdayOffset);
    }

    public static DateTime getWeekDayStart(String dtStr) {
        DateTime dt = DateUtil.parse(dtStr, "yyyyMMdd");
        return getWeekDayStart(dt);
    }

    /**
     * 获取当前日期前{{@offset}}周的开始日期
     *
     * @param dt
     * @param offset
     * @return
     */
    public static DateTime getWeekDayStart(DateTime dt, int offset) {
        DateTime currentWeekStartDay = getWeekDayStart(dt);
        return DateUtil.offsetWeek(currentWeekStartDay, offset);
    }

    @Test
    public void testMonth() throws ParseException {

        ArrayList<LinkedHashMap<String, String>> lineData = new ArrayList<>();

        LinkedHashMap<String, String> map = new LinkedHashMap<>();
        map.put(BpmapCorrelation.LOT_ID, "5");
        map.put(BpmapCorrelation.EVENTTIME, "2020-02-31 21:08:32");
        map.put(BpmapCorrelation.PROCESSOPERATION_ID, "5");
        map.put(BpmapCorrelation.R, "5");
        map.put(BpmapCorrelation.PARAM_VALUE_AVG, "5");
        lineData.add(map);

        LinkedHashMap<String, String> map1 = new LinkedHashMap<>();
        map1.put(BpmapCorrelation.LOT_ID, "4");
        map1.put(BpmapCorrelation.EVENTTIME, "2020-12-31 21:08:32");
        map1.put(BpmapCorrelation.PROCESSOPERATION_ID, "4");
        map1.put(BpmapCorrelation.R, "4");
        map1.put(BpmapCorrelation.PARAM_VALUE_AVG, "4");
        lineData.add(map1);

        LinkedHashMap<String, String> map2 = new LinkedHashMap<>();
        map2.put(BpmapCorrelation.LOT_ID, "3");
        map2.put(BpmapCorrelation.EVENTTIME, "2020-09-31 21:08:32");
        map2.put(BpmapCorrelation.PROCESSOPERATION_ID, "3");
        map2.put(BpmapCorrelation.R, "3");
        map2.put(BpmapCorrelation.PARAM_VALUE_AVG, "3");
        lineData.add(map2);

        LinkedHashMap<String, String> map3 = new LinkedHashMap<>();
        map3.put(BpmapCorrelation.LOT_ID, "2");
        map3.put(BpmapCorrelation.EVENTTIME, "2020-03-31 21:08:32");
        map3.put(BpmapCorrelation.PROCESSOPERATION_ID, "2");
        map3.put(BpmapCorrelation.R, "2");
        map3.put(BpmapCorrelation.PARAM_VALUE_AVG, "2");
        lineData.add(map3);

        LinkedHashMap<String, String> map4 = new LinkedHashMap<>();
        map4.put(BpmapCorrelation.LOT_ID, "1");
        map4.put(BpmapCorrelation.EVENTTIME, "2020-07-31 21:08:32");
        map4.put(BpmapCorrelation.PROCESSOPERATION_ID, "1");
        map4.put(BpmapCorrelation.R, "1");
        map4.put(BpmapCorrelation.PARAM_VALUE_AVG, "1");
        lineData.add(map4);

        System.out.println(lineData);

        Collections.sort(lineData,new Comparator<LinkedHashMap<String,String>>() {
            @Override
            public int compare(LinkedHashMap m1, LinkedHashMap m2) {
                System.out.println(((String)m1.get(BpmapCorrelation.EVENTTIME)) + "==="+ ((String)m2.get(BpmapCorrelation.EVENTTIME)));
                System.out.println(((String)m1.get(BpmapCorrelation.EVENTTIME)).compareTo((String)m2.get(BpmapCorrelation.EVENTTIME)));
                return ((String)m1.get(BpmapCorrelation.EVENTTIME)).compareTo((String)m2.get(BpmapCorrelation.EVENTTIME));
            }
        });

        System.out.println("After : "+lineData);

//        System.out.println(dayOffset(new DateTime(sdf.parse("2021-03-31 21:08:32")), -7).toString());
//        System.out.println(getWeekDayStart(new DateTime(sdf.parse("2021-03-31 21:08:32")), -4).toString());
//        System.out.println(getMonthStart(new DateTime(sdf.parse("2021-03-31 21:08:32")), -3).toString());
//        System.out.println(Integer.valueOf(new SimpleDateFormat("HH").format(new Date())) >= 15);
//        DateTime dt = new DateTime();
       /* SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        DateTime dt = new DateTime(sdf.parse("2021-03-31 21:08:32"));
        int offset = -3;
        System.out.println(Utils.getMonthStart( dt, offset).toString());*/
/*        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy/MM/dd (HH点) ");
        System.out.println(sdf1.format(sdf.parse("20200511161500")));*/
/*        String str = "tracking_hglass";
        System.out.println(Arrays.asList(str.toUpperCase().split(",")));*/
   /*     HashMap<String, String[]> m = new HashMap<>();
        m.put("key", str.toUpperCase().split(","));
        System.out.println(m);*/


    }

    public static String before3Month(String queryTtime)  {
        try {
            return getMonthStart(new DateTime(sdf.parse(queryTtime)), -3).toString();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return queryTtime;
    }

    public static String before4Week(String queryTtime)  {
        try {
            return getWeekDayStart(new DateTime(sdf.parse(queryTtime)), -4).toString();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return queryTtime;
    }

    public static String before7Day(String queryTtime)  {
        try {
            return dayOffset(new DateTime(sdf.parse(queryTtime)), -7).toString();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return queryTtime;
    }

    public static String getMonth(DateTime dt) {
        return dt.toString("yyyyMM");
    }

    public static String getMonth(String str) {
        return str.substring(0, 6);
    }

    public static DateTime dayOffset(DateTime dt, int offset) {
        return DateUtil.offsetDay(dt, offset);
    }

    public static DateTime dayNext(DateTime dt) {
        return dayOffset(dt, 1);
    }

/*
    public static DateTime getMonthStart(DateTime dt, int offset) {
        int dayOffsetOfMonth = dt.dayOfMonth();
        int monthStartDayOffset = dayOffsetOfMonth - 1;

        System.out.println(monthStartDayOffset);
        DateTime startDate = DateUtil.offsetDay(dt, -monthStartDayOffset);
        System.out.println(startDate);
        return DateUtil.offsetMonth(startDate, offset);
    }*/
    public static DateTime getMonthStart(DateTime dt, int offset) {

        DateTime startDate = DateUtil.offsetDay(dt, -0);
        return DateUtil.offsetMonth(startDate, offset);
    }

    public static String date2Str(DateTime dt) {
        return dt.toString(dateFormat);
    }

    public static String date2MonthStr(DateTime dt) {
        return dt.toString(monthFormat);
    }

    public static String date2WeekStr(DateTime dt) {
        return getWeekDayStart(dt).toString(dateFormat);
    }
    public static String date2WeekStr(String dt) {
        return getWeekDayStart(dt).toString(dateFormat);
    }
}
