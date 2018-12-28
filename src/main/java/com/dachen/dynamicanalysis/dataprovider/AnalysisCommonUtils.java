package com.dachen.dynamicanalysis.dataprovider;

import com.dachen.dynamicanalysis.pojo.AnalysisListVo;
import com.dachen.util.ImpalaUtil;
import org.apache.commons.beanutils.BeanComparator;
import org.apache.commons.collections.comparators.ComparableComparator;
import org.apache.commons.collections4.ComparatorUtils;

import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class AnalysisCommonUtils {

    private static final String DATE_FORMAT_NORMAL = "yyyy-MM-dd";

    static Connection conn = null;
    static Statement stat = null;
    static ResultSet rs = null;

    public static Object filter(String dimension) throws Exception {

        List<String> dList = new ArrayList<>();

        String sql = "";
        if (null != dimension && dimension.length() > 0 && "level".equals(dimension)) {
            sql = "select distinct(if(" + dimension + " is null or " + dimension + "='' or " + dimension + "='NULL',\"未知\",level)) from ods.ods_b_hospital";
        } else if (null != dimension && dimension.length() > 0 && "source_sourcetype".equals(dimension)) {
            sql = "select distinct((case when " + dimension + "='1' then 'APP注册' when " + dimension + "='2' then '集团邀请' when " + dimension + "='3' then '医院邀请' \n" +
                    "        when " + dimension + "='4' then '集团新建' when " + dimension + "='5' then '运营新建' when " + dimension + "='6' then '医院新建' when " + dimension + "='7' then '博德嘉联客户端注册' \n" +
                    "        when " + dimension + "='8' then '医生邀请' when " + dimension + "='9' then '微信用户注册' when " + dimension + "='10' then '博德嘉联医生助手邀请' when " + dimension + "='11' then '农牧项目批量导入' \n" +
                    "        when " + dimension + "='12' then '运营平台批量导入' when " + dimension + "='13' then '分享页面注册' when " + dimension + "='14' then '药店圈邀请' when " + dimension + "='15' then '第三方' \n" +
                    "        when " + dimension + "='16' then '药企圈' when " + dimension + "='17' then '医生圈' when " + dimension + "='18' then '医生圈H5邀请加入圈子'  else '未知' end)) from ods.ods_user";
        } else if (null != dimension && dimension.length() > 0 && "status".equals(dimension)) {
            sql = "select distinct((case when " + dimension + "='1' then '正常' when " + dimension + "='2' then '待审核' when " + dimension + "='3' then '审核未通过' when " + dimension + "='4' then '暂时禁用' when " + dimension + "='5' \n" +
                    "       \tthen '永久禁用' when " + dimension + "='6' then '未激活' when " + dimension + "='7' then '未认证' when " + dimension + "='8' then '离职' when " + dimension + "='9' then '注销' else '未知' end)) from ods.ods_user";
        } else if (null != dimension && dimension.length() > 0 && "userlevel".equals(dimension)) {
            sql = "select distinct((case when " + dimension + "='0' then '到期' when " + dimension + "='1' then '游客' when " + dimension + "='2' then '临时用户' when " + dimension + "='3' then '认证用户' else '未知' end)) from ods.ods_user";
        } else if (null != dimension && dimension.length() > 0 && "model".equals(dimension)) {
            dimension = "loginlog_model";
            sql = "select distinct(if(" + dimension + " is null,\"未知\"," + dimension + ")) from ods.ods_user";
        } else if (null != dimension && dimension.length() > 0) {
            sql = "select distinct(if(" + dimension + " is null,\"未知\"," + dimension + ")) from ods.ods_user";
        }

        try {
            conn = ImpalaUtil.getConnection();
            stat = conn.createStatement();
            rs = stat.executeQuery(sql);
            while (rs.next()) {
                String list = rs.getString(1).trim();
                dList.add(list);
            }

        } catch (Exception e) {
            throw new Exception("ERROR:" + e.getMessage(), e);
        } finally {
            try {
                conn.close();
                stat.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return dList;
    }


    public static int getDayLength(String start_date, String end_date) throws Exception {
        Date fromDate = getStrToDate(start_date, DATE_FORMAT_NORMAL);
        Date toDate = getStrToDate(end_date, DATE_FORMAT_NORMAL);
        long from = fromDate.getTime();
        long to = toDate.getTime();
        int days = (int) ((to - from) / (24 * 60 * 60 * 1000));
        return days + 1;
    }

    public static Date getStrToDate(String date, String fomtter) throws Exception {
        DateFormat df = new SimpleDateFormat(fomtter);
        return df.parse(date);
    }

    public static List sortList(List list, String propertyName, boolean isAsc) {
        Comparator mycmp = ComparableComparator.getInstance();
        mycmp = ComparatorUtils.nullLowComparator(mycmp);  //允许null
        if (isAsc) {
            mycmp = ComparatorUtils.reversedComparator(mycmp); //逆序
        }
        Comparator cmp = new BeanComparator(propertyName, mycmp);
        Collections.sort(list, cmp);
        return list;
    }

    public static List<String> removeDuplicate(List<String> list) {
        Set<String> set = new HashSet<>();
        List<String> newList = new ArrayList<>();
        for (Iterator<String> iter = list.iterator(); iter.hasNext(); ) {
            String element = iter.next();
            if (set.add(element))
                newList.add(element);
        }
        return newList;
    }

    public static List<String> dateSplit(String begin_date, String end_date, String type_date) throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date startDate = sdf.parse(begin_date);
        Date endDate = sdf.parse(end_date);
        List<String> dList = new ArrayList<>();
//        if (startDate.compareTo(endDate)<=0) throw new Exception("开始时间应该在结束时间之后");
        Long spi = endDate.getTime() - startDate.getTime();
        int splitDate = 0;
        if (type_date.equals("days")) {
            splitDate = 24 * 60 * 60 * 1000;
        } else if ("weeks".equals(type_date)) {
            splitDate = 1000 * 3600 * 24 * 7;
        } else if ("months".equals(type_date)) {
            splitDate = 1000 * 3600 * 24 * 30;
        } else if ("hours".equals(type_date)) {
            for (int i = 1; i <= 24; i++) {
                if (i < 10) {
                    dList.add("0" + i);
                } else {
                    dList.add("" + i);
                }

            }
            return dList;
        }
        Long step = Math.abs(spi / splitDate);// 相隔天数

        List<Date> dateList = new ArrayList<>();
        dateList.add(endDate);
        for (int i = 1; i <= step; i++) {
            dateList.add(new Date(dateList.get(i - 1).getTime() - Math.abs(splitDate)));// 比上一天减一
        }

        if (!dateList.isEmpty()) {
            for (Date date : dateList) {
                if ("weeks".equals(type_date)) {
                    dList.add(getWeekByDate(date));
                } else if ("months".equals(type_date)) {
                    dList.add(sdf.format(date).substring(0, 7));
                } else {
                    dList.add(sdf.format(date));
                }

            }
        }
        dList =  removeDuplicate(dList);
        return dList;
    }

    public static String getWeekByDate(Date dateStr) {
        Calendar cl = Calendar.getInstance();
        try {
            cl.setTime(dateStr);
        } catch (Exception e) {
            e.printStackTrace();
        }
        cl.setFirstDayOfWeek(Calendar.MONDAY);
        int week = cl.get(Calendar.WEEK_OF_YEAR);
        cl.add(Calendar.DAY_OF_MONTH, -7);
        int year = cl.get(Calendar.YEAR);
        return year + "-" + week;
    }

    public static Map mapCombine(List<Map> list) {
        Map<Object, List> map = new LinkedHashMap<>();
        for (Map m : list) {
            Iterator<Object> it = m.keySet().iterator();
            while (it.hasNext()) {
                Object key = it.next();
                if (!map.containsKey(key)) {
                    List newList = new ArrayList<>();
                    newList.add(m.get(key));
                    map.put(key, newList);
                } else {
                    map.get(key).add(m.get(key));
                }
            }
        }
        return map;
    }

    public static void sortValue(List<AnalysisListVo> list) {
        Collections.sort(list, (o1, o2) -> {
            int sum1 = 0;
            int sum2 = 0;
            for (Object v : o1.getValues()) {
                sum1 += Float.valueOf(v.toString());
            }
            for (Object v : o2.getValues()) {
                sum2 += Float.valueOf(v.toString());
            }
            return sum2 - sum1;
        });
    }

    public static String formatNumber(double value) {

        NumberFormat nf = NumberFormat.getNumberInstance();
        nf.setMaximumFractionDigits(2);
        /*
         * setMinimumFractionDigits设置成2
         *
         * 如果不这么做，那么当value的值是100.00的时候返回100
         *
         * 而不是100.00
         */
        nf.setMinimumFractionDigits(2);
        nf.setRoundingMode(RoundingMode.HALF_UP);
        /*
         * 如果想输出的格式用逗号隔开，可以设置成true
         */
        nf.setGroupingUsed(false);
        return nf.format(value);
    }
}
