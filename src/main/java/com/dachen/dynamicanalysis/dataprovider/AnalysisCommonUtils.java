package com.dachen.dynamicanalysis.dataprovider;

import com.dachen.dynamicanalysis.pojo.AnalysisListVo;
import com.dachen.util.ImpalaUtil;
import org.apache.commons.beanutils.BeanComparator;
import org.apache.commons.collections.comparators.ComparableComparator;
import org.apache.commons.collections4.ComparatorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

@Component
public class AnalysisCommonUtils {

    private static final String DATE_FORMAT_NORMAL = "yyyy-MM-dd";
    private static final Logger logger = LoggerFactory.getLogger(AnalysisCommonUtils.class);

    @Autowired
	ImpalaUtil impalaUtil;

    public List<String> filter(String dimension) {

        String sql = "select dimtb,field from bds.bds_dimension_map where id='%s'";
        sql = String.format(sql,dimension);
        String tbname = "";
        String field ="";
        List<String> list = new LinkedList<>();
		Connection conn = null;
		Statement stat = null;
		ResultSet rs = null;
        try{
            conn = impalaUtil.getConnection();
            stat = conn.createStatement();
            rs = stat.executeQuery(sql);
            while (rs.next()) {
                tbname = rs.getString(1);
                field = rs.getString(2);
            }
        }catch(Exception e){
            logger.error("getClusterCount ERROR:{}",e.getMessage());
        } finally {
            try {
                if(Objects.nonNull(conn)) conn.close();
                if(Objects.nonNull(stat)) stat.close();
            } catch (SQLException e) {
                logger.error("getClusterCount ERROR:{}",e.getMessage());
            }
        }
        String sql2 = "select %s from %s";
        if("doctor_province".equals(dimension)|"hospital_level".equals(dimension)){sql2 = "select distinct %s from %S";}
        sql2 = String.format(sql2,field,tbname);
        try{
                conn = impalaUtil.getConnection();
            stat = conn.createStatement();
            rs = stat.executeQuery(sql2);
            while (rs.next()) {
                list.add(rs.getString(1));
            }
        }catch(Exception e){
            logger.error("getClusterCount ERROR:{}",e.getMessage());
        } finally {
            try {
                if(Objects.nonNull(conn)) conn.close();
                if(Objects.nonNull(stat)) stat.close();
            } catch (SQLException e) {
                logger.error("getClusterCount ERROR:{}",e.getMessage());
            }
        }
        return list;
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
        return year + "-" + (week<10?"0"+week:week);
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

}
