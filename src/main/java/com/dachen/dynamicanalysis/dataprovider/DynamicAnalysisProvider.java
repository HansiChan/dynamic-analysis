package com.dachen.dynamicanalysis.dataprovider;

import com.dachen.dynamicanalysis.dto.Index;
import com.dachen.dynamicanalysis.pojo.AnalysisLineChartVo;
import com.dachen.dynamicanalysis.pojo.AnalysisListVo;
import com.dachen.dynamicanalysis.pojo.AnalysisVo;
import com.dachen.util.ImpalaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

@Repository
public class DynamicAnalysisProvider {
    Logger LOG = LoggerFactory.getLogger(this.getClass());
    @Autowired
    AnalysisCommonUtils analysisCommonUtils;
    @Autowired
	ImpalaUtil impalaUtil;

    public Object proportion(String module, String dimension, String dimension_sub,
                             String filter_condition, String begin_date, String end_date, String sqlJoin) throws Exception {

        Map<String, Object> aList = new LinkedHashMap<>();
        List<Object> sList = new LinkedList<>();

        String sqlFilter = "";
        String sql = "";
        String sqlModule = " where days>='" + begin_date + "' and days<='" + end_date + "' ";
        int subLength = analysisCommonUtils.filter(dimension).size();

        if (null != module && module.length() > 0 && "authenticated".equals(module)
                && null != begin_date && null != end_date && begin_date.length() > 0 && end_date.length() > 0) {
            sqlModule = " where x.days>='" + begin_date + "' and x.days<='" + end_date + "' and checkstatus='正常(审核通过)'";
        }

        if (null != filter_condition && filter_condition.length() > 0) {
            sqlFilter = filter_condition.replace("where", "and");
        }

        if (null != dimension_sub && dimension_sub.length() > 0) {
            String sqlTSub = "and " + dimension + " in ('" + dimension_sub.replace(",", "','") + "')";
            String sqlFSub = "and " + dimension + " not in ('" + dimension_sub.replace(",", "','") + "')";
            sql = "with t as " + sqlJoin + "select if(" + dimension + " is null,\"其他\"," + dimension + ") ,count(distinct(userid)) value from t "
                    + sqlModule + sqlFilter + sqlTSub + " group by " + dimension + " order by value desc union all select '其他',count(distinct(userid)) value from t "
                    + sqlModule + sqlFilter + sqlFSub;
        } else {
            String st = sqlFilter;
            String px = "where id>9";
            if (null != dimension_sub && dimension_sub.length() > 0) {
                if (sqlFilter.contains(dimension)) {
                    st = sqlFilter.replace("and", "and not");
                    px = "";
                }
            }
            if (subLength<10) {
                sql = "with t as " + sqlJoin + " select if(" + dimension + " is null or " + dimension + "='' or " +
                        dimension + " in ('NULL','未知'),\"其他\"," + dimension + ") name,count(distinct(userid)) value from t "
                        + sqlModule + sqlFilter + " group by " + dimension + " order by value desc";
            } else {
                sql = "with t as " + sqlJoin + " select if(" + dimension + " is null or " + dimension + "='' or " +
                        dimension + " in ('NULL','未知'),\"其他\"," + dimension + ") name,count(distinct(userid)) value from t "
                        + sqlModule + sqlFilter + " group by " + dimension + " order by value desc limit 9 union all "
                        + "select '其他' name,nvl(sum(value),0) value from (select row_number() over(order by count(distinct(userid)) desc) id,if(" + dimension
                        + " is null,\"其他\"," + dimension + ") ,count(distinct(userid)) value from t " + sqlModule + st + " group by "
                        + dimension + " order by value desc) as x " + px;
            }
        }

        Connection conn = null;
        Statement stat = null;
        ResultSet rs = null;
        try {
            LOG.info(sql);
            conn = impalaUtil.getConnection();
            stat = conn.createStatement();
            rs = stat.executeQuery(sql);

            while (rs.next()) {
                Map<String, Object> nMap = new LinkedHashMap<>();
                List<String> nList = new LinkedList<>();
                List<String> vList = new LinkedList<>();
                nList.add(rs.getString(1).trim());
                nList.add(dimension);
                vList.add(rs.getString(2).trim());
                nMap.put("names", nList);
                nMap.put("value", vList);
                sList.add(nMap);
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

        aList.put("duration", begin_date + "|" + end_date);
        aList.put("series", sList);

        return aList;
    }

    public Object getActiveLineChart(String module, String dimension, String dimension_sub, String begin_date,
                                     String end_date, String dateSql, String sqlWhere, String sqlJoin) throws Exception {
        String sql = "";
        int daysLen = AnalysisCommonUtils.getDayLength(begin_date, end_date);
        int subLength = 1;
        String[] subString = {module};

        /*if(dimension == null || "".equals(dimension)){
            if ("active".equals(module)) {
                module="活跃用户数";
            } else if ("new".equals(module)) {
                module="活跃用户数";
            } else if ("authenticating".equals(module)) {
                module="提交认证用户数";
            } else if ("authenticated".equals(module)) {
                module="认证通过用户数";
            }
        } else {
            subLength = filter(dimension).toString().split(",").length;
            subString = filter(dimension).toString().replace("[", "")
                    .replace("]", "").replace(" ", "").split(",");
        }


        if (dimension_sub != null && !"".equals(dimension_sub)) {
            dimension_sub = "'" + dimension_sub.replace(",", "','") + "'";
        } else {
            dimension_sub = "'" + String.join(",", subString).replace(",", "','").trim() + "'";
        }

        if (dimension == null || "".equals(dimension)) {
            sql = "with x as " + sqlJoin + "select " + dateSql + " as dt,'" +
                    "" + module + "' as name,count(distinct userid)  as value from x where days>='" + begin_date
                    + "' and days<='" + end_date + "'" + sqlWhere + "group by dt,name order by value desc";
        } else {
            String filter="";
            if(!sqlWhere.contains(dimension)) {filter=dimension + " in (" + dimension_sub + ") and ";}
            sql = "with x as " + sqlJoin + "select dt,name,value from \n" +
                    "(SELECT " + dateSql + " AS dt," + dimension + " AS name, count(distinct userid) AS value\n" +
                    " FROM x where " + filter +" days>='" + begin_date + "' and days<='"
                    + end_date + "'" + sqlWhere + "group by dt,name) z order by value desc ";
        }*/

        if (dimension == null || "".equals(dimension)) {
            module = Index.getName(module);
            subString = new String[]{module};
            sql = "with x as " + sqlJoin + "select " + dateSql + " as dt," +
                    "if('" + module + "' is null,\"其他\",'" + module + "') as name,count(distinct userid)  as value from x where days>='" + begin_date
                    + "' and days<='" + end_date + "'" + sqlWhere + "group by dt,name order by value desc";
        } else {
            /*String filter="";*/
            subLength = analysisCommonUtils.filter(dimension).size();
            subString = analysisCommonUtils.filter(dimension).toArray(new String[analysisCommonUtils.filter(dimension).size()]);
            /*if (dimension_sub != null && !"".equals(dimension_sub)) {
                dimension_sub = "'" + dimension_sub.replace(",", "','") + "'";
            } else {
                dimension_sub = "'" + String.join(",", subString).replace(",", "','").trim() + "'";
            }
            if(!sqlWhere.contains(dimension)) {
                filter=dimension + " in (" + dimension_sub + ") and ";
            }*/
            sql = "with x as " + sqlJoin + "select dt,name,value from \n" +
                    "(SELECT " + dateSql + " AS dt,if(" + dimension + " is null or " + dimension + "='' or " +
                    dimension + " in ('NULL','未知'),\"其他\"," + dimension + ") AS name, count(distinct userid) AS value\n" +
                    " FROM x where days>='" + begin_date + "' and days<='"
                    + end_date + "'" + sqlWhere + "group by dt,name) z order by value desc ";
        }

        List<AnalysisVo> voList = new ArrayList<>();
        List<Map> dtNameList = new ArrayList<>();
        Set<String> dtSet = new HashSet<>();
        Connection conn = null;
        Statement stat = null;
        ResultSet rs = null;
        try {
            LOG.info(sql);
            conn = impalaUtil.getConnection();
            stat = conn.createStatement();
            rs = stat.executeQuery(sql);
                while (rs.next()) {
                AnalysisVo vo2 = new AnalysisVo();
                Map<String, String> map = new HashMap<>();
                String dt = rs.getString(1);
                String name = rs.getString(2);
                String value = rs.getString(3);
                dtSet.add(dt);
                map.put(dt, name.trim());
                dtNameList.add(map);
                vo2.setDt(dt);
                vo2.setName(name.trim());
                vo2.setValue(value);
                voList.add(vo2);
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

        //
        Map<String, List> m = AnalysisCommonUtils.mapCombine(dtNameList);
        if (dimension != null && !"".equals(dimension)) {
            if (sqlWhere.contains(dimension)) {
                List nameList = new LinkedList();
                for (Map.Entry<String, List> entry : m.entrySet()) {
                    for (Object value : entry.getValue()) {
                        if (!nameList.contains(value)) {
                            nameList.add(value);
                        }
                    }
                    subString = (String[]) nameList.toArray(new String[nameList.size()]);
                    subLength = subString.length;
                /*if (entry.getValue().size() > nameListSize) {
                    nameListSize = entry.getValue().size();
                    subString = (String[]) entry.getValue().toArray(new String[entry.getValue().size()]);
                    subLength = subString.length;
                }*/
                }
            }
        }
        for (Map.Entry<String, List> entry : m.entrySet()) {
            String dt = entry.getKey();
            List<String> nameList = entry.getValue();
            if (nameList.size() <= subLength) {
                List<String> newList = new ArrayList<>();
                if (dimension != null || !"".equals(dimension)) {
                    for (String sub : subString) {
                        if (!nameList.contains(sub.trim())) {
                            newList.add(sub.trim());
                        }
                    }
                }
                for (String xx : newList) {
                    AnalysisVo vo2 = new AnalysisVo();
                    vo2.setDt(dt);
                    vo2.setName(xx.trim());
                    vo2.setValue("0");
                    voList.add(vo2);
                }
            }
        }



        List<String> everyDateList = AnalysisCommonUtils.dateSplit(begin_date, end_date, dateSql);
        if ("hours".equals(dateSql)) {
            daysLen = 24;
        }
        if (voList.size() < daysLen * subLength) {
            for (String day : everyDateList) {
                if (!dtSet.contains(day)) {
                    for (String sub : subString) {
                        AnalysisVo vo2 = new AnalysisVo();
                        vo2.setDt(day);
                        if (null == dimension || "" == dimension) {
                            vo2.setName(module);
                        } else {
                            vo2.setName(sub.trim());
                        }
                        vo2.setValue("0");
                        voList.add(vo2);
                    }
                }
            }
        }

        voList = AnalysisCommonUtils.sortList(voList, "dt", false);
        List<Map> aggMap = new ArrayList<>();
        List<String> xList = new ArrayList<>();
        for (AnalysisVo vo : voList) {
            Map<String, String> map = new HashMap<>();
            map.put(vo.getName(), vo.getValue());
            xList.add(vo.getDt());
            aggMap.add(map);
        }
        xList = AnalysisCommonUtils.removeDuplicate(xList);

        Map<String, List> map = AnalysisCommonUtils.mapCombine(aggMap);
        List<AnalysisListVo> dvoList = new ArrayList<>();
        for (Map.Entry<String, List> entry : map.entrySet()) {
            AnalysisListVo dvo = new AnalysisListVo();
            List<String> nameList = new ArrayList<>();
            List<String> valueList = new ArrayList<>();
            nameList.add(entry.getKey());
            for (int i = 0; i < entry.getValue().size(); i++) {
                valueList.add((String) entry.getValue().get(i));
            }
            dvo.setNames(nameList);
            dvo.setValues(valueList);
            dvoList.add(dvo);
        }
        AnalysisCommonUtils.sortValue(dvoList);
        AnalysisLineChartVo dyo = new AnalysisLineChartVo();
        dyo.setSeries(dvoList);
        dyo.setX_axis(xList);

        return dyo;
    }


    /*private List<String> getDimensionSub(String dimension) throws Exception {
        String sql = "select value from kudu_db.dimension_sub where sub_id='" + dimension + "'";
        List<String> subList = new ArrayList<>();
        try {
            conn = ImpalaUtil.getConnection();
            stat = conn.createStatement();
            rs = stat.executeQuery(sql);
            while (rs.next()) {
                subList.add(rs.getString(1));
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
        return subList;
    }*/

}