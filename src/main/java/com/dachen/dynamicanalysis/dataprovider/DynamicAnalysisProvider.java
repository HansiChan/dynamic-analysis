package com.dachen.dynamicanalysis.dataprovider;

import com.alibaba.fastjson.JSON;
import com.dachen.dynamicanalysis.dto.GroupCount;
import com.dachen.dynamicanalysis.dto.Index;
import com.dachen.dynamicanalysis.pojo.AnalysisLineChartVo;
import com.dachen.dynamicanalysis.pojo.AnalysisListVo;
import com.dachen.dynamicanalysis.pojo.AnalysisVo;
import com.dachen.util.ImpalaUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Collectors;

@Repository
public class DynamicAnalysisProvider {
    Logger LOG = LoggerFactory.getLogger(this.getClass());
    @Autowired
    AnalysisCommonUtils analysisCommonUtils;
    @Autowired
	ImpalaUtil impalaUtil;

    public Object proportion(String module, String dimension, String dimension_sub,
                             String filter_condition, String begin_date, String end_date, String sqlJoin,String sqlTable11,String product,String cluster) throws Exception {

        Map<String, Object> aList = new LinkedHashMap<>();
        List<Object> sList = new LinkedList<>();

        String sqlFilter = "";
        String sql = "";
        String sqlModule = " where product='"+product+"'"+" and days>='" + begin_date + "' and days<='" + end_date + "' ";
        int subLength = analysisCommonUtils.filter(dimension).size();

        if (null != module && module.length() > 0 && "autenticated".equals(module)
                && null != begin_date && null != end_date && begin_date.length() > 0 && end_date.length() > 0) {
            sqlModule = " where product='"+product+"'"+" and days>='" + begin_date + "' and days<='" + end_date + "' and checkstatus='正常(审核通过)'";
        }

        if (null != filter_condition && filter_condition.length() > 0) {
            sqlFilter = filter_condition.replace("where", "and");
            if(filter_condition.contains("其他")){
                String x =filter_condition.split(" ")[1];
                sqlFilter = filter_condition.replace("where"," and (").replace("'其他'","'','NULL','未知'")+ " or " + x + " is null) ";
            }else if(filter_condition.contains("无")){
                String x =filter_condition.split(" ")[1];
                sqlFilter = filter_condition.replace("where", " and (") + " or " + x + " is null" + ") ";
            }
        }

        if (null != dimension_sub && dimension_sub.length() > 0) {
            String sqlTSub = "and " + dimension + " in ('" + dimension_sub.replace(",", "','") + "')";
            String sqlFSub = "and " + dimension + " not in ('" + dimension_sub.replace(",", "','") + "')";
            /*sql = "with t as " + sqlJoin + "select if(" + dimension + " is null,\"其他\"," + dimension + ") ,count(distinct(userid)) value from t "
                    + sqlModule + sqlFilter + sqlTSub + " group by " + dimension + " order by value desc union all select '其他',count(distinct(userid)) value from t "
                    + sqlModule + sqlFilter + sqlFSub;*/
            sql = "with t as " + "(select  row_number() over (order by count(distinct userid) desc) rn ,"+dimension+",count(distinct(userid)) value from "+sqlJoin+" tt"
                	+ sqlModule + sqlFilter + sqlTSub + " group by " + dimension + " order by value desc)"
                	+"select "+dimension+", value from t where rn <= 10 "
                	+" union all select '其它', sum(value) from t where rn>10";
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
                /*sql = "with t as " + sqlJoin + " select if(" + dimension + " is null or " + dimension + "='' or " +
                        dimension + " in ('NULL','未知'),\"其他\"," + dimension + ") name,count(distinct(userid)) value from t "
                        + sqlModule + sqlFilter + " group by " + dimension + " order by value desc";*/
            	 sql = "with t as " + "(select  row_number() over (order by count(distinct userid) desc) rn ,"+dimension+",count(distinct(userid)) value from "+sqlJoin+" tt"
            			 + sqlModule + sqlFilter + " group by " + dimension + " order by value desc)"
            			 +"select "+dimension+", value from t where rn <= 10 "
                     	+" union all select '其它', sum(value) from t where rn>10";
            	
            } else {
                /*sql = "with t as " + sqlJoin + " select if(" + dimension + " is null or " + dimension + "='' or " +
                        dimension + " in ('NULL','未知'),\"其他\"," + dimension + ") name,count(distinct(userid)) value from t "
                        + sqlModule + sqlFilter + " group by " + dimension + " order by value desc limit 9 union all "
                        + "select '其他' name,nvl(sum(value),0) value from (select row_number() over(order by count(distinct(userid)) desc) id,if(" + dimension
                        + " is null,\"其他\"," + dimension + ") ,count(distinct(userid)) value from t " + sqlModule + st + " group by "
                        + dimension + " order by value desc) as x " + px;*/
                sql = "with t as " + "(select  row_number() over (order by count(distinct userid) desc) rn ,"+dimension+",count(distinct(userid)) value from "+sqlJoin+" tt"
           			 + sqlModule + sqlFilter + " group by " + dimension + " order by value desc)"
           			 +"select "+dimension+", value from t where rn <= 10 "
                     +" union all select '其它', sum(value) from t where rn>10";
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
            
            List<GroupCount> groupList = Lists.newArrayList();
            List<GroupCount> groupNoNList = Lists.newArrayList();
            while (rs.next()) {
            	
            	GroupCount gc = new GroupCount();
	        	gc.setName(rs.getString(1));
	        	gc.setCount(rs.getLong(2));
	        	groupList.add(gc);
            }
            
            if(!CollectionUtils.isEmpty(groupList)){
    			List<GroupCount> groupNullList = groupList.stream().filter(g->StringUtils.isEmpty(g.getName())).collect(Collectors.toList());
    			if(!CollectionUtils.isEmpty(groupNullList)){
    				//合并
    				groupNullList = groupList.stream().filter(g->StringUtils.isEmpty(g.getName()) || Objects.equals(g.getName(),"其它")).collect(Collectors.toList());
    				groupNoNList = groupList.stream().filter(g-> !StringUtils.isEmpty(g.getName()) && !Objects.equals(g.getName(),"其它")).collect(Collectors.toList());
    				GroupCount tempGroupCount = new GroupCount();
    				tempGroupCount.setName("其它");
    				Long tempCount = 0L;
    				for(GroupCount g : groupNullList){
    					if(Objects.nonNull(g.getCount())){
    						tempCount+=g.getCount();
    					}
    				}
    				tempGroupCount.setCount(tempCount);
    				groupNoNList.add(tempGroupCount);
    			}else{
    				if(groupList.size()>10){
    					Long tempCount = 0L;
    					for(int i=0;i<groupList.size();i++){
    						if(i<9){
    							groupNoNList.add(groupList.get(i));
    						}else{
    							if(Objects.nonNull(groupList.get(i).getCount())){
    								tempCount+=groupList.get(i).getCount();
    							}
    						}
    					}
    					GroupCount tempGroupCount = new GroupCount();
    					tempGroupCount.setName("其它");
    					tempGroupCount.setCount(tempCount);
    					groupNoNList.add(tempGroupCount);
    				}else{
    					groupNoNList = groupList;
    				}
    			}
    		}
            groupNoNList.forEach(g->{
            	Map<String, Object> nMap = new LinkedHashMap<>();
                List<String> nList = new LinkedList<>();
                List<String> vList = new LinkedList<>();
                nList.add(g.getName());
                nList.add(dimension);
                vList.add(g.getCount().toString());
                nMap.put("names", nList);
                nMap.put("value", vList);
                sList.add(nMap);
    			
    		});
            

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
                                     String end_date, String dateSql, String sqlWhere, String sqlJoin,String product,String cluster) throws Exception {
        String sql = "";
        int daysLen = AnalysisCommonUtils.getDayLength(begin_date, end_date);
        List<String> dimensionList = Lists.newArrayList();
        if (dimension == null || "".equals(dimension)) {
            module = Index.getName(module);
            sql = "with x as " + sqlJoin + "select " + dateSql + " as dt," +
                    "if('" + module + "' is null,\"其他\",'" + module + "') as name,count(distinct userid)  as value from x where product='"+product+"'"+" and days>='" + begin_date
                    + "' and days<='" + end_date + "'" + sqlWhere + "group by dt,name order by value desc";
        } else {
        	dimensionList = analysisCommonUtils.filter(dimension);
            sql = "with x as " + sqlJoin + "select dt,name,value from \n" +
                    "(SELECT " + dateSql + " AS dt,if(" + dimension + " is null or " + dimension + "='' or " +
                    dimension + " in ('NULL','未知'),\"其他\"," + dimension + ") AS name, count(distinct userid) AS value\n" +
                    " FROM x where product='"+product+"'"+" and days>='" + begin_date + "' and days<='"
                    + end_date + "'" + sqlWhere + "group by dt,name) z order by value desc ";
        }
        List<Map<String,String>> resultList = new ArrayList<Map<String,String>>();
        Connection conn = null;
        Statement stat = null;
        ResultSet rs = null;
        try {
            LOG.info(sql);
            conn = impalaUtil.getConnection();
            stat = conn.createStatement();
            rs = stat.executeQuery(sql);
                while (rs.next()) {
	        	String dt = rs.getString(1);
	            String name = rs.getString(2);
	            String value = rs.getString(3);
                Map<String, String> map = new HashMap<>();
                map.put("dt",dt);
                map.put("name",name);
                map.put("value",value);
                resultList.add(map);
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
        List<String> everyDateList = AnalysisCommonUtils.dateSplit(begin_date, end_date, dateSql);
        if(StringUtils.isNotEmpty(dateSql) && !Objects.equals("hours", dateSql)){
        	Collections.sort(everyDateList,((o1, o2) -> {
            	return o1.compareTo(o2);
            }));
        }
        
        //分组不就完事了吗
        Map<String,List<Map<String,String>>> resultGroup = resultList.stream().collect(Collectors.groupingBy(r->r.get("name")));
        List<String> resultDimensionList  = Lists.newArrayList(resultGroup.keySet());
        dimensionList =  resultDimensionList;
        List<String> others = Lists.newArrayList();
        if(dimensionList.contains("其他")){
        	others.add("其他");
        	dimensionList.remove("其他");
        }
        if(dimensionList.contains("其它")){
        	others.add("其它");
        	dimensionList.remove("其它");
        }
        Collections.sort(dimensionList,((o1, o2) -> {
        	Integer o11 = (Integer)o1.hashCode();
        	Integer o22 = (Integer)o1.hashCode();
        	return -o11.compareTo(o22);
        }));
        LOG.info("everyDateList-{}",JSON.toJSONString(everyDateList));
        if(!CollectionUtils.isEmpty(others)){
        	dimensionList.addAll(others);
        }
        List<AnalysisListVo> dvoList = new ArrayList<>();
        dimensionList.forEach(d->{
        	dvoList.add(packageData(d,resultGroup,everyDateList));
        });
        //排序dvoList
        Collections.sort(dvoList,new Comparator<AnalysisListVo>() {
			@Override
			public int compare(AnalysisListVo o1, AnalysisListVo o2) {
				int num1 = 0;
				int num2 = 0;
				if(Objects.nonNull(o1) && !CollectionUtils.isEmpty(o1.getValues()) && o1.getValues().size()>0){
					if(StringUtils.isNotEmpty(o1.getValues().get(o1.getValues().size()-1))){
						num1 = Integer.parseInt(o1.getValues().get(o1.getValues().size()));
					}
				}
				if(Objects.nonNull(o2) && !CollectionUtils.isEmpty(o2.getValues()) && o2.getValues().size()>0){
					if(StringUtils.isNotEmpty(o2.getValues().get(o2.getValues().size()))){
						num2 = Integer.parseInt(o2.getValues().get(o2.getValues().size()));
					}
				}
				return num2-num1;
			}
		}); // 按第一个值排序
        AnalysisLineChartVo dyo = new AnalysisLineChartVo();
        dyo.setSeries(dvoList);
        dyo.setX_axis(everyDateList);
        return dyo;
    }

	private AnalysisListVo packageData(String d, Map<String, List<Map<String, String>>> resultGroup,
			List<String> everyDateList) {
		AnalysisListVo vo = new AnalysisListVo();
		List<String> names = Lists.newArrayList(d);
		List<String> values = Lists.newArrayListWithExpectedSize(everyDateList.size());
		List<Map<String, String>> valuesList  = resultGroup.get(d);
		Map<String,String> valuesMap = Maps.newHashMap();
		if(!CollectionUtils.isEmpty(valuesList)){
			valuesMap = Maps.newHashMapWithExpectedSize(valuesList.size());
			for(Map<String,String> r : valuesList){
				valuesMap.put(r.get("dt"), r.get("value"));
			}
		}
		for(String dt : everyDateList){
			if(!CollectionUtils.isEmpty(valuesMap)){
				if(StringUtils.isNotEmpty(valuesMap.get(dt))){
					values.add(valuesMap.get(dt));
				}else{
					values.add("0");
				}
			}else{
				values.add("0");
			}
		}
		vo.setNames(names);
		vo.setValues(values);
		return vo;
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
	
	public static void main(String[] args){
		List<String> xx = Lists.newArrayList("2019-07-23","2019-07-22");
		Collections.sort(xx,((o1, o2) -> {
        	return o1.compareTo(o2);
        }));
		System.out.println(xx.toString());
		
	}

}