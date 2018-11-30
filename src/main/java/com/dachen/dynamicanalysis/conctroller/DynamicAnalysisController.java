package com.dachen.dynamicanalysis.conctroller;


import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.dachen.dynamicanalysis.dataprovider.CommonDataProvider;
import com.dachen.dynamicanalysis.service.ExcelExportService;
import com.dachen.dynamicanalysis.service.ImpalaDataService;
import com.dachen.util.JSONMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletResponse;
import java.util.LinkedList;
import java.util.List;

@RestController
@RequestMapping("/dynamic")
public class DynamicAnalysisController {

    @Autowired
    private CommonDataProvider commonProvider;

    @Autowired
    private ImpalaDataService dataService;

    @Autowired
    private ExcelExportService excelService;

    @RequestMapping("/indexes")
    public JSONMessage index() {

        Object list  = commonProvider.getIndexes();
        return JSONMessage.success("Request success", list);
    }

    @RequestMapping("/dimensions")
    public JSONMessage dimension() throws Exception {
        Object list  = commonProvider.getDimensions();
        return JSONMessage.success("Request success", list);
    }

    @RequestMapping("/filter")
    public JSONMessage filter(@RequestParam(name = "dimension", required = false) String dimension) throws Exception {
        Object res = dataService.filter(dimension);
        return JSONMessage.success("Request success", res);
    }

    @RequestMapping(value = "/query")
    public JSONMessage query(@RequestParam(name = "module") String module,
                             @RequestParam(name = "dimension", required = false) String dimension,
                             @RequestParam(name = "dimension_sub", required = false) String dimension_sub,
//                           @RequestParam(name = "analysis_index", required = false) String analysis_index,
                             @RequestParam(name = "chartType") String chartType,
                             @RequestParam(name = "filter_condition", required = false) String filter_condition,
                             @RequestParam(name = "dimension_date", required = false) String dimension_date,
                             @RequestParam(name = "begin_date", required = false) String begin_date,
                             @RequestParam(name = "end_date", required = false) String end_date
    ) throws Exception {

        String dateSql = "";
        if ("hour".equals(dimension_date)) {
            end_date = begin_date;
            dateSql = "hours";
        } else if ("day".equals(dimension_date)) {
            dateSql = "days";
        } else if ("week".equals(dimension_date)) {
            dateSql = "weeks";
        } else if ("month".equals(dimension_date)){
            dateSql = "months";
        }
        String sqlWhere = "";
        if (!"".equals(filter_condition) && filter_condition != null) {
            sqlWhere = sqlWhere + filter_condition.replace("where", "and (") + ") ";
        }

        String sqlTable = "";
        if ("active".equals(module)) {
            sqlTable = "dw_user_login";
        } else if ("new".equals(module)) {
            sqlTable = "dw_user_register";
        } else if ("authenticating".equals(module)) {
            sqlTable = "dw_user_certify";
        } else if ("authenticated".equals(module)) {
            sqlTable = "dw_user_check";
            sqlWhere = sqlWhere + " and status='正常'";
        }

        String sqlJoin = "(select * from dw."+sqlTable+")";


        Object res = null;
        if (chartType == "lines" || "lines".equals(chartType)) {
            res = dataService.queryLineChart(module, dimension, dimension_sub, begin_date, end_date, dateSql, sqlWhere, sqlJoin);
        } else if (chartType == "pie" || "pie".equals(chartType)) {
            res = dataService.proportion(module, dimension, dimension_sub, filter_condition, begin_date, end_date, sqlJoin);
        }
        return JSONMessage.success("Request success", res);
    }

    @RequestMapping("/download")
    public void DownloadExcel(@RequestParam(name = "module") String module,
                              @RequestParam(name = "dimension", required = false) String dimension,
                              @RequestParam(name = "chartType") String chartType,
                              @RequestParam(name = "res") String res,
                              HttpServletResponse response) {

        String sheetName = module;
        List<String> fields = new LinkedList<>();
        List<String> firstCell = new LinkedList<>();
        List<String> secondCell = new LinkedList<>();

        JSONObject jo = JSONObject.parseObject(res);
        JSONObject data = jo.getJSONObject("data");
        JSONArray series = data.getJSONArray("series");
        if ("lines".equals(chartType)) {
            firstCell = data.getJSONArray("x_axis").toJavaList(String.class);
            fields.add("时间");
            for (int i = 0; i < series.size(); i++) {
                String x = series.getJSONObject(i).getJSONArray("names").get(0).toString();
                String values = series.getJSONObject(i).getJSONArray("values").toString();
                fields.add(x);
                secondCell.add(values);
            }
        } else {
            for (int i = 0; i < series.size(); i++) {
                String name = series.getJSONObject(i).getJSONArray("names").get(0).toString();
                String value = series.getJSONObject(i).getJSONArray("value").get(0).toString();
                firstCell.add(name);
                secondCell.add(value);
            }
        }

        excelService.export(dimension, chartType, fields, sheetName, firstCell, secondCell, response);
    }

        /*@RequestMapping("/proportion")
    public JSONMessage proportion(@RequestParam(name = "module", required = false) String module,
                                  @RequestParam(name = "dimension", required = false) String dimension,
                                  @RequestParam(name = "dimension_sub", required = false) String dimension_sub,
                                  @RequestParam(name = "analysis_index", required = false) String analysis_index,
                                  @RequestParam(name = "filter_condition", required = false) String filter_condition,
                                  @RequestParam(name = "user_attr", required = false) String user_attr,
                                  @RequestParam(name = "begin_date", required = false) String begin_date,
                                  @RequestParam(name = "end_date", required = false) String end_date) throws Exception {
        Map<String, Object> res = dataService.proportion(module, dimension, dimension_sub, analysis_index, filter_condition, user_attr, begin_date, end_date);
        return JSONMessage.success("Request success", res);
    }*/

}