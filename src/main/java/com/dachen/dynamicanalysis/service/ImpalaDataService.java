package com.dachen.dynamicanalysis.service;

import com.dachen.dynamicanalysis.dataprovider.DynamicAnalysisProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class ImpalaDataService {

    @Autowired
    DynamicAnalysisProvider dynamicProvider;

    public Object proportion(String module, String dimension, String dimension_sub, String filter_condition, String begin_date, String end_date, String sqlJoin,String sqlTable) throws Exception {
        Object proMap = dynamicProvider.proportion(module, dimension, dimension_sub, filter_condition, begin_date, end_date, sqlJoin,sqlTable);
        return proMap;
    }

    public Object queryLineChart(String module, String dimension, String dimension_sub, String begin_date, String end_date, String dateSql, String sqlWhere, String sqlJoin) throws Exception {
        Object vo = dynamicProvider.getActiveLineChart(module, dimension, dimension_sub, begin_date, end_date, dateSql, sqlWhere, sqlJoin);
        return vo;
    }

}
