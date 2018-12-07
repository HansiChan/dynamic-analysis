package com.dachen.dynamicanalysis.dataprovider;


import com.dachen.dynamicanalysis.dto.Dimension;
import com.dachen.dynamicanalysis.dto.Index;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Repository
public class CommonDataProvider {



    public List<Map<String, String>> getIndexes() {
        List<Map<String, String>> aList = new ArrayList<>();
        List<String> mList = new ArrayList<>();
        List<String> tList = new ArrayList<>();

        mList.addAll(Index.getInedxList());
        tList.addAll(Index.getNameList());

        for (int i = 0; i < mList.size(); i++) {
            Map<String, String> eMap = new LinkedHashMap<>();
            eMap.put("module", mList.get(i));
            eMap.put("text", tList.get(i));
            aList.add(eMap);
        }
        return aList;
    }

    public List<Map<String, String>> getDimensions(String module) {
        List<Map<String, String>> aList = new ArrayList<>();
        List<String> dList = new ArrayList<>();
        List<String> tList = new ArrayList<>();

        if ("new".equals(module)) {
            dList.add("doctor_departments");
            dList.add("doctor_title");
            dList.add("doctor_province");
            dList.add("level");
            dList.add("source_sourcetype");

            tList.add("科室");
            tList.add("职称");
            tList.add("省份");
            tList.add("医院等级");
            tList.add("注册来源");
        } else if ("active".equals(module)) {
            dList.add("doctor_departments");
            dList.add("doctor_title");
            dList.add("doctor_province");
            dList.add("level");
            dList.add("status");
            dList.add("model");
            dList.add("source_sourcetype");

            tList.add("科室");
            tList.add("职称");
            tList.add("省份");
            tList.add("医院等级");
            tList.add("账号状态");
            tList.add("手机设备");
            tList.add("注册来源");
        } else if ("authenticating".equals(module) || "authenticated".equals(module)) {
            dList.add("doctor_departments");
            dList.add("doctor_title");
            dList.add("doctor_province");
            dList.add("level");
            dList.add("model");
            dList.add("source_sourcetype");

            tList.add("科室");
            tList.add("职称");
            tList.add("省份");
            tList.add("医院等级");
            tList.add("手机设备");
            tList.add("注册来源");
        }  else {
            dList.addAll(Dimension.getInedxList());
            tList.addAll(Dimension.getNameList());
        }

        for (int i = 0; i < dList.size(); i++) {
            Map<String, String> eMap = new LinkedHashMap();
            eMap.put("dimension_sub", dList.get(i));
            eMap.put("text", tList.get(i));
            aList.add(eMap);
        }
        return aList;
    }

}
