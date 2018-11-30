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

    public List<Map<String, String>> getDimensions() {
        List<Map<String, String>> aList = new ArrayList<>();
        List<String> dList = new ArrayList<>();
        List<String> tList = new ArrayList<>();

        dList.addAll(Dimension.getInedxList());
        tList.addAll(Dimension.getNameList());

        for (int i = 0; i < dList.size(); i++) {
            Map<String, String> eMap = new LinkedHashMap();
            eMap.put("dimension_sub", dList.get(i));
            eMap.put("text", tList.get(i));
            aList.add(eMap);
        }
        return aList;
    }

}
