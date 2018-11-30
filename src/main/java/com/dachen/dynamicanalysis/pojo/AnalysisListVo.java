package com.dachen.dynamicanalysis.pojo;

import java.io.Serializable;
import java.util.List;

public class AnalysisListVo implements Serializable {
    private List<String> names;
    private List<String> values;

    public List<String> getValues() {
        return values;
    }

    public void setValues(List<String> values) {
        this.values = values;
    }

    public List<String> getNames() {
        return names;
    }

    public void setNames(List<String> names) {
        this.names = names;
    }

}
