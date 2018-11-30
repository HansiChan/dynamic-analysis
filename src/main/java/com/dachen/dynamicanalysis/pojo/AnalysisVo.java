package com.dachen.dynamicanalysis.pojo;

import java.io.Serializable;

public class AnalysisVo implements Serializable {
    private String dt;
    private String name;
    private String value;

    public String getDt() {
        return dt;
    }

    public void setDt(String dt) {
        this.dt = dt;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
