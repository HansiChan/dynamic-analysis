package com.dachen.dynamicanalysis.pojo;

import java.util.List;

public class AnalysisLineChartVo {
    private List<String> x_axis ;
    private List<AnalysisListVo> series;

    public List<String> getX_axis() {
        return x_axis;
    }

    public void setX_axis(List<String> x_axis) {
        this.x_axis = x_axis;
    }

    public List<AnalysisListVo> getSeries() {
        return series;
    }

    public void setSeries(List<AnalysisListVo> series) {
        this.series = series;
    }
}
