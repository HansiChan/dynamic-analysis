package com.dachen.dynamicanalysis.dto;

import java.util.LinkedList;
import java.util.List;

public enum  Dimension {

    DOCTOR_DEPATMENTS("科室","doctor_departments"),
    DOCTOR_TITLE("职称","doctor_title"),
    DOCTOR_PROVINCE("省份","doctor_province"),
    LEVEL("医院等级","level"),
    STATUS("账号状态","status"),
    USER_LEVEL("身份状态","userLevel"),
    MODEL("手机设备","model"),
    SOURCE_SOURCETYPE("注册来源","source_sourcetype");

    private String name;
    private String index;

    // 构造方法
    Dimension(String name, String index) {
        this.name = name;
        this.index = index;
    }

    // 普通方法
    public static String getName(String  index) {
        for (Dimension i : Dimension.values()) {
            if (i.getIndex().equals(index)) {
                return i.name;
            }
        }
        return null;
    }

    public static List<String> getNameList(){
        List<String> list = new LinkedList<>();
        for (Dimension i : Dimension.values()) {
            list.add(i.getName());
        }
        return list;
    }

    public static List<String> getInedxList(){
        List<String> list = new LinkedList<>();
        for (Dimension i : Dimension.values()) {
            list.add(i.getIndex());
        }
        return list;
    }

    // get set 方法
    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }
    public String getIndex() {
        return index;
    }
    public void setIndex(String index) {
        this.index = index;
    }
}
