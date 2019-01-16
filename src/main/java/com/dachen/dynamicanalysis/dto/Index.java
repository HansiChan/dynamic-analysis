package com.dachen.dynamicanalysis.dto;

import java.util.LinkedList;
import java.util.List;


public enum  Index {

    NEW("新增用户数", "new"),
    ACTIVE("活跃用戶数", "active"),
    AUTHENTICATING("提交认证用户数", "authenticating"),
    AUTHENTICATED("认证通过用户数", "autenticated");

    private String name;
    private String index;

    // 构造方法
    Index(String name, String index) {
        this.name = name;
        this.index = index;
    }

    // 普通方法
    public static String getName(String index) {
        for (Index i : Index.values()) {
            if (i.getIndex().equals(index)) {
                return i.name;
            }
        }
        return null;
    }

    public static List<String> getNameList() {
        List<String> list = new LinkedList<>();
        for (Index i : Index.values()) {
            list.add(i.getName());
        }
        return list;
    }

    public static List<String> getInedxList() {
        List<String> list = new LinkedList<>();
        for (Index i : Index.values()) {
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
