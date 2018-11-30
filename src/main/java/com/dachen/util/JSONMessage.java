package com.dachen.util;

import com.alibaba.fastjson.JSONObject;

public class JSONMessage extends JSONObject {

    public static final Object EMPTY_OBJECT = new Object();
    public static final int COMMON_ERROR = 100;//标准错误
    public static final int TOKEN_INVALID = 1030102;//token不存在
    public static final int TOKEN_ERROR = 100001;//token错误
    public static final int PARAM_ERROR = 100200;//token参数错误
    public static final int GUEST_TOKEN_ERROP = 1030104;//游客令牌错误
    public static final int IDENTITY_ERROR = 100300;//身份标识错误
    public static final int Failure = 0;//失败
    public static final int Success = 1;//成功
    private static final long serialVersionUID = 1L;

    public JSONMessage() {
    }

    public JSONMessage(int resultCode, String resultMsg) {
        setResultCode(resultCode);
        setResultMsg(resultMsg);
    }

    public JSONMessage(int resultCode, String resultMsg, String detailMsg) {
        setResultCode(resultCode);
        setResultMsg(resultMsg);
        setDetailMsg(detailMsg);
    }

    public JSONMessage(int resultCode, String resultMsg, Object data) {
        setResultCode(resultCode);
        setResultMsg(resultMsg);
        setData(data);
    }

    public JSONMessage(String groupCode, String serviceCode, String nodeCode,
        String resultMsg) {
        setResultCode(new StringBuffer().append(groupCode).append(serviceCode)
            .append(nodeCode).toString());
        setResultMsg(resultMsg);
    }

    public static JSONMessage success(String resultMsg) {
        return new JSONMessage(Success, resultMsg);
    }

    public static JSONMessage success() {
        return success(null, null);
    }

    public static JSONMessage success(Object data) {
        return success(null, data);
    }

    public static JSONMessage success(String resultMsg, Object data) {
        return new JSONMessage(Success, resultMsg, data);
    }

    public static JSONMessage failure(String resultMsg) {
        return new JSONMessage(Failure, resultMsg);
    }

    public static JSONMessage error(Exception e) {
        return new JSONMessage(1020101, "服务器繁忙，请稍后再试！", e.getMessage());
    }

    public Object getResultCode() {
        return get("resultCode");
    }

    public void setResultCode(Object resultCode) {
        put("resultCode", resultCode);
    }

    public String getResultMsg() {
        return getString("resultMsg");
    }

    public void setResultMsg(String resultMsg) {
        put("resultMsg", resultMsg);
    }

    public String getDetailMsg() {
        return getString("detailMsg");
    }

    public void setDetailMsg(String detailMsg) {
        put("detailMsg", detailMsg);
    }

    public Object getData() {
        return get("data");
    }

    public void setData(Object data) {
        put("data", data);
    }

}
