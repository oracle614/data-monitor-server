package com.yiche.bean;

import java.util.List;
import java.util.Map;

public class MailJsonDataEntity {

    private String from;

    private List<String> primaryTo;

    private List<String> carbonCopy;

    private String subject;

    private Map<String,Object> data;

    private String userId;

    private String groupId;

    private String weixinReceiver;

    public String getFrom() {
        return from;
    }

    public void setFrom(String from) {
        this.from = from;
    }

    public List<String> getPrimaryTo() {
        return primaryTo;
    }

    public void setPrimaryTo(List<String> primaryTo) {
        this.primaryTo = primaryTo;
    }

    public List<String> getCarbonCopy() {
        return carbonCopy;
    }

    public void setCarbonCopy(List<String> carbonCopy) {
        this.carbonCopy = carbonCopy;
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public Map<String, Object> getData() {
        return data;
    }

    public void setData(Map<String, Object> data) {
        this.data = data;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getWeixinReceiver() {
        return weixinReceiver;
    }

    public void setWeixinReceiver(String weixinReceiver) {
        this.weixinReceiver = weixinReceiver;
    }

    @Override
    public String toString() {
        return "MailJsonDataEntity{" +
                "from='" + from + '\'' +
                ", primaryTo=" + primaryTo +
                ", carbonCopy=" + carbonCopy +
                ", subject='" + subject + '\'' +
                ", data=" + data +
                ", userId='" + userId + '\'' +
                ", groupId='" + groupId + '\'' +
                ", weixinReceiver='" + weixinReceiver + '\'' +
                '}';
    }
}
