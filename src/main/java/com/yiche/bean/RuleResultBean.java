package com.yiche.bean;

public class RuleResultBean {
    private String value;

    private String valueCompare;

    private String scope;

    private String is_pass;

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getValueCompare() {
        return valueCompare;
    }

    public void setValueCompare(String valueCompare) {
        this.valueCompare = valueCompare;
    }

    public String getScope() {
        return scope;
    }

    public void setScope(String scope) {
        this.scope = scope;
    }

    public String getIs_pass() {
        return is_pass;
    }

    public void setIs_pass(String is_pass) {
        this.is_pass = is_pass;
    }

    @Override
    public String toString() {
        return "RuleResultBean{" +
                "value='" + value + '\'' +
                ", valueCompare='" + valueCompare + '\'' +
                ", scope='" + scope + '\'' +
                ", is_pass='" + is_pass + '\'' +
                '}';
    }
}
