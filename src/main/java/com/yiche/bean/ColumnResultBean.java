package com.yiche.bean;

public class ColumnResultBean  extends  RuleResultBean {

       private Integer nullValue;

       private Integer minValue;

       private Integer avgValue;

       private Integer emptyValue;

       private Integer specialValue;

       private Integer repeatValue;

       private Double regression;

       private Double movingAvg;

    public Integer getNullValue() {
        return nullValue;
    }

    public void setNullValue(Integer nullValue) {
        this.nullValue = nullValue;
    }

    public Integer getMinValue() {
        return minValue;
    }

    public void setMinValue(Integer minValue) {
        this.minValue = minValue;
    }

    public Integer getAvgValue() {
        return avgValue;
    }

    public void setAvgValue(Integer avgValue) {
        this.avgValue = avgValue;
    }

    public Integer getEmptyValue() {
        return emptyValue;
    }

    public void setEmptyValue(Integer emptyValue) {
        this.emptyValue = emptyValue;
    }

    public Integer getSpecialValue() {
        return specialValue;
    }

    public void setSpecialValue(Integer specialValue) {
        this.specialValue = specialValue;
    }

    public Integer getRepeatValue() {
        return repeatValue;
    }

    public void setRepeatValue(Integer repeatValue) {
        this.repeatValue = repeatValue;
    }

    public Double getRegression() {
        return regression;
    }

    public void setRegression(Double regression) {
        this.regression = regression;
    }

    public Double getMovingAvg() {
        return movingAvg;
    }

    public void setMovingAvg(Double movingAvg) {
        this.movingAvg = movingAvg;
    }


    @Override
    public String toString() {
        return "ColumnResultBean{" +
                "nullValue=" + nullValue +
                ", minValue=" + minValue +
                ", avgValue=" + avgValue +
                ", emptyValue=" + emptyValue +
                ", specialValue=" + specialValue +
                ", repeatValue=" + repeatValue +
                ", regression=" + regression +
                ", movingAvg=" + movingAvg +
                '}';
    }
}
