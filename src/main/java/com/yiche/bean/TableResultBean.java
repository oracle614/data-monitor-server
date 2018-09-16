package com.yiche.bean;

public class TableResultBean  extends  RuleResultBean{

    private String count;

    private String  runningTime;

    private String finishTime;

    public String getCount() {
        return count;
    }

    public void setCount(String count) {
        this.count = count;
    }

    public String getRunningTime() {
        return runningTime;
    }

    public void setRunningTime(String runningTime) {
        this.runningTime = runningTime;
    }

    public String getFinishTime() {
        return finishTime;
    }

    public void setFinishTime(String finishTime) {
        this.finishTime = finishTime;
    }

    @Override
    public String toString() {
        return "TableResultBean{" +
                "count=" + count +
                ", runningTime='" + runningTime + '\'' +
                ", finishTime='" + finishTime + '\'' +
                '}';
    }
}
