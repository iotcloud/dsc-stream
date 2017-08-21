package edu.indiana.soic.dsc.stream.debs.msg;

import java.util.ArrayList;

public class PlugMsg {
  public int taskId;
  public float hourlySum;
  public float dailySum;

  public int noOfHourlyMsgs;
  public int noOfDailyMsgs;
  public ArrayList<PlugValue> aggregatedHourlyPlugs = new ArrayList<>();
  public ArrayList<PlugValue> aggregatedDailyPlugs = new ArrayList<>();
  public String line;

  public PlugMsg() {
  }

  public PlugMsg(int id, float hourlySum, float dailySum, int noOfHourlyMsgs,
                 int noOfDailyMsgs, ArrayList<PlugValue> aggregatedHourlyPlugs,
                 ArrayList<PlugValue> aggregatedDailyPlugs) {
    this.taskId = id;
    this.hourlySum = hourlySum;
    this.dailySum = dailySum;
    this.noOfHourlyMsgs = noOfHourlyMsgs;
    this.noOfDailyMsgs = noOfDailyMsgs;
    this.aggregatedHourlyPlugs = aggregatedHourlyPlugs;
    this.aggregatedDailyPlugs = aggregatedDailyPlugs;
  }

  public PlugMsg(int id, float hourlySum, float dailySum, int noOfHourlyMsgs,
                 int noOfDailyMsgs, ArrayList<PlugValue> aggregatedPlugs,
                 ArrayList<PlugValue> aggregatedDailyPlugs, String line) {
    this.taskId = id;
    this.hourlySum = hourlySum;
    this.dailySum = dailySum;
    this.noOfHourlyMsgs = noOfHourlyMsgs;
    this.noOfDailyMsgs = noOfDailyMsgs;
    this.aggregatedHourlyPlugs = aggregatedPlugs;
    this.aggregatedDailyPlugs = aggregatedDailyPlugs;
    this.line = line;
  }

  public int getTaskId() {
    return taskId;
  }

  public float getHourlySum() {
    return hourlySum;
  }

  public void setTaskId(int taskId) {
    this.taskId = taskId;
  }

  public void setHourlySum(float hourlySum) {
    this.hourlySum = hourlySum;
  }

  public float getDailySum() {
    return dailySum;
  }

  public void setDailySum(float dailySum) {
    this.dailySum = dailySum;
  }

  public void setNoOfHourlyMsgs(int noOfHourlyMsgs) {
    this.noOfHourlyMsgs = noOfHourlyMsgs;
  }

  public void setNoOfDailyMsgs(int noOfDailyMsgs) {
    this.noOfDailyMsgs = noOfDailyMsgs;
  }
}
