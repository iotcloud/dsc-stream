package edu.indiana.soic.dsc.stream.debs.msg;

public class PlugMsg {
  public int id;
  public float averageHourly;
  public float averageDaily;
  public long hourlyStartTs;
  public long hourlyEndTs;
  public long dailyStartTs;
  public long dailyEndTs;
  public int noOfHourlyMsgs = 3600;
  public int noOfDailyMsgs = 3600 * 24;

  public PlugMsg() {
  }

  public PlugMsg(int id, float averageHourly, float averageDaily,
                 long hourlyStartTs, long hourlyEndTs, long dailyStartTs, long dailyEndTs) {
    this.id = id;
    this.averageHourly = averageHourly;
    this.averageDaily = averageDaily;
    this.hourlyStartTs = hourlyStartTs;
    this.hourlyEndTs = hourlyEndTs;
    this.dailyEndTs = dailyEndTs;
    this.dailyStartTs = dailyStartTs;
  }

  public int getId() {
    return id;
  }

  public float getAverageHourly() {
    return averageHourly;
  }

  public void setId(int id) {
    this.id = id;
  }

  public void setAverageHourly(float averageHourly) {
    this.averageHourly = averageHourly;
  }

  public float getAverageDaily() {
    return averageDaily;
  }

  public void setAverageDaily(float averageDaily) {
    this.averageDaily = averageDaily;
  }

  public void setNoOfHourlyMsgs(int noOfHourlyMsgs) {
    this.noOfHourlyMsgs = noOfHourlyMsgs;
  }

  public void setNoOfDailyMsgs(int noOfDailyMsgs) {
    this.noOfDailyMsgs = noOfDailyMsgs;
  }
}
