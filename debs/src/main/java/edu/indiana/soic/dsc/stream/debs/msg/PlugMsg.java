package edu.indiana.soic.dsc.stream.debs.msg;

public class PlugMsg {
  private int id;
  private float averageHourly;
  private float averageDaily;

  public PlugMsg() {
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

  public PlugMsg(int id, float averageHourly, float averageDaily) {
    this.id = id;
    this.averageHourly = averageHourly;
    this.averageDaily = averageDaily;
  }

  public float getAverageDaily() {
    return averageDaily;
  }

  public void setAverageDaily(float averageDaily) {
    this.averageDaily = averageDaily;
  }
}
