package edu.indiana.soic.dsc.stream.debs.model;

import edu.indiana.soic.dsc.stream.debs.msg.DataReading;

public class Plug implements Entity{
  public int id;

  public CircularArray hourly;
  public CircularArray daily;

  int window1, window2;

  public Plug(int id, int window1, int window2) {
    if (window1 <= 0 || window2 <= 0) {
      throw new IllegalArgumentException();
    }

    this.id = id;
    this.window1 = window1;
    this.window2 = window2;

    hourly = new CircularArray(window1);
    daily = new CircularArray(window2);
  }

  @Override
  public void addReading(DataReading reading) {
    hourly.add(reading.value, reading.timeStamp);
    daily.add(reading.value, reading.timeStamp);
  }

  public float averageHourly() {
    return hourly.average();
  }

  public float averageDaily() {
    return daily.average();
  }

  public long hourlyStartTime() {
    return hourly.getStartTime();
  }

  public long hourlyEndTime() {
    return hourly.getEndTime();
  }

  public long dailyStartTime() {
    return hourly.getStartTime();
  }

  public long dailyEndTime() {
    return hourly.getEndTime();
  }
}
