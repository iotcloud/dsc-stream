package edu.indiana.soic.dsc.stream.debs.model;

import edu.indiana.soic.dsc.stream.debs.msg.DataReading;

public class Plug implements Entity{
  public int id;

  public CircularArray hourly;

  int window;

  public Plug(int id, int window) {
    if (window <= 0) {
      throw new IllegalArgumentException();
    }

    this.id = id;
    this.window = window;

    hourly = new CircularArray(window);
  }

  @Override
  public void addReading(DataReading reading) {
    hourly.add(reading.value, reading.timeStamp);
  }

  public long startTime() {
    return hourly.getStartTime();
  }

  public long endTime() {
    return hourly.getEndTime();
  }

  public Calculation calculate() {
    return new Calculation(hourly.sum(), hourly.noOfValues());
  }
}
