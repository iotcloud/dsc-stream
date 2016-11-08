package edu.indiana.soic.dsc.stream.perf;

public class SingleTrace {
  private long time;

  private long[]receiveTimes;

  public long getTime() {
    return time;
  }

  public long[] getReceiveTimes() {
    return receiveTimes;
  }

  public void setTime(long time) {
    this.time = time;
  }

  public void setReceiveTimes(long[] receiveTimes) {
    this.receiveTimes = receiveTimes;
  }
}
