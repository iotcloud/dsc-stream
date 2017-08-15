package edu.indiana.soic.dsc.stream.debs.msg;

public class OutputMessage {
  public long tsStart;
  public long tsEnd;
  public int houseId;
  public int percentage;

  public OutputMessage(long tsStart, long tsEnd, int houseId, int percentage) {
    this.tsStart = tsStart;
    this.tsEnd = tsEnd;
    this.houseId = houseId;
    this.percentage = percentage;
  }

  public OutputMessage() {
  }
}
