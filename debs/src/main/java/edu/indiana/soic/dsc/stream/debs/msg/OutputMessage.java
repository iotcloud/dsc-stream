package edu.indiana.soic.dsc.stream.debs.msg;

public class OutputMessage {
  public long tsStart;
  public long tsEnd;
  public int houseId;
  public int percentage;
  public boolean daily;

  public OutputMessage(long tsStart, long tsEnd, int houseId, int percentage, boolean daily) {
    this.tsStart = tsStart;
    this.tsEnd = tsEnd;
    this.houseId = houseId;
    this.percentage = percentage;
    this.daily = daily;
  }

  public OutputMessage() {
  }

  @Override
  public String toString() {
    return "tsStart=" + tsStart +
        ", tsEnd=" + tsEnd +
        ", houseId=" + houseId +
        ", percentage=" + percentage +
        ", daily=" + daily;
  }
}
