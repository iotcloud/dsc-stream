package edu.indiana.soic.dsc.stream.debs.msg;

public class HouseMsg {
  public int id;
  public float[] values;
  public int[] plugIds;
  public long startTs;
  public long endTs;

  public HouseMsg() {
  }

  public HouseMsg(int id, float[] values, int[] plugIds, long startTs, long endTs) {
    this.id = id;
    this.values = values;
    this.plugIds = plugIds;
    this.startTs = startTs;
    this.endTs = endTs;
  }

  public int getId() {
    return id;
  }

  public float[] getValues() {
    return values;
  }

  public int[] getPlugIds() {
    return plugIds;
  }

  public void setId(int id) {
    this.id = id;
  }

  public void setValues(float[] values) {
    this.values = values;
  }

  public void setPlugIds(int[] plugIds) {
    this.plugIds = plugIds;
  }

  public long getStartTs() {
    return startTs;
  }

  public long getEndTs() {
    return endTs;
  }

  public void setStartTs(int startTs) {
    this.startTs = startTs;
  }

  public void setEndTs(int endTs) {
    this.endTs = endTs;
  }
}
