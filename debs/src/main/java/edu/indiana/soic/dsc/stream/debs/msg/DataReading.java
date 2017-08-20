package edu.indiana.soic.dsc.stream.debs.msg;

public class DataReading {
  public int id;
  public long timeStamp;
  public float value;
  public boolean property;
  public int plugId;
  public int householdId;
  public int houseId;
  public String line;

  public DataReading(int id, long timeStamp, float value, boolean property, int plugId, int householdId, int houseId) {
    this.id = id;
    this.timeStamp = timeStamp;
    this.value = value;
    this.property = property;
    this.plugId = plugId;
    this.householdId = householdId;
    this.houseId = houseId;
  }

  public DataReading(int id, long timeStamp, float value, boolean property, int plugId, int householdId, int houseId, String line) {
    this.id = id;
    this.timeStamp = timeStamp;
    this.value = value;
    this.property = property;
    this.plugId = plugId;
    this.householdId = householdId;
    this.houseId = houseId;
    this.line = line;
  }

  public DataReading() {
  }
}
