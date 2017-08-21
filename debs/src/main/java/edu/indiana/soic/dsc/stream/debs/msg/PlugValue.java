package edu.indiana.soic.dsc.stream.debs.msg;

public class PlugValue {
  public float value;
  public int house;
  public int houseHold;
  public int plug;

  public PlugValue(float value, int house, int houseHold, int plug) {
    this.value = value;
    this.house = house;
    this.houseHold = houseHold;
    this.plug = plug;
  }

  public PlugValue() {
  }
}
