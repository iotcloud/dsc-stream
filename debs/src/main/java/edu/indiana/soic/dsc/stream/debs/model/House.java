package edu.indiana.soic.dsc.stream.debs.model;

import edu.indiana.soic.dsc.stream.debs.msg.DataReading;

import java.util.HashMap;
import java.util.Map;

public class House implements Entity {
  private Map<Integer, HouseHold> houseHoldMap = new HashMap<>();

  public void addReading(DataReading reading) {
    HouseHold houseHold = null;
    if (!houseHoldMap.containsKey(reading.householdId)) {
      houseHold = new HouseHold();
      houseHoldMap.put(reading.householdId, houseHold);
    } else {
      houseHold = houseHoldMap.get(reading.householdId);
    }
    houseHold.addReading(reading);
  }

  public Plug getPlug(int houseHold, int plug) {
    return houseHoldMap.get(houseHold).getPlug(plug);
  }
}
