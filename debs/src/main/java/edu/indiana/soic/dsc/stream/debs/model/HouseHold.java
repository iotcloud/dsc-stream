package edu.indiana.soic.dsc.stream.debs.model;

import edu.indiana.soic.dsc.stream.debs.msg.DataReading;

import java.util.HashMap;
import java.util.Map;

public class HouseHold implements Entity {

  private Map<Integer, Plug> plugMap = new HashMap<>();

  @Override
  public void addReading(DataReading reading) {
    Plug plug = null;
    if (!plugMap.containsKey(reading.householdId)) {
      plug = new Plug(0, 3600, 3600 * 24);
      plugMap.put(reading.householdId, plug);
    } else {
      plug = plugMap.get(reading.householdId);
    }
    plug.addReading(reading);
  }

  public Plug getPlug(int plug) {
    return plugMap.get(plug);
  }
}
