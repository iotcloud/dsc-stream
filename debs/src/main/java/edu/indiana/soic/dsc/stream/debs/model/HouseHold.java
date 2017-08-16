package edu.indiana.soic.dsc.stream.debs.model;

import edu.indiana.soic.dsc.stream.debs.msg.DataReading;

import java.util.HashMap;
import java.util.Map;

public class HouseHold implements Entity {

  private Map<Integer, Plug> plugMap = new HashMap<>();

  @Override
  public void addReading(DataReading reading) {
    Plug plug = null;
    if (!plugMap.containsKey(reading.plugId)) {
      plug = new Plug(reading.plugId, 3600, 3600 * 24);
      plugMap.put(reading.plugId, plug);
    } else {
      plug = plugMap.get(reading.plugId);
    }
    plug.addReading(reading);
  }

  public Plug getPlug(int plug) {
    return plugMap.get(plug);
  }
}
