package edu.indiana.soic.dsc.stream.debs.bolt;


import edu.indiana.soic.dsc.stream.debs.msg.PlugValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BoltUtils {
  public static void housePercentage(float average, HashMap<Integer, List<PlugValue>> dailyHousePlugs,
                               Map<Integer, Float> housePercentages) {
    for (Map.Entry<Integer, List<PlugValue>> e : dailyHousePlugs.entrySet()) {
      int count = 0;
      for (PlugValue p : e.getValue()) {
        if (p.value > average) {
          count++;
        }
      }
      housePercentages.put(e.getKey(), ((float) count / e.getValue().size()));
    }
  }

  public static void housePlugValues(ArrayList<PlugValue> aggrValues, HashMap<Integer, List<PlugValue>> dailyHousePlugs) {
    for (PlugValue p : aggrValues) {
      int h = p.house;
      List<PlugValue> plugValues;
      if (dailyHousePlugs.containsKey(h)) {
        plugValues = dailyHousePlugs.get(h);
      } else {
        plugValues = new ArrayList<>();
        dailyHousePlugs.put(h, plugValues);
      }
      plugValues.add(p);
    }
  }
}
