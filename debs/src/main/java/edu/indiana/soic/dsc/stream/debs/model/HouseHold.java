package edu.indiana.soic.dsc.stream.debs.model;

import edu.indiana.soic.dsc.stream.debs.msg.DataReading;

import java.util.HashMap;
import java.util.Map;

public class HouseHold implements Entity {

  private Map<Integer, Plug> dailyPlugMap = new HashMap<>();
  private Map<Integer, Plug> hourlyPlugMap = new HashMap<>();

  public Map<Integer, Plug> getDailyPlugMap() {
    return dailyPlugMap;
  }

  public Map<Integer, Plug> getHourlyPlugMap() {
    return hourlyPlugMap;
  }

  @Override
  public void addReading(DataReading reading) {
    Plug dailyPlug;
    Plug hourlyPlug;
    if (!dailyPlugMap.containsKey(reading.plugId)) {
      dailyPlug = new Plug(reading.plugId, 3600 * 24);
      hourlyPlug = new Plug(reading.plugId, 3600);
      dailyPlugMap.put(reading.plugId, dailyPlug);
      hourlyPlugMap.put(reading.plugId, hourlyPlug);
    } else {
      dailyPlug = dailyPlugMap.get(reading.plugId);
      hourlyPlug = hourlyPlugMap.get(reading.plugId);
    }
    dailyPlug.addReading(reading);
    hourlyPlug.addReading(reading);
  }

  public Plug getPlug(int plug) {
    return dailyPlugMap.get(plug);
  }

  public Calculation calculateDaily() {
    float sum = 0;
    int no = 0;
    for (Map.Entry<Integer, Plug> e : dailyPlugMap.entrySet()) {
      Calculation c = e.getValue().calculate();
      sum += c.value;
      no += c.noOfItems;
    }
    return new Calculation(sum, no);
   }

  public Calculation calculateHourly() {
    float sum = 0;
    int no = 0;
    for (Map.Entry<Integer, Plug> e : hourlyPlugMap.entrySet()) {
      Calculation c = e.getValue().calculate();
      sum += c.value;
      no += c.noOfItems;
    }
    return new Calculation(sum, no);
  }
}
