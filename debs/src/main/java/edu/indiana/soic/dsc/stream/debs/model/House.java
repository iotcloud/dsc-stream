package edu.indiana.soic.dsc.stream.debs.model;

import edu.indiana.soic.dsc.stream.debs.msg.DataReading;
import edu.indiana.soic.dsc.stream.debs.msg.PlugValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class House implements Entity {
  private Map<Integer, HouseHold> houseHoldMap = new HashMap<>();

  private int id;

  public House(int id) {
    this.id = id;
  }

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

  public List<String> getPlugIds() {
    List<String> plugs = new ArrayList<>();
    for (Map.Entry<Integer, HouseHold> e : houseHoldMap.entrySet()) {
      for (Map.Entry<Integer, Plug> hhe : e.getValue().getDailyPlugMap().entrySet()) {
        plugs.add(id + "_" + e.getKey() + "_" + hhe.getKey());
      }
    }
    return plugs;
  }

  public List<PlugValue>  getHourlyPlugValues() {
    List<PlugValue> plugs = new ArrayList<>();
    for (Map.Entry<Integer, HouseHold> e : houseHoldMap.entrySet()) {
      for (Map.Entry<Integer, Plug> hhe : e.getValue().getHourlyPlugMap().entrySet()) {
        Calculation calculate = hhe.getValue().calculate();
        plugs.add(new PlugValue(calculate.value / calculate.noOfItems, id, e.getKey(), hhe.getKey()));
      }
    }
    return plugs;
  }

  public List<PlugValue> getDailyPlugValues() {
    List<PlugValue> plugs = new ArrayList<>();
    for (Map.Entry<Integer, HouseHold> e : houseHoldMap.entrySet()) {
      for (Map.Entry<Integer, Plug> hhe : e.getValue().getDailyPlugMap().entrySet()) {
        Calculation calculate = hhe.getValue().calculate();
        plugs.add(new PlugValue(calculate.value / calculate.noOfItems, id, e.getKey(), hhe.getKey()));
      }
    }
    return plugs;
  }

  public Calculation calculateHourly() {
    float sum = 0;
    int no = 0;
    for (Map.Entry<Integer, HouseHold> e : houseHoldMap.entrySet()) {
      Calculation c = e.getValue().calculateHourly();
      sum += c.value;
      no += c.noOfItems;
    }
    return new Calculation(sum, no);
  }

  public Calculation calculateDaily() {
    float sum = 0;
    int no = 0;
    for (Map.Entry<Integer, HouseHold> e : houseHoldMap.entrySet()) {
      Calculation c = e.getValue().calculateDaily();
      sum += c.value;
      no += c.noOfItems;
    }
    return new Calculation(sum, no);
  }
}
