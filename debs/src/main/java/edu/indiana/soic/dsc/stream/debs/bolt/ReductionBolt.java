package edu.indiana.soic.dsc.stream.debs.bolt;

import com.esotericsoftware.kryo.Kryo;
import com.twitter.heron.api.bolt.BaseRichBolt;
import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Tuple;
import edu.indiana.soic.dsc.stream.debs.Constants;
import edu.indiana.soic.dsc.stream.debs.DebsUtils;
import edu.indiana.soic.dsc.stream.debs.model.Plug;
import edu.indiana.soic.dsc.stream.debs.msg.PlugMsg;
import edu.indiana.soic.dsc.stream.debs.msg.PlugValue;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class ReductionBolt extends BaseRichBolt {
  private static Logger LOG = Logger.getLogger(ReductionBolt.class.getName());

  private String fileName;

  PrintWriter hourlyBufferWriter;
  PrintWriter dailyBufferWriter;

  boolean debug;
  int pi;
  private Kryo kryo;

  @Override
  public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
    fileName = (String) map.get(Constants.ARGS_OUT_FILE);
    openFile(fileName + "_" + topologyContext.getThisTaskId());
    this.debug = (boolean) map.get(Constants.ARGS_DEBUG);
    this.pi = (int) map.get(Constants.ARGS_PRINT_INTERVAL);
    kryo = new Kryo();
    DebsUtils.registerClasses(kryo);
  }

  @Override
  public void execute(Tuple tuple) {
    Object output = tuple.getValueByField(Constants.PLUG_FIELD);
    PlugMsg msg = (PlugMsg) DebsUtils.deSerialize(kryo, (byte[]) output, PlugMsg.class);

    float dailyAverage = msg.dailySum / msg.noOfDailyMsgs;
    float hourlyAverage = msg.hourlySum / msg.noOfHourlyMsgs;

    HashMap<Integer, List<PlugValue>> dailyHousePlugs = new HashMap<>();
    HashMap<Integer, List<PlugValue>> hourlyHousePlugs = new HashMap<>();
    housePlugValues(msg.aggregatedDailyPlugs, dailyHousePlugs);
    housePlugValues(msg.aggregatedHourlyPlugs, hourlyHousePlugs);

    Map<Integer, Float> hourlyHousePercentages = new HashMap<>();
    Map<Integer, Float> dailyHousePercentages = new HashMap<>();
    housePercentage(hourlyAverage, hourlyHousePlugs, hourlyHousePercentages);
    housePercentage(dailyAverage, dailyHousePlugs, dailyHousePercentages);

    String hourly = "";
    for (Map.Entry<Integer, Float> e : hourlyHousePercentages.entrySet()) {
      hourly += e.getKey() + ":" + e.getValue() + ", ";
    }

    String daily = "";
    for (Map.Entry<Integer, Float> e : dailyHousePercentages.entrySet()) {
      daily += e.getKey() + ":" + e.getValue() + ", ";
    }

    LOG.info(msg.noOfDailyMsgs + "," + msg.dailySum / msg.noOfDailyMsgs + "," +
        msg.noOfHourlyMsgs + ", " + msg.hourlySum / msg.noOfHourlyMsgs + ", " + msg.aggregatedHourlyPlugs.size() + " hourly: " + hourly + " daily: " + daily);
  }

  private void housePercentage(float average, HashMap<Integer, List<PlugValue>> dailyHousePlugs,
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

  private void housePlugValues(ArrayList<PlugValue> aggrValues, HashMap<Integer, List<PlugValue>> dailyHousePlugs) {
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

  private void openFile(String openFile) {
    try {
      hourlyBufferWriter = new PrintWriter(new BufferedWriter(new FileWriter(openFile + "_hourly.csv")));
      dailyBufferWriter = new PrintWriter(new BufferedWriter(new FileWriter(openFile + "_daily.csv")));
    } catch (FileNotFoundException e) {
      throw new RuntimeException("Failed to open file" + openFile, e);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

  }
}
