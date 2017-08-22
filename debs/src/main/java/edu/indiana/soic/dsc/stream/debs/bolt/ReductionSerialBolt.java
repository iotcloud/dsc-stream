package edu.indiana.soic.dsc.stream.debs.bolt;

import com.esotericsoftware.kryo.Kryo;
import com.twitter.heron.api.bolt.BaseRichBolt;
import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Tuple;
import edu.indiana.soic.dsc.stream.debs.Constants;
import edu.indiana.soic.dsc.stream.debs.DebsUtils;
import edu.indiana.soic.dsc.stream.debs.msg.PlugMsg;
import edu.indiana.soic.dsc.stream.debs.msg.PlugValue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class ReductionSerialBolt extends BaseRichBolt {
  private static Logger LOG = Logger.getLogger(ReductionSerialBolt.class.getName());

  private int thisTaskId;

  private Kryo kryo;

  // tasId, <plugId, PlugMsg>
  private Map<Integer, TaskPlugMessages> plugMessages;
  private OutputCollector outputCollector;
  private boolean debug;
  private int pi;
  private int count = 0;

  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    List<Integer> list = topologyContext.getComponentTasks(Constants.MEDIAN_BOLT);

    thisTaskId = topologyContext.getThisTaskId();
    plugMessages = new HashMap<>();
    for (int i : list) {
      plugMessages.put(i, new TaskPlugMessages());
    }
    this.outputCollector = outputCollector;
    this.kryo = new Kryo();
    DebsUtils.registerClasses(kryo);
    this.debug = (boolean) map.get(Constants.ARGS_DEBUG);
    this.pi = (int) map.get(Constants.ARGS_PRINT_INTERVAL);
  }

  @Override
  public void execute(Tuple tuple) {
    Object object = tuple.getValueByField(Constants.PLUG_FIELD);
    Object time = tuple.getValueByField(Constants.TIME_FIELD);

    if (debug && count % pi == 0) {
      LOG.info("Got message from " + tuple.getSourceTask());
    }

    PlugMsg plugMsg = (PlugMsg) DebsUtils.deSerialize(kryo, (byte [])object, PlugMsg.class);
    TaskPlugMessages taskPlugMessages = plugMessages.get(tuple.getSourceTask());

    taskPlugMessages.plugMsgs.add(plugMsg);
    taskPlugMessages.times.add((Long) time);

    PlugMsg msg = processTaskPlugMessages();

    if (msg == null) {
      return;
    }

    float dailyAverage = msg.dailySum / msg.noOfDailyMsgs;
    float hourlyAverage = msg.hourlySum / msg.noOfHourlyMsgs;

    HashMap<Integer, List<PlugValue>> dailyHousePlugs = new HashMap<>();
    HashMap<Integer, List<PlugValue>> hourlyHousePlugs = new HashMap<>();
    BoltUtils.housePlugValues(msg.aggregatedDailyPlugs, dailyHousePlugs);
    BoltUtils.housePlugValues(msg.aggregatedHourlyPlugs, hourlyHousePlugs);

    Map<Integer, Float> hourlyHousePercentages = new HashMap<>();
    Map<Integer, Float> dailyHousePercentages = new HashMap<>();
    BoltUtils.housePercentage(hourlyAverage, hourlyHousePlugs, hourlyHousePercentages);
    BoltUtils.housePercentage(dailyAverage, dailyHousePlugs, dailyHousePercentages);

    String hourly = "";
    for (Map.Entry<Integer, Float> e : hourlyHousePercentages.entrySet()) {
      hourly += e.getKey() + ":" + e.getValue() + ", ";
    }

    String daily = "";
    for (Map.Entry<Integer, Float> e : dailyHousePercentages.entrySet()) {
      daily += e.getKey() + ":" + e.getValue() + ", ";
    }
    count++;

    LOG.info(msg.noOfDailyMsgs + "," + msg.dailySum / msg.noOfDailyMsgs + "," +
        msg.noOfHourlyMsgs + ", " + msg.hourlySum / msg.noOfHourlyMsgs + ", " +
        msg.aggregatedHourlyPlugs.size() + " hourly: " + hourly + " daily: " + daily);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

  }

  private PlugMsg processTaskPlugMessages() {
    if (checkState()) {
      if (debug && count % pi == 0) {
        LOG.info("Good state - emitting");
      }
      PlugMsg plugMsg = reduceMessages();
      removeFirst();

      return plugMsg;
    }
    return null;
  }

  private boolean checkState() {
    for (Map.Entry<Integer, TaskPlugMessages> e : plugMessages.entrySet()) {
      TaskPlugMessages tpm = e.getValue();

      if (tpm.plugMsgs.size() < 1) {
        return false;
      }
    }
    return true;
  }

  private void removeFirst() {
    for (Map.Entry<Integer, TaskPlugMessages> e : plugMessages.entrySet()) {
      e.getValue().removeFirst();
    }
  }

  private PlugMsg reduceMessages() {
    PlugMsg aggrPlugMsg = new PlugMsg();

    for (Map.Entry<Integer, TaskPlugMessages> e : plugMessages.entrySet()) {
      TaskPlugMessages tpm = e.getValue();
      PlugMsg plugMsg = tpm.plugMsgs.get(0);

      aggrPlugMsg.aggregatedHourlyPlugs.addAll(plugMsg.aggregatedHourlyPlugs);
      aggrPlugMsg.aggregatedDailyPlugs.addAll(plugMsg.aggregatedDailyPlugs);
      aggrPlugMsg.dailySum += plugMsg.dailySum;
      aggrPlugMsg.hourlySum += plugMsg.hourlySum;
      aggrPlugMsg.noOfDailyMsgs += plugMsg.noOfDailyMsgs;
      aggrPlugMsg.noOfHourlyMsgs += plugMsg.noOfHourlyMsgs;
    }
    aggrPlugMsg.taskId = thisTaskId;
    return aggrPlugMsg;
  }
}
