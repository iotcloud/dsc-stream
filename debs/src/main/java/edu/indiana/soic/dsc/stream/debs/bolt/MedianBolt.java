package edu.indiana.soic.dsc.stream.debs.bolt;

import com.esotericsoftware.kryo.Kryo;
import com.twitter.heron.api.bolt.BaseRichBolt;
import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Tuple;
import edu.indiana.soic.dsc.stream.debs.Constants;
import edu.indiana.soic.dsc.stream.debs.DebsUtils;
import edu.indiana.soic.dsc.stream.debs.model.House;
import edu.indiana.soic.dsc.stream.debs.model.Plug;
import edu.indiana.soic.dsc.stream.debs.msg.DataReading;
import edu.indiana.soic.dsc.stream.debs.msg.PlugMsg;

import java.util.*;
import java.util.logging.Logger;

public class MedianBolt extends BaseRichBolt {
  private static Logger LOG = Logger.getLogger(MedianBolt.class.getName());

  private Map<Integer, House> houses;

  private OutputCollector outputCollector;

  private Kryo kryo;
  
  private int thisTaskId;

  private boolean debug;

  private int pi;

  private int count = 0;

  private int removeTotalCount = 0;

  private int removeTimeCount = 0;

  // plugid, plugmessage
  private Map<String, PlugsMessages> plugMsgs;

  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    houses = new HashMap<>();
    plugMsgs = new HashMap<>();
    this.outputCollector = outputCollector;
    kryo = new Kryo();
    DebsUtils.registerClasses(kryo);
    thisTaskId = topologyContext.getThisTaskId();
    this.debug = (boolean) map.get(Constants.ARGS_DEBUG);
    this.pi = (int) map.get(Constants.ARGS_PRINT_INTERVAL);
  }

  @Override
  public void execute(Tuple tuple) {
    Object input = tuple.getValueByField(Constants.DATA_FIELD);
    Object time = tuple.getValueByField(Constants.TIME_FIELD);
    String plugId = (String)tuple.getValueByField(Constants.PLUG_ID_FILED);

    DataReading reading = (DataReading) DebsUtils.deSerialize(kryo, (byte [])input, DataReading.class);
    House house;
    if (houses.containsKey(reading.houseId)) {
      house = houses.get(reading.houseId);
    } else {
      house = new House();
      houses.put(reading.houseId, house);
    }
    house.addReading(reading);

    Plug plug = house.getPlug(reading.householdId, reading.plugId);
    PlugMsg plugMsg = createPlugMsg(reading, plug);
    if (debug && count % pi == 0) {
      LOG.info("Got message : " + plugId + " line: " + plugMsg.line + " source: " + tuple.getSourceTask());
    }
    PlugsMessages plugsMessages = plugMsgs.get(plugId);
    if (plugsMessages == null) {
      plugsMessages = new PlugsMessages();
      plugMsgs.put(plugId, plugsMessages);
    }
    plugsMessages.add((Long) time, plugMsg.dailyEndTs, plugMsg);

    processTaskPlugMessages();

    count++;
  }

  private PlugMsg createPlugMsg(DataReading reading, Plug plug) {
    float averageHourly = plug.averageHourly();
    float averageDaily = plug.averageDaily();
    long hourlyStart = plug.hourlyStartTime();
    long hourlyEnd = plug.hourlyEndTime();
    long dailyStart = plug.dailyStartTime();
    long dailyEnd = plug.dailyEndTime();
    ArrayList<Integer> aggrs = new ArrayList<>();
    aggrs.add(reading.plugId);

    return new PlugMsg(plug.id, averageHourly, averageDaily, hourlyStart,
        hourlyEnd, dailyStart, dailyEnd, aggrs, reading.line);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declareStream(Constants.PLUG_REDUCE_STREAM,
        new Fields(Constants.TIME_FIELD, Constants.PLUG_FIELD));

    outputFieldsDeclarer.declareStream(Constants.HOUSE_REDUCE_STREAM,
        new Fields(Constants.TIME_FIELD, Constants.HOUSE_PLUG_FIELD));
  }

  private class PlugsMessages {
    List<Long> times = new ArrayList<>();
    List<Long> endTimes = new ArrayList<>();
    List<PlugMsg> plugReadings = new ArrayList<>();

    void add(Long time, Long endTime, PlugMsg msg) {
      times.add(time);
      endTimes.add(endTime);
      plugReadings.add(msg);
    }

    void removeFirst() {
      times.remove(0);
      Long time = endTimes.remove(0);
      if (debug && count % pi == 0) {
        LOG.info("removing time: " + time);
      }
      plugReadings.remove(0);
    }
  }

  private void processTaskPlugMessages() {
    MessageState state = checkState();
    while (state == MessageState.NUMBER_MISMATCH) {
      removeOld();
      state = checkState();
    }

    if (state == MessageState.GOOD) {
      if (debug && count % pi == 0) {
        LOG.info("Good state - emitting");
      }
      PlugMsg plugMsg = reduceMessages();
      // remove the first element of the queues
      removeFirst();
      byte[] b = DebsUtils.serialize(kryo, plugMsg);

      List<Object> emit = new ArrayList<>();
      emit.add(System.nanoTime());
      emit.add(b);

      outputCollector.emit(Constants.PLUG_REDUCE_STREAM, emit);
    }
  }

  private void removeOld() {
    List<Long> values = new ArrayList<>();
    for (Map.Entry<String, PlugsMessages> e : plugMsgs.entrySet()) {
        PlugsMessages tpm = e.getValue();
        values.add(tpm.endTimes.get(0));
    }
    String s = "";
    for (Long i : values) {
      s += i + " ";
    }
//    LOG.info("Removing: " + s);
    removeTimeCount++;

    Collections.sort(values);
    long largest = values.get(values.size() - 1);
    for (Map.Entry<String, PlugsMessages> e : plugMsgs.entrySet()) {
      PlugsMessages tpm = e.getValue();
      if (tpm.endTimes.get(0) != largest) {
        tpm.removeFirst();
        removeTotalCount++;
      }
    }
    if (debug) {
      LOG.info("Remove time count: " + removeTimeCount + " removeTotal: " + removeTotalCount + " values: " + s);
    }
  }

  private MessageState checkState() {
    long endTime = -1;
    MessageState state = MessageState.GOOD;
    for (Map.Entry<String, PlugsMessages> e : plugMsgs.entrySet()) {
      PlugsMessages tpm = e.getValue();
      String s = "";
      for (PlugMsg i : e.getValue().plugReadings) {
        s += i.line + " ";
      }
      if (debug && count % pi == 0) {
        LOG.info("PlugId: " + e.getKey() + " Endtime: " +
            (tpm.endTimes.size() > 0 ? tpm.endTimes.get(0) : 0) + " size=" +
            tpm.endTimes.size() + " id=" + e.getKey() + " list: " + s);
      }

      if (tpm.endTimes.size() < 2) {
        state = MessageState.WAITING;
      }

      if (state == MessageState.WAITING) {
        continue;
      }

//      if (endTime < 0) {
//        endTime = tpm.endTimes.get(0);
//      } else {
//        if (endTime != tpm.endTimes.get(0)) {
//          state = MessageState.NUMBER_MISMATCH;
//        }
//      }
    }
    return state;
  }

  private void removeFirst() {
    for (Map.Entry<String, PlugsMessages> e : plugMsgs.entrySet()) {
      e.getValue().removeFirst();
    }
  }

  private PlugMsg reduceMessages() {
    PlugMsg aggrPlugMsg = new PlugMsg();

    for (Map.Entry<String, PlugsMessages> e : plugMsgs.entrySet()) {
      PlugsMessages tpm = e.getValue();
      PlugMsg plugMsg = tpm.plugReadings.get(0);

      aggrPlugMsg.aggregatedPlugs.addAll(plugMsg.aggregatedPlugs);
      aggrPlugMsg.averageDaily += plugMsg.averageDaily;
      aggrPlugMsg.averageHourly += plugMsg.averageHourly;
      aggrPlugMsg.dailyEndTs = plugMsg.dailyEndTs;
      aggrPlugMsg.dailyStartTs = plugMsg.dailyStartTs;
      aggrPlugMsg.hourlyEndTs = plugMsg.hourlyEndTs;
      aggrPlugMsg.hourlyStartTs = plugMsg.hourlyStartTs;
    }
    aggrPlugMsg.id = thisTaskId;

    return aggrPlugMsg;
  }
}
