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

  private Map<Integer, House> plugToHouse;

  private OutputCollector outputCollector;

  private Kryo kryo;
  
  private int thisTaskId;

  // plugid, plugmessage
  private Map<Integer, PlugsMessages> plugMsgs;

  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    plugToHouse = new HashMap<>();
    plugMsgs = new HashMap<>();
    this.outputCollector = outputCollector;
    kryo = new Kryo();
    DebsUtils.registerClasses(kryo);
    thisTaskId = topologyContext.getThisTaskId();
  }

  @Override
  public void execute(Tuple tuple) {
    Object input = tuple.getValueByField(Constants.DATA_FIELD);
    Object time = tuple.getValueByField(Constants.TIME_FIELD);

    DataReading reading = (DataReading) DebsUtils.deSerialize(kryo, (byte [])input, DataReading.class);

    House house;
    if (plugToHouse.containsKey(reading.houseId)) {
      house = plugToHouse.get(reading.houseId);
    } else {
      house = new House();
      plugToHouse.put(reading.houseId, house);
    }
    house.addReading(reading);

    Plug plug = house.getPlug(reading.householdId, reading.plugId);
    PlugMsg plugMsg = createPlugMsg(reading, plug);

    PlugsMessages plugsMessages = plugMsgs.get(plug.id);
    if (plugsMessages == null) {
      plugsMessages = new PlugsMessages();
      plugMsgs.put(plug.id, plugsMessages);
    }
    plugsMessages.add((Long) time, plugMsg.dailyEndTs, plugMsg);

    processTaskPlugMessages();
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

    return new PlugMsg(plug.id, averageHourly, averageDaily, hourlyStart, hourlyEnd, dailyStart, dailyEnd, aggrs);
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
      endTimes.remove(0);
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
      PlugMsg plugMsg = reduceMessages();
      byte[] b = DebsUtils.serialize(kryo, plugMsg);

      List<Object> emit = new ArrayList<>();
      emit.add(System.nanoTime());
      emit.add(b);

      outputCollector.emit(Constants.PLUG_REDUCE_STREAM, new ArrayList<Tuple>(), emit);
    }
  }

  private void removeOld() {
    List<Long> values = new ArrayList<>();
    for (Map.Entry<Integer, PlugsMessages> e : plugMsgs.entrySet()) {
        PlugsMessages tpm = e.getValue();
        values.add(tpm.endTimes.get(0));
    }

    Collections.sort(values);
    long largest = values.get(values.size() - 1);
    for (Map.Entry<Integer, PlugsMessages> e : plugMsgs.entrySet()) {
      PlugsMessages tpm = e.getValue();
      if (tpm.endTimes.get(0) != largest) {
        tpm.removeFirst();
      }
    }
  }

  private MessageState checkState() {
    long endTime = -1;
    for (Map.Entry<Integer, PlugsMessages> e : plugMsgs.entrySet()) {
      PlugsMessages tpm = e.getValue();

      if (tpm.endTimes.size() < 2) {
        return MessageState.WAITING;
      }

      if (endTime < 0) {
        endTime = tpm.endTimes.get(0);
      } else {
        if (endTime != tpm.endTimes.get(0)) {
          LOG.warning("End times are not equal");
          return MessageState.NUMBER_MISMATCH;
        }
      }
    }
    return MessageState.GOOD;
  }

  private PlugMsg reduceMessages() {
    PlugMsg aggrPlugMsg = new PlugMsg();

    for (Map.Entry<Integer, PlugsMessages> e : plugMsgs.entrySet()) {
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
