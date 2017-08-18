package edu.indiana.soic.dsc.stream.debs.bolt;

import com.esotericsoftware.kryo.Kryo;
import com.twitter.heron.api.bolt.IOutputCollector;
import com.twitter.heron.api.grouping.IReduce;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Tuple;
import edu.indiana.soic.dsc.stream.debs.Constants;
import edu.indiana.soic.dsc.stream.debs.DebsUtils;
import edu.indiana.soic.dsc.stream.debs.msg.PlugMsg;

import java.util.*;
import java.util.logging.Logger;

public class ReductionFunction implements IReduce {
  private static Logger LOG = Logger.getLogger(ReductionFunction.class.getName());

  private int thisTaskId;

  private Kryo kryo;

  // tasId, <plugId, PlugMsg>
  private Map<Integer, TaskPlugMessages> plugMessages;
  private IOutputCollector outputCollector;
  private boolean debug;
  private int pi;
  private int count = 0;

  @Override
  public void prepare(Map<String, Object> map, TopologyContext topologyContext,
                      List<Integer> list, IOutputCollector iOutputCollector) {
    thisTaskId = topologyContext.getThisTaskId();
    plugMessages = new HashMap<>();
    for (int i : list) {
      plugMessages.put(i, new TaskPlugMessages());
    }
    this.outputCollector = iOutputCollector;
    this.kryo = new Kryo();
    DebsUtils.registerClasses(kryo);
    this.debug = (boolean) map.get(Constants.ARGS_DEBUG);
    this.pi = (int) map.get(Constants.ARGS_PRINT_INTERVAL);
  }

  @Override
  public void execute(int i, Tuple tuple) {
    Object object = tuple.getValueByField(Constants.PLUG_FIELD);
    Object time = tuple.getValueByField(Constants.TIME_FIELD);

    if (debug && count % pi == 0) {
      LOG.info("Got message from " + i);
    }

    PlugMsg plugMsg = (PlugMsg) DebsUtils.deSerialize(kryo, (byte [])object, PlugMsg.class);
    TaskPlugMessages taskPlugMessages = plugMessages.get(i);

    taskPlugMessages.endTimes.add(plugMsg.dailyEndTs);
    taskPlugMessages.plugMsgs.add(plugMsg);
    taskPlugMessages.times.add((Long) time);
    processTaskPlugMessages();
    count++;
  }

  private void processTaskPlugMessages() {
    MessageState state = checkState();
    while (state == MessageState.NUMBER_MISMATCH) {
      if (debug && count % pi == 0) {
        LOG.info("Mismatch state");
      }
      removeOld();
      state = checkState();
    }

    if (state == MessageState.GOOD) {
      if (debug && count % pi == 0) {
        LOG.info("Good state - emitting");
      }
      PlugMsg plugMsg = reduceMessages();
      removeFirst();
      byte[] b = DebsUtils.serialize(kryo, plugMsg);

      List<Object> emit = new ArrayList<>();
      emit.add(System.nanoTime());
      emit.add(b);

      outputCollector.emit(Constants.PLUG_REDUCE_STREAM, null, emit);
    }
  }

  private void removeOld() {
    List<Long> values = new ArrayList<>();
    for (Map.Entry<Integer, TaskPlugMessages> e : plugMessages.entrySet()) {
      TaskPlugMessages tpm = e.getValue();
      values.add(tpm.endTimes.get(0));
    }

    Collections.sort(values);
    long largest = values.get(values.size() - 1);
    for (Map.Entry<Integer, TaskPlugMessages> e : plugMessages.entrySet()) {
      TaskPlugMessages tpm = e.getValue();

      if (tpm.endTimes.get(0) != largest) {
        tpm.removeFirst();
      }
    }
  }

  private MessageState checkState() {
    long endTime = -1;
    MessageState state = MessageState.GOOD;
    for (Map.Entry<Integer, TaskPlugMessages> e : plugMessages.entrySet()) {
      TaskPlugMessages tpm = e.getValue();

      LOG.info("Endtime: " + (tpm.endTimes.size() > 0 ? tpm.endTimes.get(0) : 0) + " size=" + tpm.endTimes.size() + " id=" + e.getKey());
      if (tpm.endTimes.size() < 2) {
        return MessageState.WAITING;
      }

      if (endTime < 0) {
        endTime = tpm.endTimes.get(0);
      } else {
        if (endTime != tpm.endTimes.get(0)) {
          LOG.warning("End times are not equal");
          state = MessageState.NUMBER_MISMATCH;
        }
      }
    }
    return state;
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

  @Override
  public void cleanup() {

  }

  private class TaskPlugMessages {
    List<Long> times = new ArrayList<>();
    List<Long> endTimes = new ArrayList<>();
    List<PlugMsg> plugMsgs = new ArrayList<>();

    void removeFirst() {
      times.remove(0);
      endTimes.remove(0);
      plugMsgs.remove(0);
    }
  }
}
