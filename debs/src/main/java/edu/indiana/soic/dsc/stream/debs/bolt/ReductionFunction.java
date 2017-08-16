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
  private Logger LOG = Logger.getLogger(ReductionFunction.class.getName());

  private int thisTaskId;

  private Kryo kryo;

  private class PlugKey {
    boolean aggregate;
    int id;

    public PlugKey(boolean aggregate, int id) {
      this.aggregate = aggregate;
      this.id = id;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      PlugKey plugKey = (PlugKey) o;

      if (aggregate != plugKey.aggregate) return false;
      return id == plugKey.id;

    }

    @Override
    public int hashCode() {
      int result = (aggregate ? 1 : 0);
      result = 31 * result + id;
      return result;
    }
  }

  // tasId, <plugId, PlugMsg>
  private Map<Integer, Map<PlugKey, TaskPlugMessages>> plugMessages = new HashMap<>();
  private IOutputCollector outputCollector;

  @Override
  public void prepare(Map<String, Object> map, TopologyContext topologyContext,
                      List<Integer> list, IOutputCollector iOutputCollector) {
    thisTaskId = topologyContext.getThisTaskId();
    for (int i : list) {
      plugMessages.put(i, new HashMap<PlugKey, TaskPlugMessages>());
    }
    this.outputCollector = iOutputCollector;
    this.kryo = new Kryo();
    DebsUtils.registerClasses(kryo);
  }

  @Override
  public void execute(int i, Tuple tuple) {
    Object object = tuple.getValueByField(Constants.PLUG_FIELD);
    Object time = tuple.getValueByField(Constants.TIME_FIELD);

    PlugMsg plugMsg = (PlugMsg) object;
    Map<PlugKey, TaskPlugMessages> plugMessagesForTask = plugMessages.get(i);

    PlugKey key = new PlugKey(plugMsg.aggregate, plugMsg.id);
    TaskPlugMessages taskPlugMessages = plugMessagesForTask.get(key);
    if (taskPlugMessages == null) {
      taskPlugMessages = new TaskPlugMessages();
      plugMessagesForTask.put(key, taskPlugMessages);
    }

    taskPlugMessages.endTimes.add(plugMsg.dailyEndTs);
    taskPlugMessages.plugMsgs.add(plugMsg);
    taskPlugMessages.times.add((Long) time);

    processTaskPlugMessages();
  }

  private enum State {
    GOOD,
    NUMBER_MISMATCH,
    WAITING,
  }

  private void processTaskPlugMessages() {
    State state = checkState();
    while (state == State.NUMBER_MISMATCH) {
      removeOld();
      state = checkState();
    }

    if (state == State.GOOD) {
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
    for (Map.Entry<Integer, Map<PlugKey, TaskPlugMessages>> e : plugMessages.entrySet()) {
      for (Map.Entry<PlugKey, TaskPlugMessages> te : e.getValue().entrySet()) {
        TaskPlugMessages tpm = te.getValue();

        values.add(tpm.endTimes.get(0));
      }
    }

    Collections.sort(values);
    long largest = values.get(values.size() - 1);
    for (Map.Entry<Integer, Map<PlugKey, TaskPlugMessages>> e : plugMessages.entrySet()) {
      for (Map.Entry<PlugKey, TaskPlugMessages> te : e.getValue().entrySet()) {
        TaskPlugMessages tpm = te.getValue();

        if (tpm.endTimes.get(0) != largest) {
          tpm.removeFirst();
        }
      }
    }
  }

  private State checkState() {
    for (Map.Entry<Integer, Map<PlugKey, TaskPlugMessages>> e : plugMessages.entrySet()) {
      long endTime = -1;

      if (e.getValue().size() <= 0) {
        return State.WAITING;
      }

      for (Map.Entry<PlugKey, TaskPlugMessages> te : e.getValue().entrySet()) {
        TaskPlugMessages tpm = te.getValue();

        if (tpm.endTimes.size() < 2) {
          return State.WAITING;
        }

        if (endTime < 0) {
          endTime = tpm.endTimes.get(0);
        } else {
          if (endTime != tpm.endTimes.get(0)) {
            LOG.warning("End times are not equal");
            return State.NUMBER_MISMATCH;
          }
        }
      }
    }
    return State.GOOD;
  }

  private PlugMsg reduceMessages() {
    PlugMsg aggrPlugMsg = new PlugMsg();

    for (Map.Entry<Integer, Map<PlugKey, TaskPlugMessages>> e : plugMessages.entrySet()) {
      for (Map.Entry<PlugKey, TaskPlugMessages> te : e.getValue().entrySet()) {
        TaskPlugMessages tpm = te.getValue();
        PlugMsg plugMsg = tpm.plugMsgs.get(0);

        aggrPlugMsg.aggregatedPlugs.addAll(plugMsg.aggregatedPlugs);
        aggrPlugMsg.averageDaily += plugMsg.averageDaily;
        aggrPlugMsg.averageHourly += plugMsg.averageHourly;
        aggrPlugMsg.dailyEndTs = plugMsg.dailyEndTs;
        aggrPlugMsg.dailyStartTs = plugMsg.dailyStartTs;
        aggrPlugMsg.hourlyEndTs = plugMsg.hourlyEndTs;
        aggrPlugMsg.hourlyStartTs = plugMsg.hourlyStartTs;
      }
    }
    aggrPlugMsg.aggregate = true;
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
