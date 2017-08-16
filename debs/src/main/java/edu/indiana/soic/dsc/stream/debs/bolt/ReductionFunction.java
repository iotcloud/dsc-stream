package edu.indiana.soic.dsc.stream.debs.bolt;

import com.twitter.heron.api.bolt.IOutputCollector;
import com.twitter.heron.api.grouping.IReduce;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Tuple;
import edu.indiana.soic.dsc.stream.debs.Constants;
import edu.indiana.soic.dsc.stream.debs.msg.PlugMsg;

import java.util.*;
import java.util.logging.Logger;

public class ReductionFunction implements IReduce {
  private Logger LOG = Logger.getLogger(ReductionFunction.class.getName());

  private int thisTaskId;

  // tasId, <plugId, PlugMsg>
  private Map<Integer, Map<Integer, TaskPlugMessages>> plugMessages = new HashMap<>();

  private Long currentEndTIme;

  @Override
  public void prepare(Map<String, Object> map, TopologyContext topologyContext,
                      List<Integer> list, IOutputCollector iOutputCollector) {
    thisTaskId = topologyContext.getThisTaskId();
    for (int i : list) {
      plugMessages.put(i, new HashMap<Integer, TaskPlugMessages>());
    }
  }

  @Override
  public void execute(int i, Tuple tuple) {
    Object object = tuple.getValueByField(Constants.PLUG_FIELD);
    Object time = tuple.getValueByField(Constants.TIME_FIELD);

    PlugMsg plugMsg = (PlugMsg) object;
    Map<Integer, TaskPlugMessages> plugMessagesForTask = plugMessages.get(i);

    TaskPlugMessages taskPlugMessages = plugMessagesForTask.get(plugMsg.id);
    if (taskPlugMessages == null) {
      taskPlugMessages = new TaskPlugMessages();
      plugMessagesForTask.put(plugMsg.id, taskPlugMessages);
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
      reduceMessages();
    }
  }

  private void removeOld() {
    List<Long> values = new ArrayList<>();
    for (Map.Entry<Integer, Map<Integer, TaskPlugMessages>> e : plugMessages.entrySet()) {
      for (Map.Entry<Integer, TaskPlugMessages> te : e.getValue().entrySet()) {
        TaskPlugMessages tpm = te.getValue();

        values.add(tpm.endTimes.get(0));
      }
    }

    Collections.sort(values);
    long largest = values.get(values.size() - 1);
    for (Map.Entry<Integer, Map<Integer, TaskPlugMessages>> e : plugMessages.entrySet()) {
      for (Map.Entry<Integer, TaskPlugMessages> te : e.getValue().entrySet()) {
        TaskPlugMessages tpm = te.getValue();

        if (tpm.endTimes.get(0) != largest) {
          tpm.removeFirst();
        }
      }
    }
  }

  private State checkState() {
    for (Map.Entry<Integer, Map<Integer, TaskPlugMessages>> e : plugMessages.entrySet()) {
      long endTime = -1;

      if (e.getValue().size() <= 0) {
        return State.WAITING;
      }

      for (Map.Entry<Integer, TaskPlugMessages> te : e.getValue().entrySet()) {
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

    for (Map.Entry<Integer, Map<Integer, TaskPlugMessages>> e : plugMessages.entrySet()) {
      for (Map.Entry<Integer, TaskPlugMessages> te : e.getValue().entrySet()) {
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
