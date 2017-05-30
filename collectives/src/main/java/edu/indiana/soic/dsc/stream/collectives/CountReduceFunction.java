package edu.indiana.soic.dsc.stream.collectives;

import com.twitter.heron.api.bolt.IOutputCollector;
import com.twitter.heron.api.grouping.IReduce;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Tuple;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class CountReduceFunction implements IReduce {
  private static Logger LOG = Logger.getLogger(CountReduceFunction.class.getName());

  private IOutputCollector collector;

  private TopologyContext context;

  private int count = 0;

  private Map<Integer, Queue<Tuple>> incoming = new HashMap<>();

  private Map<Integer, Integer> counts = new HashMap<>();

  private boolean debug = false;
  private int printInveral = 0;

  @Override
  public void prepare(Map<String, Object> map, TopologyContext topologyContext, List<Integer> list,
                      IOutputCollector iOutputCollector) {
    this.collector = iOutputCollector;
    this.context = topologyContext;
    this.debug = (boolean) map.get(Constants.ARGS_DEBUG);
    this.printInveral = (int) map.get(Constants.ARGS_PRINT_INTERVAL);
    for (int i : list) {
      incoming.put(i, new LinkedList<Tuple>());
      counts.put(i, 0);
    }
    LOG.info("Reduce function");
  }

  @Override
  public void execute(int i, Tuple tuple) {
    if (debug && count % printInveral == 0) {
      LOG.info(String.format("%d Reduction Received message from %d %d", context.getThisTaskId(), i, count));
    }
    count++;
    List<Tuple> anchors = new ArrayList<>();

    Queue<Tuple> in = incoming.get(i);
    in.add(tuple);
    long tupleTime = tuple.getLongByField(Constants.Fields.TIME_FIELD);

    int c = counts.get(i);
    counts.put(i, c + 1);

    boolean allIn = true;
    for (Map.Entry<Integer, Queue<Tuple>> e : incoming.entrySet()) {
      if (e.getValue().size() <= 0) {
        allIn = false;
      }
    }

    if (allIn) {
      List<Long> times = new ArrayList<>();
      Object body = null;
      Integer size = 0;
      Integer index = -1;

      for (Map.Entry<Integer, Queue<Tuple>> e : incoming.entrySet()) {
        Tuple t = e.getValue().poll();
        anchors.add(t);
        times.add(t.getLongByField(Constants.Fields.TIME_FIELD));

        body = t.getValueByField(Constants.Fields.BODY);
        size = t.getIntegerByField(Constants.Fields.MESSAGE_SIZE_FIELD);
        int currentIndex = t.getIntegerByField(Constants.Fields.MESSAGE_INDEX_FIELD);

        if (index != -1 && currentIndex != index) {
          LOG.severe("Index not equal ***********************  ");
        }
        index = currentIndex;
      }

      Collections.sort(times);
      Long time = times.get(0);

      List<Object> list = new ArrayList<>();
      list.add(body);
      list.add(index);
      list.add(size);
      list.add(time);
      list.add(System.nanoTime());
      collector.emit(Constants.Fields.CHAIN_STREAM, anchors, list);
      for (Tuple t : anchors) {
        collector.ack(t);
      }
    }

    if (debug && count % printInveral == 0) {
      for (Map.Entry<Integer, Queue<Tuple>> e : incoming.entrySet()) {
        long elapsed = (System.nanoTime() - tupleTime) / 1000000;
        LOG.log(Level.INFO, String.format("%d Incoming %d, %d total: %d %d",
            context.getThisTaskId(), e.getKey(), e.getValue().size(), counts.get(e.getKey()), elapsed));
      }
    }

  }

  @Override
  public void cleanup() {

  }
}
