package edu.indiana.soic.dsc.stream.collectives;

import com.twitter.heron.api.bolt.IOutputCollector;
import com.twitter.heron.api.grouping.IReduce;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Tuple;
import edu.indiana.soic.dsc.stream.perf.Constants;

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

  @Override
  public void prepare(Map<String, Object> map, TopologyContext topologyContext, List<Integer> list,
                      IOutputCollector iOutputCollector) {
    this.collector = iOutputCollector;
    this.context = topologyContext;
    this.debug = (boolean) map.get(Constants.ARGS_DEBUG);
    for (int i : list) {
      incoming.put(i, new LinkedList<Tuple>());
      counts.put(i, 0);
    }
    LOG.info("Reduce function");
  }

  @Override
  public void execute(int i, Tuple tuple) {
    if (debug) {
      LOG.info(String.format("%d Reduction Received message from %d %d", context.getThisTaskId(), i, count));
    }
    count++;
    List<Tuple> anchors = new ArrayList<>();

    Queue<Tuple> in = incoming.get(i);
    in.add(tuple);

    int c = counts.get(i);
    counts.put(i, c + 1);

    boolean allIn = true;
    for (Map.Entry<Integer, Queue<Tuple>> e : incoming.entrySet()) {
      if (e.getValue().size() <= 0) {
        allIn = false;
      }
    }

    if (allIn) {
      for (Map.Entry<Integer, Queue<Tuple>> e : incoming.entrySet()) {
        anchors.add(e.getValue().poll());
      }

      Object body = tuple.getValueByField(Constants.Fields.BODY);
      Integer size = tuple.getIntegerByField(Constants.Fields.MESSAGE_SIZE_FIELD);
      Object index = tuple.getValueByField(Constants.Fields.MESSAGE_INDEX_FIELD);
      Long time = tuple.getLongByField(Constants.Fields.TIME_FIELD);

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

    if (debug) {
      for (Map.Entry<Integer, Queue<Tuple>> e : incoming.entrySet()) {

        LOG.log(Level.INFO, String.format("%d Incoming %d, %d total: %d",
            context.getThisTaskId(), e.getKey(), e.getValue().size(), counts.get(e.getKey())));
      }
    }

  }

  @Override
  public void cleanup() {

  }
}
