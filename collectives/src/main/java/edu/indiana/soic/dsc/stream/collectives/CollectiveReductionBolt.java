package edu.indiana.soic.dsc.stream.collectives;

import com.esotericsoftware.kryo.Kryo;
import com.twitter.heron.api.bolt.BaseRichBolt;
import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Tuple;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class CollectiveReductionBolt extends BaseRichBolt {
  private static Logger LOG = Logger.getLogger(CollectiveReductionBolt.class.getName());
  private OutputCollector outputCollector;
  private int count = 0;
  private boolean debug = false;
  private int printInveral = 0;
  private TopologyContext context;
  private Map<Integer, Queue<Tuple>> incoming = new HashMap<>();
  private Map<Integer, Integer> counts = new HashMap<>();
  private Kryo kryo;

  @Override
  public void prepare(Map stormConf, TopologyContext topologyContext, OutputCollector outputCollector) {
    List<Integer> taskIds = topologyContext.getComponentTasks(Constants.ThroughputTopology.THROUGHPUT_PASS_THROUGH);
    this.debug = (boolean) stormConf.get(Constants.ARGS_DEBUG);
    this.printInveral = (int) stormConf.get(Constants.ARGS_PRINT_INTERVAL);

    this.outputCollector = outputCollector;
    this.context = topologyContext;
    this.kryo = new Kryo();

    for (int t : taskIds) {
      incoming.put(t, new LinkedList<Tuple>());
    }
  }

  @Override
  public void execute(Tuple tuple) {
    int sourceTask = tuple.getSourceTask();
    Queue<Tuple> queue = incoming.get(sourceTask);
    queue.add(tuple);

    boolean allIn = true;
    for (Map.Entry<Integer, Queue<Tuple>> e : incoming.entrySet()) {
      if (e.getValue().size() <= 0) {
        allIn = false;
        // LOG.log(Level.INFO, String.format("%d Size %d %d", context.getThisTaskId(), e.getKey(), e.getValue().size()));
      }
    }

    if (allIn) {
      List<Long> times = new ArrayList<>();
      List<Tuple> anchors = new ArrayList<>();
      int index = -1;
      for (Map.Entry<Integer, Queue<Tuple>> e : incoming.entrySet()) {
        Tuple t = e.getValue().poll();
        anchors.add(t);
        times.add(t.getLongByField(Constants.Fields.TIME_FIELD));
        int currentIndex = t.getIntegerByField(Constants.Fields.MESSAGE_INDEX_FIELD);
        if (index != -1 && currentIndex != index) {
          LOG.severe("Index not equal ***********************  ");
        }
        index = currentIndex;
      }

      byte []b;
      List<Object> list = new ArrayList<>();
      Collections.sort(times);
      Long time = times.get(0);
      SingleTrace singleTrace = new SingleTrace();
      b = Utils.serialize(kryo, singleTrace);
      list.add(b);
      list.add("");
      list.add(time);

      for (Tuple t : anchors) {
        outputCollector.ack(t);
      }

      outputCollector.emit(Constants.Fields.CHAIN_STREAM, list);
    }

    for (Map.Entry<Integer, Queue<Tuple>> e : incoming.entrySet()) {
      LOG.log(Level.INFO, String.format("%d Incoming %d, %d total: %d",
          context.getThisTaskId(), e.getKey(), e.getValue().size(), counts.get(e.getKey())));
    }

    if (debug && count % printInveral == 0) {
      LOG.info(context.getThisTaskId() + " Last Received tuple: " + count);
    }
    count++;
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declareStream(Constants.Fields.CHAIN_STREAM, new Fields(
        Constants.Fields.BODY,
        Constants.Fields.SENSOR_ID_FIELD,
        Constants.Fields.TIME_FIELD));
  }
}
