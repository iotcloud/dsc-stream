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

public class MultiDataCollectionBolt extends BaseRichBolt {
  private static Logger LOG = Logger.getLogger(MultiDataCollectionBolt.class.getName());
  private OutputCollector outputCollector;
  private int count = 0;
  private boolean debug = false;
  private int printInveral = 0;
  private TopologyContext context;
  private Map<Integer, Queue<Tuple>> incoming = new HashMap<>();
  private Map<Integer, Queue<Long>> arrivalTimes = new HashMap<>();
  private Kryo kryo;
  private boolean passThrough = false;
  private String upperComponentName;

  public boolean isPassThrough() {
    return passThrough;
  }

  public void setPassThrough(boolean passThrough) {
    this.passThrough = passThrough;
  }

  public String getUpperComponentName() {
    return upperComponentName;
  }

  public void setUpperComponentName(String upperComponentName) {
    this.upperComponentName = upperComponentName;
  }

  @Override
  public void prepare(Map stormConf, TopologyContext topologyContext, OutputCollector outputCollector) {
    String upName = (String) stormConf.get(Constants.UPPER_COMPONENT_NAME);
    if (upName != null) {
      upperComponentName = upName;
    }
    List<Integer> taskIds = topologyContext.getComponentTasks(upperComponentName);
    this.debug = (boolean) stormConf.get(Constants.ARGS_DEBUG);
    this.printInveral = (int) stormConf.get(Constants.ARGS_PRINT_INTERVAL);

    this.outputCollector = outputCollector;
    this.context = topologyContext;
    this.kryo = new Kryo();

    for (int t : taskIds) {
      incoming.put(t, new LinkedList<Tuple>());
      arrivalTimes.put(t, new LinkedList<Long>());
    }
  }

  @Override
  public void execute(Tuple tuple) {
    int sourceTask = tuple.getSourceTask();
    Queue<Tuple> queue = incoming.get(sourceTask);
    queue.add(tuple);
    Queue<Long> arrivalQueue = arrivalTimes.get(sourceTask);
    arrivalQueue.add(System.nanoTime());

    boolean allIn = true;
    for (Map.Entry<Integer, Queue<Tuple>> e : incoming.entrySet()) {
      if (e.getValue().size() <= 0) {
        allIn = false;
      }
    }

    if (allIn) {
      List<Long> timings = new ArrayList<>();
      List<Long> arrivalTimesList = new ArrayList<>();
      List<Tuple> anchors = new ArrayList<>();
      Object body = null;
      int index = -1;
      int tmpIndex;
      for (Map.Entry<Integer, Queue<Tuple>> e : incoming.entrySet()) {
        Tuple t = e.getValue().poll();
        tmpIndex = t.getIntegerByField(Constants.Fields.MESSAGE_INDEX_FIELD);
        if (index == -1) {
          index = tmpIndex;
        } else {
          if (tmpIndex != index) {
            LOG.severe(String.format("Indexes are not equal, something is wrong %d %d", index, tmpIndex));
          }
        }
        body = tuple.getValueByField(Constants.Fields.BODY);
        anchors.add(t);
        arrivalTimesList.add(arrivalTimes.get(e.getKey()).poll());
        timings.add(t.getLongByField(Constants.Fields.TIME_FIELD));
      }

      byte []b;
      List<Object> list = new ArrayList<>();
      Collections.sort(timings);
      Collections.sort(arrivalTimesList);

      Long time = timings.get(0);
      SingleTrace singleTrace = new SingleTrace();
      long[] arrivalTimesArray = new long[arrivalTimesList.size()];
      for (int i = 0; i < arrivalTimesArray.length; i++) {
        arrivalTimesArray[i] = arrivalTimesList.get(0);
      }
      if (!passThrough) {
        // singleTrace.setReceiveTimes(arrivalTimesArray);
        b = Utils.serialize(kryo, singleTrace);
        list.add(b);
      } else {
        list.add(body);
      }
      list.add(0);
      list.add(0);
      list.add(time);
      list.add(System.nanoTime());

      for (Tuple t : anchors) {
        outputCollector.ack(t);
      }

      outputCollector.emit(Constants.Fields.CHAIN_STREAM, list);
    }

    if (debug && count % printInveral == 0) {
      for (Map.Entry<Integer, Queue<Tuple>> e : incoming.entrySet()) {
        LOG.log(Level.INFO, String.format("%d Incoming %d, %d total: %d",
            context.getThisTaskId(), e.getKey(), e.getValue().size(), 0));
      }
      LOG.info(context.getThisTaskId() + " Last Received tuple: " + count);
    }
    count++;
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declareStream(Constants.Fields.CHAIN_STREAM, new Fields(
        Constants.Fields.BODY,
        Constants.Fields.MESSAGE_INDEX_FIELD,
        Constants.Fields.MESSAGE_SIZE_FIELD,
        Constants.Fields.TIME_FIELD,
        Constants.Fields.TIME_FIELD2));
  }
}
