package edu.indiana.soic.dsc.stream.perf;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.esotericsoftware.kryo.Kryo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class GatherBolt extends BaseRichBolt {
  private Logger LOG = LoggerFactory.getLogger(GatherBolt.class);
  private int workers = 0;
  private TopologyContext topologyContext;
  private OutputCollector collector;

  private Kryo kryo;
  private Map<String, List<Trace>> inputs = new HashMap<String, List<Trace>>();
  private boolean synchronous = true;

  private class Trace {
    String id;
    BTrace trace;
    long receiveTime;

    private Trace(String id, BTrace trace, long receiveTime) {
      this.id = id;
      this.trace = trace;
      this.receiveTime = receiveTime;
    }
  }

  public void setSynchronous(boolean synchronous) {
    this.synchronous = synchronous;
  }

  @Override
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    this.collector = collector;
    this.topologyContext = context;
    this.workers = context.getComponentTasks(Constants.Topology.WORKER_BOLT).size();
    this.kryo = new Kryo();
    Utils.registerClasses(this.kryo);
  }

  @Override
  public void execute(Tuple tuple) {
    String stream = tuple.getSourceStreamId();
    if (stream.equals(Constants.Fields.CONTROL_STREAM)) {
      return;
    }
    String messageId = tuple.getStringByField(Constants.Fields.MESSAGE_ID_FIELD);
    LOG.info("Message ID: " + messageId);

    // wait until we get all the inputs
    processInput(tuple, messageId);

    List<Trace> tuples = inputs.get(messageId);
    if (tuples.size() == workers) {
      //LOG.info("Gather done for message with ID: " + messageId);
      // emit the current trace

      SingleTrace singleTrace = createSingleTrace(tuples);
      byte []b = Utils.serialize(kryo, singleTrace);

      List<Object> list = new ArrayList<Object>();
      list.add(b);
      list.add(messageId);
      list.add(singleTrace.getTime());
      topologyContext.getThisTaskIndex();
      collector.emit(Constants.Fields.SEND_STREAM, list);

      if (synchronous) {
        list = new ArrayList<Object>();
        list.add("ready");
        collector.emit(Constants.Fields.READY_STREAM, list);
      }
    } else if (tuples.size() > workers) {
      LOG.error("received more than expected: " + tuples.size());
    }

    if (inputs.size() > 100) {
      for (Iterator<Map.Entry<String, List<Trace>>> it = inputs.entrySet().iterator(); it.hasNext();) {
        Map.Entry<String, List<Trace>> e = it.next();
        if (e.getValue().size() > workers) {
          LOG.error("Received more than expected: "  + e.getValue().size());
          it.remove();
        }
        if (e.getValue().size() == workers) {
          it.remove();
        }
      }
      // LOG.info("Pending messages ids for removal: " + inputs.size());
    }
  }

  private SingleTrace createSingleTrace(List<Trace> traces) {
    SingleTrace singleTrace = new SingleTrace();
    long[] times = new long[workers];
    for (Trace t : traces) {
      times[t.trace.getTaskId()] = t.receiveTime;
      singleTrace.setTime(t.trace.getTime());
    }
    singleTrace.setReceiveTimes(times);
    return singleTrace;
  }

  private void processInput(Tuple tuple, String messageId) {
    List<Trace> tuples = inputs.get(messageId);
    if (tuples == null) {
      tuples = new ArrayList<Trace>();
      inputs.put(messageId, tuples);
    }

    byte []traceBytes = (byte[]) tuple.getValueByField(Constants.Fields.TRACE_FIELD);
    BTrace trace = (BTrace) Utils.deSerialize(kryo, traceBytes, BTrace.class);

    for (int i = 0; i < tuples.size(); i++) {
      BTrace b = tuples.get(i).trace;
      if (b.getTaskId() == trace.getTaskId()) {
        LOG.error("Existing tuple received {} : {}", b.getTaskId(), trace.getTaskId());
      }
    }

    tuples.add(new Trace(messageId, trace, System.nanoTime()));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declareStream(Constants.Fields.SEND_STREAM,
        new Fields("body", Constants.Fields.SENSOR_ID_FIELD, Constants.Fields.TIME_FIELD));
    outputFieldsDeclarer.declareStream(Constants.Fields.READY_STREAM,
        new Fields(Constants.Fields.DATA_FIELD));
  }
}

