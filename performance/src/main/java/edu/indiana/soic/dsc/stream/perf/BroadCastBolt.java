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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class BroadCastBolt extends BaseRichBolt {
  private Logger LOG = LoggerFactory.getLogger(BroadCastBolt.class);

  private TopologyContext context;
  private OutputCollector collector;
  private Kryo kryo;
  private boolean synchronous = true;
  private State state = State.WAITING_FOR_READING;

  private enum State {
    WAITING_FOR_READING,
    WAITING_FOR_READY,
  }

  public void setSynchronous(boolean synchronous) {
    this.synchronous = synchronous;
  }

  @Override
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    this.context = context;
    this.collector = collector;
    this.kryo = new Kryo();
    Utils.registerClasses(kryo);
  }

  @Override
  public void execute(Tuple tuple) {
    String stream = tuple.getSourceStreamId();

    if (synchronous) {
      if (stream.equals(Constants.Fields.READY_STREAM)) {
        // LOG.info("Received READY, waiting for message");
        this.state = State.WAITING_FOR_READING;
        return;
      } else if (state == State.WAITING_FOR_READING) {
        // LOG.info("Received message, emit and waiting for READY");
        Object body = tuple.getValueByField(Constants.Fields.BODY);
        Object time = tuple.getValueByField(Constants.Fields.TIME_FIELD);
        BTrace trace = new BTrace();
        trace.setTime(Long.parseLong(time.toString()));
        trace.setTaskId(context.getThisTaskIndex());
        String messageId = tuple.getStringByField(Constants.Fields.SENSOR_ID_FIELD);

        byte[] b = Utils.serialize(kryo, trace);
        List<Object> list = new ArrayList<Object>();
        list.add(body);
        list.add(b);
        list.add(messageId);
        collector.emit(Constants.Fields.BROADCAST_STREAM, list);
        // we are waiting for the tuples to finish
        state = State.WAITING_FOR_READY;
      } else {
        // LOG.info("Received message, dropping message until READY for next");
      }
    } else {
      Object body = tuple.getValueByField(Constants.Fields.BODY);
      Object time = tuple.getValueByField(Constants.Fields.TIME_FIELD);
      Object sensorId = tuple.getValueByField(Constants.Fields.SENSOR_ID_FIELD);

      BTrace trace = new BTrace();
      trace.setTime(Long.parseLong(time.toString()));
      trace.setTaskId(context.getThisTaskIndex());

      byte[] b = Utils.serialize(kryo, trace);
      List<Object> list = new ArrayList<Object>();
      list.add(body);
      list.add(b);
      list.add(sensorId);
      collector.emit(Constants.Fields.BROADCAST_STREAM, list);
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declareStream(Constants.Fields.BROADCAST_STREAM, new Fields(
        Constants.Fields.BODY,
        Constants.Fields.TRACE_FIELD,
        Constants.Fields.MESSAGE_ID_FIELD));
  }
}
