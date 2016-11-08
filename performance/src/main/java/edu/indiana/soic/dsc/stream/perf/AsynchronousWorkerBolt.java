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

public class AsynchronousWorkerBolt extends BaseRichBolt {
  private static Logger LOG = LoggerFactory.getLogger(WorkerBolt.class);

  private TopologyContext context;
  private OutputCollector collector;

  private Kryo kryo;

  @Override
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    this.collector = collector;
    this.context = context;
    this.kryo = new Kryo();
    Utils.registerClasses(kryo);
    LOG.info("Initializing task with ID {}", context.getThisTaskIndex());
  }

  @Override
  public void execute(Tuple tuple) {
    String stream = tuple.getSourceStreamId();
    if (stream.equals(Constants.Fields.CONTROL_STREAM)) {
      return;
    }
    int taskId = context.getThisTaskIndex();
    LOG.info("Message received for worker with ID {}", taskId);

    Object body = tuple.getValueByField(Constants.Fields.BODY);

    byte []traceBytes = (byte[]) tuple.getValueByField(Constants.Fields.TRACE_FIELD);
    BTrace trace = (BTrace) Utils.deSerialize(kryo, traceBytes, BTrace.class);

    String messageId = tuple.getStringByField(Constants.Fields.MESSAGE_ID_FIELD);

    trace.setTaskId(taskId);
    //trace.setBcastReceiveTime(receiveTime);
    byte []traceSendBytes = Utils.serialize(kryo, trace);
    List<Object> list = new ArrayList<Object>();
    list.add(traceSendBytes);
    list.add(messageId);
    list.add(trace.getTime());
    collector.emit(Constants.Fields.GATHER_STREAM, list);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declareStream(Constants.Fields.SEND_STREAM,
        new Fields("body", Constants.Fields.SENSOR_ID_FIELD, Constants.Fields.TIME_FIELD));
  }
}
