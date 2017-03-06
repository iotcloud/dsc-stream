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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PassThroughBolt extends BaseRichBolt {
  private static Logger LOG = LoggerFactory.getLogger(PassThroughBolt.class);
  private TopologyContext context;
  private OutputCollector collector;
  private Kryo kryo;
  private boolean last;
  private boolean first;
  private boolean debug;
  private Map<Integer, byte[]> dataCache = new HashMap<>();

  public boolean isFirst() {
    return first;
  }

  public void setFirst(boolean first) {
    this.first = first;
  }

  public boolean isLast() {
    return last;
  }

  public void setLast(boolean last) {
    this.last = last;
  }

  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    this.context = topologyContext;
    this.collector = outputCollector;
    this.kryo = new Kryo();
    Utils.registerClasses(kryo);
    this.debug = (boolean) map.get(Constants.ARGS_DEBUG);
  }

  @Override
  public void execute(Tuple tuple) {
    Object body = tuple.getValueByField(Constants.Fields.BODY);
    Object time = tuple.getValueByField(Constants.Fields.TIME_FIELD);
    Object sensorId = tuple.getValueByField(Constants.Fields.SENSOR_ID_FIELD);
    byte []b = null;

    List<Object> list = new ArrayList<Object>();
    // first bolt but not last
    if (first && !last) {
      ByteBuffer wrapped = ByteBuffer.wrap((byte[]) body); // big-endian by default
      int dataSize = wrapped.getInt(); // 1
      if (dataCache.containsKey(dataSize)) {
        b = dataCache.get(dataSize);
      } else {
        b = Utils.generateData(dataSize);
        dataCache.put(dataSize, b);
      }
      list.add(b);
    }

    // last bolt
    if (last) {
      SingleTrace singleTrace = new SingleTrace();
      b = Utils.serialize(kryo, singleTrace);
      list.add(b);
    }

    // middle bolt
    if (!last && !first) {
      list.add(body);
    }

    if (debug) {
      LOG.info("Messagre received");
    }
    list.add(sensorId);
    list.add(time);
    collector.emit(Constants.Fields.CHAIN_STREAM, list);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declareStream(Constants.Fields.CHAIN_STREAM, new Fields(
        Constants.Fields.BODY,
        Constants.Fields.SENSOR_ID_FIELD,
        Constants.Fields.TIME_FIELD));
  }
}
