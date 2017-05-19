package edu.indiana.soic.dsc.stream.collectives;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.esotericsoftware.kryo.Kryo;
import edu.indiana.soic.dsc.stream.perf.Constants;
import edu.indiana.soic.dsc.stream.perf.Utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class OriginBolt extends BaseRichBolt {
  private static Logger LOG = Logger.getLogger(OriginBolt.class.getName());

  private TopologyContext context;
  private OutputCollector collector;
  private Kryo kryo;
  private boolean debug;
  private Map<Integer, byte[]> dataCache = new HashMap<>();

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
    ByteBuffer wrapped = ByteBuffer.wrap((byte[]) body); // big-endian by default
    int dataSize = wrapped.getInt(); // 1
    if (dataCache.containsKey(dataSize)) {
      b = dataCache.get(dataSize);
    } else {
      b = Utils.generateData(dataSize);
      dataCache.put(dataSize, b);
    }
    list.add(b);

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
