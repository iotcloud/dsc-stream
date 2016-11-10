package edu.indiana.soic.dsc.stream.perf;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.esotericsoftware.kryo.Kryo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PassThroughBolt extends BaseRichBolt {
  private TopologyContext context;
  private OutputCollector collector;
  private Kryo kryo;
  private boolean last;

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
  }

  @Override
  public void execute(Tuple tuple) {
    Object body = tuple.getValueByField(Constants.Fields.BODY);
    Object time = tuple.getValueByField(Constants.Fields.TIME_FIELD);
    Object sensorId = tuple.getValueByField(Constants.Fields.SENSOR_ID_FIELD);

    List<Object> list = new ArrayList<Object>();
    if (last) {
      SingleTrace singleTrace = new SingleTrace();
      byte []b = Utils.serialize(kryo, singleTrace);
      list.add(b);
    } else {
      list.add(body);
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
