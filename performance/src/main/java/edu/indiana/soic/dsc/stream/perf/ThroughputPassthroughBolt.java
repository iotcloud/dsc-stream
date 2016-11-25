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

public class ThroughputPassthroughBolt extends BaseRichBolt {
  private TopologyContext context;
  private OutputCollector collector;
  private Kryo kryo;

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
    Object size = tuple.getValueByField(Constants.Fields.MESSAGE_SIZE_FIELD);
    Object index = tuple.getValueByField(Constants.Fields.MESSAGE_INDEX_FIELD);

    List<Object> list = new ArrayList<Object>();
    list.add(body);
    list.add(index);
    list.add(size);
    collector.emit(Constants.Fields.CHAIN_STREAM, list);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declareStream(Constants.Fields.CHAIN_STREAM, new Fields(
        Constants.Fields.BODY,
        Constants.Fields.MESSAGE_INDEX_FIELD,
        Constants.Fields.MESSAGE_SIZE_FIELD));
  }
}