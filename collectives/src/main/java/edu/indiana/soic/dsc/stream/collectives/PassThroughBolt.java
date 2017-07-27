package edu.indiana.soic.dsc.stream.collectives;

import com.esotericsoftware.kryo.Kryo;
import com.twitter.heron.api.bolt.BaseRichBolt;
import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Tuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PassThroughBolt extends BaseRichBolt {
  OutputCollector outputCollector;
  private Kryo kryo;
  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    this.outputCollector = outputCollector;
    this.kryo = new Kryo();
    Utils.registerClasses(kryo);
  }

  @Override
  public void execute(Tuple tuple) {
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
    outputCollector.emit(Constants.Fields.CHAIN_STREAM, list);
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
