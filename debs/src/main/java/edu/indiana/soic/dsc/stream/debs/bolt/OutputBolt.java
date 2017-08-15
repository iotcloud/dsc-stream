package edu.indiana.soic.dsc.stream.debs.bolt;

import com.twitter.heron.api.bolt.BaseRichBolt;
import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Tuple;

import java.util.Map;

public class OutputBolt extends BaseRichBolt {
  @Override
  public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {

  }

  @Override
  public void execute(Tuple tuple) {

  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

  }
}
