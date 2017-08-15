package edu.indiana.soic.dsc.stream.debs.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;

import java.util.Map;

public class FileReadingSpout extends BaseRichSpout {
  @Override
  public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {

  }

  @Override
  public void nextTuple() {

  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declare(new Fields("time", "reading"));
  }
}
