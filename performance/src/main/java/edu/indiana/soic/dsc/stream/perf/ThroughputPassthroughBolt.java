package edu.indiana.soic.dsc.stream.perf;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ThroughputPassthroughBolt extends BaseRichBolt {
  private static Logger LOG = LoggerFactory.getLogger(ThroughputPassthroughBolt.class);
  private OutputCollector collector;
  private List<Integer> messageSizes = new ArrayList<Integer>();
  private String id;
  private boolean debug;

  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    this.collector = outputCollector;
    messageSizes = (List<Integer>) map.get(Constants.ARGS_THRPUT_SIZES);
    this.id = topologyContext.getThisComponentId();
    this.debug = (boolean) map.get(Constants.ARGS_DEBUG);
  }

  @Override
  public void execute(Tuple tuple) {
    Object body = tuple.getValueByField(Constants.Fields.BODY);
    Object size = tuple.getValueByField(Constants.Fields.MESSAGE_SIZE_FIELD);
    Object index = tuple.getValueByField(Constants.Fields.MESSAGE_INDEX_FIELD);
    Long time = tuple.getLongByField(Constants.Fields.TIME_FIELD);

    List<Object> list = new ArrayList<Object>();
    byte []b = (byte[]) body;
    if (!messageSizes.contains(b.length) && b.length != 1) {
      LOG.error("The message size is in-correct");
    }
    list.add(body);
    list.add(index);
    list.add(size);
    list.add(System.nanoTime());
    list.add(time);

    long now = System.nanoTime();
    long expired = (now - time);
    //LOG.info("Time: " + (expired));
    //System.out.println("ID: " + id + " Time: " + expired);
    if (debug) {
      LOG.info("ID: " + id + " Time: " + expired);
      System.out.println("ID: " + id + " Time: " + expired);
    }

    List<Tuple> anchors = new ArrayList<>();
    anchors.add(tuple);
    collector.emit(Constants.Fields.CHAIN_STREAM, anchors, list);
    collector.ack(tuple);
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