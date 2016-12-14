package edu.indiana.soic.dsc.stream.perf.latency;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import edu.indiana.soic.dsc.stream.perf.Constants;
import edu.indiana.soic.dsc.stream.perf.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class LatencyLastBolt extends BaseRichBolt {
  private static Logger LOG = LoggerFactory.getLogger(LatencyLastBolt.class);
  private OutputCollector collector;
  private String id;
  private boolean debug;

  @Override
  public void prepare(Map stormConf, TopologyContext topologyContext, OutputCollector outputCollector) {
    this.collector = outputCollector;
    this.id = topologyContext.getThisComponentId();
    this.debug = (boolean) stormConf.get(Constants.ARGS_DEBUG);
  }

  @Override
  public void execute(Tuple tuple) {
    Long time = tuple.getLongByField(Constants.Fields.TIME_FIELD);
    Integer size = tuple.getIntegerByField(Constants.Fields.MESSAGE_SIZE_FIELD);
    Long previousTime = null;
    if (tuple.getFields().contains(Constants.Fields.TIME_FIELD2)) {
      previousTime = tuple.getLongByField(Constants.Fields.TIME_FIELD2);
    }
    if (debug) {
      LOG.info("Messagre received");
      Utils.printTime(id, size, time, previousTime);
    }

    collector.ack(tuple);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declareStream(Constants.Fields.CHAIN_STREAM, new Fields(
        Constants.Fields.BODY,
        Constants.Fields.SENSOR_ID_FIELD,
        Constants.Fields.TIME_FIELD,
        Constants.Fields.TIME_FIELD2));
  }
}
