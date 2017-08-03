package edu.indiana.soic.dsc.stream.collectives.thrput;

import com.twitter.heron.api.bolt.BaseRichBolt;
import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Tuple;
import edu.indiana.soic.dsc.stream.collectives.Constants;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ThroughputSendBolt extends BaseRichBolt {
  OutputCollector outputCollector;
  @Override
  public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
    this.outputCollector = outputCollector;
  }

  @Override
  public void execute(Tuple tuple) {
    List<Object> list = new ArrayList<Object>();
    list.add(0);
    outputCollector.emit(Constants.Fields.CHAIN_STREAM, list);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declareStream(Constants.Fields.CHAIN_STREAM, new Fields(
        Constants.Fields.BODY));
  }
}
