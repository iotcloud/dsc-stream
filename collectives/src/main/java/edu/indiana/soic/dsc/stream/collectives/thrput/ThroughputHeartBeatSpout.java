package edu.indiana.soic.dsc.stream.collectives.thrput;

import com.twitter.heron.api.spout.BaseRichSpout;
import com.twitter.heron.api.spout.SpoutOutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Fields;
import edu.indiana.soic.dsc.stream.collectives.Constants;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * This spout sends a message every 1 secs to invoke the OriginBolt
 */
public class ThroughputHeartBeatSpout extends BaseRichSpout {
  SpoutOutputCollector spoutOutputCollector;
  long lastSend = 0;
  long start = 0;
  @Override
  public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
    this.spoutOutputCollector = spoutOutputCollector;
    lastSend = System.currentTimeMillis();
    start = System.currentTimeMillis();
  }

  @Override
  public void nextTuple() {
    if (System.currentTimeMillis() - start < 15000 ) {
      return;
    }

    if (System.currentTimeMillis() - lastSend > 1000) {
      List<Object> tuple = new ArrayList<Object>();
      tuple.add(0);
      spoutOutputCollector.emit(tuple);
      lastSend = System.currentTimeMillis();
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declare(new Fields(
        Constants.Fields.BODY));
  }
}
