package edu.indiana.soic.dsc.stream.debs.bolt;

import com.twitter.heron.api.bolt.IOutputCollector;
import com.twitter.heron.api.grouping.IReduce;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Tuple;
import edu.indiana.soic.dsc.stream.debs.msg.PlugMsg;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class ReductionFunction implements IReduce {
  private Logger LOG = Logger.getLogger(ReductionFunction.class.getName());

  private int thisTaskId;

  // tasId, <startTime, PlugMsg>
  private Map<Integer, Map<Long, PlugMsg>> plugMessages = new HashMap<>();

  private Long currentEndTIme;

  @Override
  public void prepare(Map<String, Object> map, TopologyContext topologyContext,
                      List<Integer> list, IOutputCollector iOutputCollector) {
    thisTaskId = topologyContext.getThisTaskId();
  }

  @Override
  public void execute(int i, Tuple tuple) {

  }

  @Override
  public void cleanup() {

  }
}
