package edu.indiana.soic.dsc.stream.debs.bolt;

import com.twitter.heron.api.bolt.IOutputCollector;
import com.twitter.heron.api.grouping.IReduce;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Tuple;

import java.util.List;
import java.util.Map;

public class ReductionFunction implements IReduce {
  @Override
  public void prepare(Map<String, Object> map, TopologyContext topologyContext, List<Integer> list, IOutputCollector iOutputCollector) {

  }

  @Override
  public void execute(int i, Tuple tuple) {

  }

  @Override
  public void cleanup() {

  }
}
