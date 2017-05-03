package edu.indiana.soic.dsc.stream.collectives;

import com.twitter.heron.api.bolt.IOutputCollector;
import com.twitter.heron.api.grouping.IReduce;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Tuple;
import edu.indiana.soic.dsc.stream.perf.Constants;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class CountReduceFunction implements IReduce {
  private static Logger LOG = Logger.getLogger(CountReduceFunction.class.getName());

  private IOutputCollector collector;

  private TopologyContext context;

  @Override
  public void prepare(Map<String, Object> map, TopologyContext topologyContext, List<Integer> list,
                      IOutputCollector iOutputCollector) {
    this.collector = iOutputCollector;
    this.context = topologyContext;
    LOG.info("Reduce function");
  }

  @Override
  public void execute(int i, Tuple tuple) {
    LOG.info(String.format("%d Reduction Received message from %d", context.getThisTaskId(), i));
    List<Tuple> anchors = new ArrayList<>();
    anchors.add(tuple);

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

    collector.ack(tuple);
    collector.emit(Constants.Fields.CHAIN_STREAM, anchors, list);
  }

  @Override
  public void cleanup() {

  }
}
