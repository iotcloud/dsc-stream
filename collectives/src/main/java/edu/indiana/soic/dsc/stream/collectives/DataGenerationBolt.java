package edu.indiana.soic.dsc.stream.collectives;

import com.twitter.heron.api.bolt.BaseRichBolt;
import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataGenerationBolt extends BaseRichBolt {
  private static Logger LOG = LoggerFactory.getLogger(DataGenerationBolt.class);
  private OutputCollector collector;
  private boolean debug;
  private int count;
  private int printInveral;
  private TopologyContext context;
  private Map<Integer, byte[]> dataCache = new HashMap<>();

  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    this.collector = outputCollector;
    this.debug = (boolean) map.get(Constants.ARGS_DEBUG);
    printInveral = (int) map.get(Constants.ARGS_PRINT_INTERVAL);
    context = topologyContext;
  }

  @Override
  public void execute(Tuple tuple) {
    try {
      Object body = tuple.getValueByField(Constants.Fields.BODY);
      Integer size = tuple.getIntegerByField(Constants.Fields.MESSAGE_SIZE_FIELD);
      Object index = tuple.getValueByField(Constants.Fields.MESSAGE_INDEX_FIELD);
      Long time = tuple.getLongByField(Constants.Fields.TIME_FIELD);

      List<Object> list = new ArrayList<Object>();
      byte[] b;

      ByteBuffer wrapped = ByteBuffer.wrap((byte[]) body); // big-endian by default
      int dataSize = wrapped.getInt(); // 1
      if (dataCache.containsKey(dataSize)) {
        b = dataCache.get(dataSize);
      } else {
        b = Utils.generateData(dataSize);
        dataCache.put(dataSize, b);
      }

      if (debug && count % printInveral == 0) {
        LOG.info("Data size: " + dataSize);
      }

      if (size != b.length) {
        LOG.error("The message size is in-correct");
        System.out.println("The message size is in-correct");
      }

      list.add(b);
      list.add(index);
      list.add(size);
      list.add(time);
      list.add(System.nanoTime());

      count++;
      if (debug && count % printInveral == 0) {
        long elapsed = (System.nanoTime() - time) / 1000000;
        LOG.info("" + context.getThisTaskId() + " Passthotugh Messagre received count: " + count + " size: " + b.length);
      }

      collector.emit(Constants.Fields.CHAIN_STREAM, tuple, list);
      collector.ack(tuple);
    } catch (Throwable t) {
      t.printStackTrace();
    }
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
