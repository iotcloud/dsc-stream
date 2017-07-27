package edu.indiana.soic.dsc.stream.collectives;

import com.esotericsoftware.kryo.Kryo;
import com.twitter.heron.api.bolt.BaseRichBolt;
import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Tuple;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class OriginBolt extends BaseRichBolt {
  private static Logger LOG = Logger.getLogger(OriginBolt.class.getName());

  private TopologyContext context;
  private OutputCollector collector;
  private Kryo kryo;
  private boolean debug;
  private int printInveral;
  private int index = 0;

  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    this.context = topologyContext;
    this.collector = outputCollector;
    this.kryo = new Kryo();
    Utils.registerClasses(kryo);
    this.debug = (boolean) map.get(Constants.ARGS_DEBUG);
    this.printInveral = (int) map.get(Constants.ARGS_PRINT_INTERVAL);
  }

  @Override
  public void execute(Tuple tuple) {
    Object body = tuple.getValueByField(Constants.Fields.BODY);
    Object time = tuple.getValueByField(Constants.Fields.TIME_FIELD);
    Long t = Long.valueOf(time.toString());

    List<Object> list = new ArrayList<Object>();
    index++;
    // first bolt but not last
    ByteBuffer wrapped = ByteBuffer.wrap((byte[]) body); // big-endian by default
    int dataSize = wrapped.getInt(); // 1

    //LOG.log(Level.INFO, "Received message: " + index + " for size: " + dataSize );
    if (debug && index % printInveral == 0) {
      long elapsed = (System.nanoTime() - t) / 1000000;
      LOG.log(Level.INFO, "Received message: " + index + " for size: " + dataSize + " " + elapsed);
    }

    list.add(body);
    list.add(index);
    list.add(dataSize);
    list.add(t);
    list.add(t);
    collector.emit(Constants.Fields.CHAIN_STREAM, list);
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
