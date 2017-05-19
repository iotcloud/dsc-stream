package edu.indiana.soic.dsc.stream.collectives;

import com.esotericsoftware.kryo.Kryo;
import com.twitter.heron.api.bolt.BaseRichBolt;
import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CollectiveLastBolt extends BaseRichBolt {
  private static Logger LOG = LoggerFactory.getLogger(CollectiveLastBolt.class);
  private OutputCollector outputCollector;
  private int count = 0;
  private boolean debug = false;
  private int printInveral = 0;
  private TopologyContext context;
  private Kryo kryo;

  @Override
  public void prepare(Map stormConf, TopologyContext topologyContext, OutputCollector outputCollector) {
    this.debug = (boolean) stormConf.get(Constants.ARGS_DEBUG);
    this.printInveral = (int) stormConf.get(Constants.ARGS_PRINT_INTERVAL);

    this.outputCollector = outputCollector;
    this.context = topologyContext;
    this.kryo = new Kryo();
    Utils.registerClasses(kryo);
  }

  @Override
  public void execute(Tuple tuple) {
    throughputProcess(tuple);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declareStream(Constants.Fields.CHAIN_STREAM, new Fields(
        Constants.Fields.BODY,
        Constants.Fields.SENSOR_ID_FIELD,
        Constants.Fields.TIME_FIELD));
  }

  private void throughputProcess(Tuple tuple) {
    try {
      outputCollector.ack(tuple);

      Long time = tuple.getLongByField(Constants.Fields.TIME_FIELD);
      if (debug && count % printInveral == 0) {
        LOG.info(context.getThisTaskId() + " Last Received tuple: " + count);
      }

      count++;
      byte []b;
      List<Object> list = new ArrayList<Object>();
      SingleTrace singleTrace = new SingleTrace();
      b = Utils.serialize(kryo, singleTrace);
      list.add(b);
      list.add("");
      list.add(time);

      outputCollector.emit(Constants.Fields.CHAIN_STREAM, list);
    } catch (Throwable t) {
      t.printStackTrace();
    }
  }
}
