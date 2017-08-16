package edu.indiana.soic.dsc.stream.debs.bolt;

import com.twitter.heron.api.bolt.BaseRichBolt;
import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Tuple;
import edu.indiana.soic.dsc.stream.debs.Constants;
import edu.indiana.soic.dsc.stream.debs.msg.PlugMsg;

import java.io.*;
import java.util.Map;
import java.util.logging.Logger;

public class ReductionBolt extends BaseRichBolt {
  private static Logger LOG = Logger.getLogger(ReductionBolt.class.getName());

  private String fileName;

  PrintWriter hourlyBufferWriter;
  PrintWriter dailyBufferWriter;

  @Override
  public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
    fileName = (String) map.get(Constants.ARGS_OUT_FILE);
    openFile(fileName + "_" + topologyContext.getThisTaskId());
  }

  @Override
  public void execute(Tuple tuple) {
    Object output = tuple.getValueByField(Constants.OUT_FILED);
    PlugMsg msg = (PlugMsg) output;

    LOG.info(msg.averageDaily + "," + msg.averageHourly + ", " + msg.aggregatedPlugs.size());
  }

  private void openFile(String openFile) {
    try {
      hourlyBufferWriter = new PrintWriter(new BufferedWriter(new FileWriter(openFile + "_hourly.csv")));
      dailyBufferWriter = new PrintWriter(new BufferedWriter(new FileWriter(openFile + "_daily.csv")));
    } catch (FileNotFoundException e) {
      throw new RuntimeException("Failed to open file" + openFile, e);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

  }
}
