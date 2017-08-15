package edu.indiana.soic.dsc.stream.debs.bolt;

import com.twitter.heron.api.bolt.BaseRichBolt;
import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Tuple;
import edu.indiana.soic.dsc.stream.debs.Constants;

import java.io.*;
import java.util.Map;

public class OutputBolt extends BaseRichBolt {
  private String fileName;

  PrintWriter bufferedWriter;

  @Override
  public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
    fileName = (String) map.get(Constants.ARGS_OUT_FILE);
  }

  @Override
  public void execute(Tuple tuple) {
    Object input = tuple.getValueByField(Constants.DATA_FIELD);
    Object time = tuple.getValueByField(Constants.TIME_FIELD);

    bufferedWriter.println();
  }

  private void openFile(String openFile) {
    try {
      bufferedWriter = new PrintWriter(new BufferedWriter(new FileWriter(openFile)));
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
