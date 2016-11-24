package edu.indiana.soic.dsc.stream.perf;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Map;

public class ThroughputLastBolt extends BaseRichBolt {
  private static Logger LOG = LoggerFactory.getLogger(ThroughputLastBolt.class);
  private long noOfMessages = 0;
  private long noOfEmptyMessages = 0;
  private String fileName;
  private long firstThroughputRecvTime = 0;
  private String currentOutPut;
  private ReceiveType receiveState = ReceiveType.EMPTY;

  private enum ReceiveType {
    DATA,
    EMPTY
  }

  @Override
  public void prepare(Map stormConf, TopologyContext topologyContext, OutputCollector outputCollector) {
    noOfMessages = (Long) stormConf.get(Constants.ARGS_THRPUT_NO_MSGS);
    fileName = (String) stormConf.get(Constants.ARGS_THRPUT_FILENAME);
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
    String stream = tuple.getSourceStreamId();
    if (stream.equals(Constants.Fields.CONTROL_STREAM)) {
      return;
    }

    Long size = tuple.getLongByField(Constants.Fields.MESSAGE_SIZE_FIELD);
    Integer messageCount = tuple.getIntegerByField(Constants.Fields.MESSAGE_INDEX_FIELD);

    if (receiveState == ReceiveType.EMPTY) {
      if (messageCount == noOfEmptyMessages) {
        receiveState = ReceiveType.DATA;
        firstThroughputRecvTime = System.nanoTime();
      }
    } else if (receiveState == ReceiveType.DATA) {
      if (messageCount == noOfMessages) {
        receiveState = ReceiveType.EMPTY;
        long time = System.nanoTime() - firstThroughputRecvTime;
        firstThroughputRecvTime = 0;
        currentOutPut = size + " " + (messageCount + 0.0)/ (time / 1000000000.0);
        writeFile(currentOutPut);
      }
    }
  }

  private void writeFile(String line) {
    try {
      Files.write(Paths.get(fileName), line.getBytes(), StandardOpenOption.APPEND);
    }catch (IOException e) {
      //exception handling left as an exercise for the reader
      LOG.error("Failed to write to the file");
    }
  }
}
