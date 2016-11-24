package edu.indiana.soic.dsc.stream.perf;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ThroughputSpout extends BaseRichSpout {
  private static Logger LOG = LoggerFactory.getLogger(ThroughputSpout.class);

  private long noOfMessages = 0;
  private long noOfEmptyMessages = 1000;
  private List<Long> messageSizes = new ArrayList<Long>();
  private int currentSendIndex = 0;
  private SpoutOutputCollector collector;
  private int currentCount = 0;
  private byte []data = null;

  private enum SendingType {
    DATA,
    EMPTY
  }

  private SendingType sendState = SendingType.EMPTY;

  @Override
  public void open(Map<String, Object> stormConf, TopologyContext topologyContext, SpoutOutputCollector outputCollector) {
    noOfMessages = (Long) stormConf.get(Constants.ARGS_THRPUT_NO_MSGS);
    messageSizes = (List<Long>) stormConf.get(Constants.ARGS_THRPUT_SIZES);
    noOfEmptyMessages = (Long) stormConf.get(Constants.ARGS_THRPUT_NO_EMPTY_MSGS);
    this.collector = outputCollector;
  }

  @Override
  public void nextTuple() {
    if (currentSendIndex >= messageSizes.size()) {
      return;
    }

    if (currentCount == 0) {
      if (sendState == SendingType.EMPTY) {
        data = Utils.generateData(1);
      } else {
        data = Utils.generateData(((Long)messageSizes.get(currentSendIndex)).intValue());
      }
    }
    currentCount++;

    List<Object> list = new ArrayList<Object>();
    long time = System.nanoTime();
    list.add(data);
    list.add(currentCount);
    list.add(time);
    collector.emit(Constants.Fields.CHAIN_STREAM, list);

    if (sendState == SendingType.EMPTY) {
      if (currentCount >= noOfEmptyMessages) {
        currentCount = 0;
        sendState = SendingType.DATA;
      }
    } else if (sendState == SendingType.DATA) {
      if (currentCount >= noOfMessages) {
        currentCount = 0;
        currentSendIndex++;
        sendState = SendingType.EMPTY;
      }
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declareStream(Constants.Fields.CHAIN_STREAM, new Fields(
        Constants.Fields.BODY,
        Constants.Fields.MESSAGE_INDEX_FIELD,
        Constants.Fields.MESSAGE_SIZE_FIELD));
  }
}
