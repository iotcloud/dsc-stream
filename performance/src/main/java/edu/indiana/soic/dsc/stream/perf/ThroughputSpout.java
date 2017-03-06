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
import java.util.UUID;

public class ThroughputSpout extends BaseRichSpout {
  private static Logger LOG = LoggerFactory.getLogger(ThroughputSpout.class);

  private long noOfMessages = 0;
  private long noOfEmptyMessages = 1000;
  private List<Integer> messageSizes = new ArrayList<Integer>();
  private int currentSendIndex = 0;
  private SpoutOutputCollector collector;
  private int currentCount = 0;
  private byte []data = null;
  private int outstandingTuples = 0;
  private int maxOutstandingTuples = 100;
  private boolean debug;

  private enum SendingType {
    DATA,
    EMPTY
  }

  private SendingType sendState = SendingType.EMPTY;

  @Override
  public void open(Map<String, Object> stormConf, TopologyContext topologyContext, SpoutOutputCollector outputCollector) {
    noOfMessages = (Integer) stormConf.get(Constants.ARGS_THRPUT_NO_MSGS);
    messageSizes = (List<Integer>) stormConf.get(Constants.ARGS_THRPUT_SIZES);
    noOfEmptyMessages = (Integer) stormConf.get(Constants.ARGS_THRPUT_NO_EMPTY_MSGS);
    this.collector = outputCollector;
    this.debug = (boolean) stormConf.get(Constants.ARGS_DEBUG);
  }

  @Override
  public void nextTuple() {
    try {
      if (currentSendIndex >= messageSizes.size()) {
        return;
      }

      // we cannot send anything until we get enough acks
      if (outstandingTuples >= maxOutstandingTuples) {
        if (debug) {
          LOG.info(String.format("Waiting for acks %d: ", outstandingTuples));
        }
        return;
      }

      int size = 1;
      if (currentCount == 0) {
        if (sendState == SendingType.EMPTY) {
          // LOG.info("Empty message generate");
          data = Utils.generateData(1);
        } else {
          // LOG.info("Data message generate");
          size = messageSizes.get(currentSendIndex);
          data = Utils.generateData(size);
        }
      } else {
        if (sendState == SendingType.DATA) {
          size = messageSizes.get(currentSendIndex);
        }
      }
      currentCount++;

      List<Object> list = new ArrayList<Object>();
      list.add(data);
      list.add(currentCount);
      list.add(size);
      list.add(System.nanoTime());
      list.add(System.nanoTime());
      String id = UUID.randomUUID().toString();
      collector.emit(Constants.Fields.CHAIN_STREAM, list, id);
      outstandingTuples++;

      if (sendState == SendingType.EMPTY) {
        if (currentCount >= noOfEmptyMessages) {
          currentCount = 0;
          if (currentSendIndex < messageSizes.size()) {
            LOG.info("Started processing size: " + messageSizes.get(currentSendIndex));
            System.out.println("Started processing size: " + messageSizes.get(currentSendIndex));
          }
          sendState = SendingType.DATA;
        }
      } else if (sendState == SendingType.DATA) {
        if (currentCount >= noOfMessages) {
          currentCount = 0;
          currentSendIndex++;
          sendState = SendingType.EMPTY;
        }
      }
    } catch (Throwable t) {
      t.printStackTrace();
    }
  }

  @Override
  public void ack(Object o) {
    outstandingTuples--;
  }

  @Override
  public void fail(Object o) {
    super.fail(o);
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
