package edu.indiana.soic.dsc.stream.perf;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.immutable.Stream;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class ThroughputAckSpout extends BaseRichSpout {
  private static Logger LOG = LoggerFactory.getLogger(ThroughputSpout.class);

  private long noOfMessages = 0;
  private long noOfEmptyMessages = 1000;
  private List<Integer> messageSizes = new ArrayList<Integer>();
  private int currentSendIndex = 0;
  private SpoutOutputCollector collector;
  private int currentSendCount = 0;
  private byte []data = null;
  private int outstandingTuples = 0;
  private int maxOutstandingTuples = 100;
  private boolean debug;
  private int totalSendCount = 0;
  private int ackReceiveCount = 0;
  private long firstThroughputSendTime = 0;
  private String fileName;
  private String id;
  private long start = 0;
  private int printInveral = 0;
  private long lastSendTime = 0;
  private boolean startFailing = false;
  private int totalAckCount = 0;
  private int totalFailCount = 0;
  private boolean fileWritten = false;
  private int spoutParallel = 1;
  private int parallel = 1;

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
    fileName = (String) stormConf.get(Constants.ARGS_THRPUT_FILENAME);
    printInveral = (int) stormConf.get(Constants.ARGS_PRINT_INTERVAL);
    id = topologyContext.getThisComponentId() + "_" + topologyContext.getThisTaskId();
    start = System.currentTimeMillis();
    lastSendTime = System.currentTimeMillis();
    spoutParallel = (int) stormConf.get(Constants.ARGS_SPOUT_PARALLEL);
    parallel = (int) stormConf.get(Constants.ARGS_PARALLEL);
    maxOutstandingTuples = (int) stormConf.get(Constants.ARGS_MAX_PENDING);
  }

  @Override
  public void nextTuple() {
    try {
      if (System.currentTimeMillis() - start < 10000 ) {
        return;
      }

      if (currentSendIndex >= messageSizes.size()) {
        return;
      }

      // we cannot send anything until we get enough acks
      if (outstandingTuples >= maxOutstandingTuples) {
        if (debug) {
          LOG.info("Next tuple return, Send cound: " + totalSendCount + " outstanding: " + outstandingTuples);
        }
        return;
      }

      if (sendState == SendingType.DATA && currentSendCount >= noOfMessages) {
        return;
      }

      if (sendState == SendingType.EMPTY && currentSendCount >= noOfEmptyMessages) {
        return;
      }

//      if (System.currentTimeMillis() - lastSendTime < 2) {
//        return;
//      }
      lastSendTime = System.currentTimeMillis();
      int size = 1;
      if (currentSendCount == 0) {
        if (sendState == SendingType.EMPTY) {
          // LOG.info("Empty message generate");
          data = Utils.generateData(1);
        } else {
          // LOG.info("Data message generate");
          size = messageSizes.get(currentSendIndex);
          data = Utils.generateData(size);
        }
        firstThroughputSendTime = System.currentTimeMillis();
      } else {
        if (sendState == SendingType.DATA) {
          size = messageSizes.get(currentSendIndex);
        }
      }
      currentSendCount++;

      List<Object> list = new ArrayList<Object>();
      list.add(data);
      list.add(currentSendCount);
      list.add(size);
      list.add(System.nanoTime());
      list.add(System.nanoTime());
      String id = UUID.randomUUID().toString();
//      String id = String.valueOf(totalSendCount);
      collector.emit(Constants.Fields.CHAIN_STREAM, list, id);
      if (debug) {
        if (totalSendCount % printInveral == 0) {
          LOG.info("Send cound: " + totalSendCount + " outstanding: " + outstandingTuples + " id: " + id);
        }
      }
      totalSendCount++;
      outstandingTuples++;
    } catch (Throwable t) {
      t.printStackTrace();
    }
  }

  @Override
  public void ack(Object o) {
    if ((debug && ackReceiveCount % printInveral == 0) || startFailing) {
      LOG.info("Acked tuple: " + o.toString() + " total acked: " + totalAckCount + " send: " + totalSendCount + " faile: " + totalFailCount + " " + o.toString());
    }
    totalAckCount++;
    handleAck(false, 0);
  }

  @Override
  public void fail(Object o) {
    LOG.info("Failed to process tuple: " + o.toString() + " total acked: " + totalAckCount + " send: " + totalSendCount + " faile: " + totalFailCount + " " + o.toString());
    startFailing = true;
    totalFailCount++;
    handleAck(true, o);
  }

  public void handleAck(boolean fail, Object ack) {
    outstandingTuples--;
    ackReceiveCount++;
    if (sendState == SendingType.EMPTY) {
      if (currentSendCount >= noOfEmptyMessages && ackReceiveCount >= noOfEmptyMessages) {
        currentSendCount = 0;
        if (currentSendIndex < messageSizes.size()) {
          LOG.info("Started processing size: " + messageSizes.get(currentSendIndex));
          System.out.println("Started processing size: " + messageSizes.get(currentSendIndex));
        }
        ackReceiveCount = 0;
        sendState = SendingType.DATA;
      }
    } else if (sendState == SendingType.DATA) {
      if (currentSendCount >= noOfMessages - noOfEmptyMessages && ackReceiveCount >= noOfMessages - noOfEmptyMessages && !fileWritten) {
        int size = messageSizes.get(currentSendIndex);
        System.out.println("Write file for size: " + size + String.format("sendCount: %d ackReceive: %d", currentSendCount, ackReceiveCount));
        long time = System.currentTimeMillis() - firstThroughputSendTime;
        String currentOutPut =  spoutParallel + "x" + parallel + " " + size + " " + (noOfMessages - noOfEmptyMessages) + " " + time + " " + (noOfMessages - noOfEmptyMessages + 0.0) / (time / 1000.0);
        writeFile(currentOutPut);
        fileWritten = true;
      } else if (currentSendCount >= noOfMessages && ackReceiveCount >= noOfMessages) {
        int size = messageSizes.get(currentSendIndex);
        LOG.info("Finished message size: " + size);
        currentSendCount = 0;
        currentSendIndex++;
        ackReceiveCount = 0;
        sendState = SendingType.EMPTY;
        fileWritten = false;
      }
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

  private void writeFile(String line) {
    try(FileWriter fw = new FileWriter(fileName + id, true);
        BufferedWriter bw = new BufferedWriter(fw);
        PrintWriter out = new PrintWriter(bw)) {
      out.println(line);
    } catch (IOException e) {
      //exception handling left as an exercise for the reader
      LOG.error("Failed to write to the file", e);
      e.printStackTrace();
    }
  }
}

