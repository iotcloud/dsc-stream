package edu.indiana.soic.dsc.stream.perf.latency;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import edu.indiana.soic.dsc.stream.perf.Constants;
import edu.indiana.soic.dsc.stream.perf.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

public class LatencySpout extends BaseRichSpout {
  private static Logger LOG = LoggerFactory.getLogger(LatencySpout.class);

  // keep track of the send times with id
  private Map<String, Send> sendTimes = new HashMap<String, Send>();
  // average latency for a message size
  private Map<Integer, Long> averageTimes = new HashMap<>();
  // latencies for the current message size
  private List<Long> latencies = new ArrayList<>();

  private long noOfMessages = 0;
  private long noOfEmptyMessages = 1000;
  private List<Integer> messageSizes = new ArrayList<Integer>();
  // current message size index
  private int currentSendIndex = 0;
  private SpoutOutputCollector collector;
  private int currentCount = 0;
  private byte []data = null;
  private String fileName;
  // we count the acks
  private int currentAckCount = 0;

  private enum SendingType {
    DATA,
    EMPTY
  }

  private LatencySpout.SendingType sendState = LatencySpout.SendingType.EMPTY;

  @Override
  public void open(Map<String, Object> stormConf, TopologyContext topologyContext, SpoutOutputCollector outputCollector) {
    noOfMessages = (Integer) stormConf.get(Constants.ARGS_THRPUT_NO_MSGS);
    messageSizes = (List<Integer>) stormConf.get(Constants.ARGS_THRPUT_SIZES);
    noOfEmptyMessages = (Integer) stormConf.get(Constants.ARGS_THRPUT_NO_EMPTY_MSGS);
    fileName = (String) stormConf.get(Constants.ARGS_THRPUT_FILENAME);

    this.collector = outputCollector;
  }

  @Override
  public void nextTuple() {
    if (currentSendIndex >= messageSizes.size()) {
      return;
    }

    // we haven't received all the acks, so wait
    if (sendState == SendingType.DATA && currentAckCount < noOfMessages && currentCount >= noOfMessages) {
      try {
        Thread.sleep(1);
      } catch (InterruptedException ignore) {}
      return;
    }

    int size = 1;
    if (currentCount == 0) {
      if (sendState == LatencySpout.SendingType.EMPTY) {
        // LOG.info("Empty message generate");
        data = Utils.generateData(1);
      } else {
        // LOG.info("Data message generate");
        size = messageSizes.get(currentSendIndex);
        data = Utils.generateData(size);
      }
    } else {
      if (sendState == LatencySpout.SendingType.DATA) {
        size = messageSizes.get(currentSendIndex);
      }
    }
    currentCount++;

    List<Object> list = new ArrayList<Object>();
    list.add(data);
    list.add(currentCount);
    list.add(size);
    String id = UUID.randomUUID().toString();
    // we only keep track of the data items
    if (sendState == SendingType.DATA) {
      sendTimes.put(id, new Send(id, size, currentCount, System.nanoTime()));
    }
    collector.emit(Constants.Fields.CHAIN_STREAM, list, id);

    if (sendState == LatencySpout.SendingType.EMPTY) {
      if (currentCount >= noOfEmptyMessages) {
        currentCount = 0;
        sendState = LatencySpout.SendingType.DATA;
      }
    } else if (sendState == LatencySpout.SendingType.DATA) {
      if (currentCount >= noOfMessages) {
        currentCount = 0;
        currentSendIndex++;
        sendState = LatencySpout.SendingType.EMPTY;
      }
    }
  }

  @Override
  public void ack(Object o) {
    if (sendTimes.containsKey(o.toString())) {
      Send send = sendTimes.remove(o.toString());
      long receiveTime = System.nanoTime();
      int currentSize = send.size;
      latencies.add((receiveTime - send.time));
      // we have received all the times
      if (latencies.size() == noOfMessages) {
        writeLatencies(currentSize + "", latencies);
        latencies.clear();
        currentAckCount = 0;
      }
      currentAckCount++;
    }
    super.ack(o);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declareStream(Constants.Fields.CHAIN_STREAM, new Fields(
        Constants.Fields.BODY,
        Constants.Fields.MESSAGE_INDEX_FIELD,
        Constants.Fields.MESSAGE_SIZE_FIELD));
  }

  private void writeLatencies(String name, List<Long> latencies) {
    StringBuilder sb = new StringBuilder();
    for (Long l : latencies) {
      sb.append(l).append("\n");
    }
    writeFile(fileName + "/" + name, sb.toString());
  }

  private void writeFile(String file, String line) {
    try(FileWriter fw = new FileWriter(file, true);
        BufferedWriter bw = new BufferedWriter(fw);
        PrintWriter out = new PrintWriter(bw)) {
        out.print(line);
    } catch (IOException e) {
      //exception handling left as an exercise for the reader
      LOG.error("Failed to write to the file");
    }
  }
}
