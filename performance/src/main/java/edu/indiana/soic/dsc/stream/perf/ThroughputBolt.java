package edu.indiana.soic.dsc.stream.perf;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ThroughputBolt extends BaseRichBolt {
  private static Logger LOG = LoggerFactory.getLogger(ThroughputBolt.class);

  private long noOfMessages = 0;
  private List<Long> messageSizes = new ArrayList<Long>();
  private boolean latencyProcess;
  private int currentSendIndex = 0;
  private volatile ThroughputState throughputState = ThroughputState.WAIT_SEND;
  private OutputCollector collector;
  private TopologyContext context;

  private enum ThroughputState {
    STARTED_SENDING,
    WAIT_READY,
    WAIT_SEND,
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declareStream(Constants.Fields.CHAIN_STREAM, new Fields(
        Constants.Fields.BODY,
        Constants.Fields.SENSOR_ID_FIELD,
        Constants.Fields.TIME_FIELD));
  }

  @Override
  public void prepare(Map stormConf, TopologyContext topologyContext, OutputCollector outputCollector) {
    boolean throughput = (boolean) stormConf.get(Constants.ARGS_THRPUT);
    latencyProcess = !throughput;

    noOfMessages = (Long) stormConf.get(Constants.ARGS_THRPUT_NO_MSGS);
    messageSizes = (List<Long>) stormConf.get(Constants.ARGS_THRPUT_SIZES);
    this.collector = outputCollector;
    this.context = topologyContext;

    if (!latencyProcess) {
      Thread t = new Thread(new MessageGenerator());
      t.start();
    }
  }

  @Override
  public void execute(Tuple tuple) {
    processThroughput(tuple);
  }

  private void processThroughput(Tuple tuple) {
    String stream = tuple.getSourceStreamId();
    // ready message indicating next process
    if (stream.equals(Constants.Fields.READY_STREAM)) {
      // LOG.info("Received READY, waiting for message");
      if (throughputState == ThroughputState.WAIT_READY) {
        LOG.info("Change state to WAIT_SEND");
        throughputState = ThroughputState.WAIT_SEND;
        currentSendIndex++;
      }
    }
  }

  private class MessageGenerator implements Runnable {
    @Override
    public void run() {
      // initial wait
      try {
        Thread.sleep(10000);
      } catch (InterruptedException ignore) {
      }
      // go through the message sizes list
      // for each message send some messages out
      while (currentSendIndex < messageSizes.size()) {
        if (throughputState == ThroughputState.WAIT_SEND) {
          throughputState = ThroughputState.STARTED_SENDING;
          Long e = messageSizes.get(currentSendIndex);
          byte []data = Utils.generateData(e.intValue());
          LOG.info("Change state to STARTED_SENDING");
          LOG.info("Size: " + e);
          for (int i = 0; i < noOfMessages; i++) {
            List<Object> list = new ArrayList<Object>();
            long time = System.nanoTime();
            list.add(data);
            list.add(e);
            list.add(time);
            collector.emit(Constants.Fields.BROADCAST_STREAM, list);
          }
          throughputState = ThroughputState.WAIT_READY;
        } else {
          try {
            Thread.sleep(100);
          } catch (InterruptedException igonore) {
          }
        }
      }
    }
  }
}
