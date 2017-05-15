package edu.indiana.soic.dsc.stream.collectives;

import com.twitter.heron.api.bolt.BaseRichBolt;
import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Tuple;
import edu.indiana.soic.dsc.stream.perf.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;

public class CollectiveLastBolt extends BaseRichBolt {
  private static Logger LOG = LoggerFactory.getLogger(CollectiveLastBolt.class);
  private int noOfMessages = 0;
  private int noOfEmptyMessages = 0;
  private String fileName;
  private long firstThroughputRecvTime = 0;
  private ReceiveType receiveState = ReceiveType.EMPTY;
  private OutputCollector outputCollector;
  private int count = 0;
  private boolean save = true;
  private int messageCount = 0;
  private boolean debug = false;
  private int printInveral = 0;
  private TopologyContext context;

  private enum ReceiveType {
    DATA,
    EMPTY
  }

  @Override
  public void prepare(Map stormConf, TopologyContext topologyContext, OutputCollector outputCollector) {
    noOfMessages = (Integer) stormConf.get(Constants.ARGS_THRPUT_NO_MSGS);
    noOfEmptyMessages = (Integer) stormConf.get(Constants.ARGS_THRPUT_NO_EMPTY_MSGS);
    fileName = (String) stormConf.get(Constants.ARGS_THRPUT_FILENAME);
    String mode = (String) stormConf.get(Constants.ARGS_MODE);
    save = !mode.equals("ta") && !mode.equals("la");
    int parallel = (int) stormConf.get(Constants.ARGS_PARALLEL);
    if (!mode.equals("t")) {
      noOfMessages = noOfMessages / parallel;
      noOfEmptyMessages = noOfEmptyMessages / parallel;
    }
    this.debug = (boolean) stormConf.get(Constants.ARGS_DEBUG);
    this.printInveral = (int) stormConf.get(Constants.ARGS_PRINT_INTERVAL);

    this.outputCollector = outputCollector;
    this.context = topologyContext;
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
      String stream = tuple.getSourceStreamId();
      outputCollector.ack(tuple);

      Integer size = tuple.getIntegerByField(Constants.Fields.MESSAGE_SIZE_FIELD);
      if (debug && messageCount % printInveral == 0) {
        LOG.info(context.getThisTaskId() + " Last Received tuple: " + count);
      }
      count++;
      // Integer messageCount = tuple.getIntegerByField(Constants.Fields.MESSAGE_INDEX_FIELD);
      messageCount++;
      if (receiveState == ReceiveType.EMPTY) {
        // LOG.info("Empty receive: " + messageCount);
        if (messageCount == noOfEmptyMessages) {
          receiveState = ReceiveType.DATA;
          firstThroughputRecvTime = System.nanoTime();
          messageCount = 0;
        }
      } else if (receiveState == ReceiveType.DATA) {
        // LOG.info("Data receive: " + messageCount);
        if (messageCount == noOfMessages) {
          receiveState = ReceiveType.EMPTY;
          long time = System.nanoTime() - firstThroughputRecvTime;
          firstThroughputRecvTime = 0;
          messageCount = 0;
          if (save) {
            System.out.println("Write file for size: " + size);
            String currentOutPut = size + " " + noOfMessages + " " + time + " " + (messageCount + 0.0) / (time / 1000000000.0);
            writeFile(currentOutPut);
          }
        }
      }
      // LOG.info("Count: " + count++);
    } catch (Throwable t) {
      t.printStackTrace();
    }
  }

  private void writeFile(String line) {
    try(FileWriter fw = new FileWriter(fileName, true);
        BufferedWriter bw = new BufferedWriter(fw);
        PrintWriter out = new PrintWriter(bw)) {
      out.println(line);
    } catch (IOException e) {
      //exception handling left as an exercise for the reader
      LOG.error("Failed to write to the file", e);
    }
  }
}
