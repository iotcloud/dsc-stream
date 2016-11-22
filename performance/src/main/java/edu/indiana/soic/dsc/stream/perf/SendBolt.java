package edu.indiana.soic.dsc.stream.perf;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.esotericsoftware.kryo.Kryo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SendBolt extends BaseRichBolt {
  private static Logger LOG = LoggerFactory.getLogger(SendBolt.class);
  private TopologyContext context;
  private OutputCollector collector;
  private Kryo kryo;
  private boolean latencyProcess;
  private long noOfMessages = 0;
  private int count = 0;
  private String fileName;
  private long firstThroughputRecvTime = 0;
  private String currentOutPut;
  private List<Long> messageSizes = new ArrayList<Long>();


  @Override
  public void prepare(Map stormConf, TopologyContext topologyContext, OutputCollector outputCollector) {
    this.context = topologyContext;
    this.collector = outputCollector;
    this.kryo = new Kryo();
    Utils.registerClasses(kryo);

    boolean throughput = (boolean) stormConf.get(Constants.ARGS_THRPUT);
    latencyProcess = !throughput;
    noOfMessages = (Long) stormConf.get(Constants.ARGS_THRPUT_NO_MSGS);
    messageSizes = (List<Long>) stormConf.get(Constants.ARGS_THRPUT_SIZES);
    fileName = (String) stormConf.get(Constants.ARGS_THRPUT_FILENAME);
  }

  @Override
  public void execute(Tuple tuple) {
    Object time = tuple.getValueByField(Constants.Fields.TIME_FIELD);
    Object sensorId = tuple.getValueByField(Constants.Fields.SENSOR_ID_FIELD);

    List<Object> list = new ArrayList<Object>();
    SingleTrace singleTrace = new SingleTrace();
    byte []b = Utils.serialize(kryo, singleTrace);
    list.add(b);
    list.add(sensorId);
    list.add(time);
    collector.emit(Constants.Fields.CHAIN_STREAM, list);
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

    Long messageId = tuple.getLongByField(Constants.Fields.MESSAGE_ID_FIELD);
    Long size = tuple.getLongByField(Constants.Fields.TRACE_FIELD);
    // wait until we get all the inputs
    processThroughputInput(tuple, messageId + "");
    if (count == 0) {
      firstThroughputRecvTime = System.nanoTime();
    }
    count++;
    if (count == noOfMessages) {
      LOG.info("Finished " + size);
      // reset the counter and ready for the next set of messages
      // write the current to file
      long time = System.nanoTime() - firstThroughputRecvTime;
      currentOutPut += " " + (count + 0.0)/ (time / 1000000000.0);
      firstThroughputRecvTime = 0;
      count = 0;
      List<Object> list = new ArrayList<Object>();
      list.add("ready");
      collector.emit(Constants.Fields.READY_STREAM, list);

      // check weather this is the last size
      Long integer = messageSizes.get(messageSizes.size() - 1);
      if (size == integer.intValue()) {
        writeFile(currentOutPut);
      }
    }

  }

  private void processThroughputInput(Tuple tuple, String messageId) {
  }

  private void writeFile(String line) {
    PrintWriter writer = null;
    try {
      File f = new File(fileName);
      if (!f.exists()) {
        f.createNewFile();
      }
      writer = new PrintWriter(new FileOutputStream(fileName, true));
      writer.println(line);
      writer.flush();
      writer.close();
    } catch (FileNotFoundException e) {
      LOG.error("Failed to write file", e);
    } catch (IOException e) {
      LOG.error("Failed to write file", e);
    }
  }

}
