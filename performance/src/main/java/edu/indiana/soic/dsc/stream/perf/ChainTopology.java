package edu.indiana.soic.dsc.stream.perf;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import cgl.sensorstream.core.StreamComponents;
import cgl.sensorstream.core.StreamTopologyBuilder;
import cgl.sensorstream.core.rabbitmq.DefaultRabbitMQMessageBuilder;
import com.ss.commons.*;
import com.ss.rabbitmq.ErrorReporter;
import com.ss.rabbitmq.RabbitMQSpout;
import com.ss.rabbitmq.bolt.RabbitMQBolt;
import edu.indiana.soic.dsc.stream.perf.latency.LatencyLastBolt;
import edu.indiana.soic.dsc.stream.perf.latency.LatencySpout;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.immutable.Stream;

import java.util.*;

public class ChainTopology {
  private static Logger LOG = LoggerFactory.getLogger(BroadCastTopology.class);

  public static void main(String[] args) throws Exception {
    TopologyBuilder builder = new TopologyBuilder();

    Options options = new Options();
    options.addOption(Constants.ARGS_NAME, true, "Name of the topology");
    options.addOption(Constants.ARGS_LOCAL, false, "Weather we want run locally");
    options.addOption(Constants.ARGS_PARALLEL, true, "No of parallel nodes");
    options.addOption(Utils.createOption(Constants.ARGS_SPOUT_PARALLEL, true, "No of parallel spout nodes", false));
    options.addOption(Constants.ARGS_SREAM_MGRS, true, "No of stream managers");
    options.addOption(Utils.createOption(Constants.ARGS_MODE, true, "Throughput mode", false));
    options.addOption(Utils.createOption(Constants.ARGS_THRPUT_FILENAME, true, "Throughput file name", false));
    options.addOption(Utils.createOption(Constants.ARGS_THRPUT_NO_EMPTY_MSGS, true, "Throughput empty messages", false));
    options.addOption(Utils.createOption(Constants.ARGS_THRPUT_NO_MSGS, true, "Throughput no of messages", false));
    options.addOption(Utils.createOption(Constants.ARGS_THRPUT_SIZES, true, "Throughput no of messages", false));
    options.addOption(Utils.createOption(Constants.ARGS_SEND_INTERVAL, true, "Send interval of messages", false));
    options.addOption(Utils.createOption(Constants.ARGS_DEBUG, false, "Print debug messages", false));
    options.addOption(Utils.createOption(Constants.ARGS_PRINT_INTERVAL, true, "Print debug messages", false));
    options.addOption(Utils.createOption(Constants.ARGS_MAX_PENDING, true, "Max pending", false));

    CommandLineParser commandLineParser = new BasicParser();
    CommandLine cmd = commandLineParser.parse(options, args);
    String name = cmd.getOptionValue(Constants.ARGS_NAME);
    boolean debug = cmd.hasOption(Constants.ARGS_DEBUG);
    boolean local = cmd.hasOption(Constants.ARGS_LOCAL);
    String pValue = cmd.getOptionValue(Constants.ARGS_PARALLEL);
    int p = Integer.parseInt(pValue);
    String mode = cmd.getOptionValue(Constants.ARGS_MODE);
    int streamManagers = Integer.parseInt(cmd.getOptionValue(Constants.ARGS_SREAM_MGRS));
    int interval = 1;
    int maxPending = 1000;
    if (cmd.hasOption(Constants.ARGS_MAX_PENDING)) {
      maxPending = Integer.parseInt(cmd.getOptionValue(Constants.ARGS_MAX_PENDING));
    }

    if (cmd.hasOption(Constants.ARGS_PRINT_INTERVAL)) {
      interval = Integer.parseInt(cmd.getOptionValue(Constants.ARGS_PRINT_INTERVAL));
    }

    int spoutParallel = 1;
    if (cmd.hasOption(Constants.ARGS_SPOUT_PARALLEL)) {
      spoutParallel = Integer.parseInt(cmd.getOptionValue(Constants.ARGS_SPOUT_PARALLEL));
    }

    Config conf = new Config();
    conf.setDebug(false);
    conf.put(Constants.ARGS_DEBUG, debug);
    conf.put(Constants.ARGS_MODE, mode);
    conf.put(Constants.ARGS_PARALLEL, p);
    conf.put(Constants.ARGS_SPOUT_PARALLEL, spoutParallel);
    conf.put(Constants.ARGS_PRINT_INTERVAL, interval);
    conf.put(Constants.ARGS_MAX_PENDING, maxPending);
    conf.setMaxSpoutPending(maxPending);

    StreamTopologyBuilder streamTopologyBuilder;
    streamTopologyBuilder = new StreamTopologyBuilder();
    if (mode.equals("t")) {
      // we are not going to track individual messages, message loss is inherent in the decoder
      // also we cannot replay message because of the decoder
      // conf.put(Config.TOPOLOGY_ACKER_EXECUTORS, 0);
      // conf.put(com.twitter.heron.api.Config.TOPOLOGY_ENABLE_ACKING, true);
      conf.setEnableAcking(true);
      //cconf.setMaxSpoutPending(1000);

      String throughputFile = cmd.getOptionValue(Constants.ARGS_THRPUT_FILENAME);
      String noEmptyMessages = cmd.getOptionValue(Constants.ARGS_THRPUT_NO_EMPTY_MSGS);
      String noMessages = cmd.getOptionValue(Constants.ARGS_THRPUT_NO_MSGS);
      String msgSizesValues = cmd.getOptionValue(Constants.ARGS_THRPUT_SIZES);
      List<Integer> msgSizes = new ArrayList<>();
      String []split = msgSizesValues.split(",");
      for (String s : split) {
        msgSizes.add(Integer.parseInt(s));
      }
      conf.put(Constants.ARGS_THRPUT_NO_MSGS, Integer.parseInt(noMessages));
      conf.put(Constants.ARGS_THRPUT_NO_EMPTY_MSGS, Integer.parseInt(noEmptyMessages));
      conf.put(Constants.ARGS_THRPUT_FILENAME, throughputFile);
      conf.put(Constants.ARGS_THRPUT_SIZES, msgSizes);
      buildThroughputTopology2(builder, p, conf);
    } else if (mode.equals("ta")) {
      // we are not going to track individual messages, message loss is inherent in the decoder
      // also we cannot replay message because of the decoder
      // conf.put(Config.TOPOLOGY_ACKER_EXECUTORS, 0);
      // conf.put(com.twitter.heron.api.Config.TOPOLOGY_ENABLE_ACKING, true);
      conf.setEnableAcking(true);
      //cconf.setMaxSpoutPending(1000);

      String throughputFile = cmd.getOptionValue(Constants.ARGS_THRPUT_FILENAME);
      String noEmptyMessages = cmd.getOptionValue(Constants.ARGS_THRPUT_NO_EMPTY_MSGS);
      String noMessages = cmd.getOptionValue(Constants.ARGS_THRPUT_NO_MSGS);
      String msgSizesValues = cmd.getOptionValue(Constants.ARGS_THRPUT_SIZES);
      List<Integer> msgSizes = new ArrayList<>();
      String []split = msgSizesValues.split(",");
      for (String s : split) {
        msgSizes.add(Integer.parseInt(s));
      }
      conf.put(Constants.ARGS_THRPUT_NO_MSGS, Integer.parseInt(noMessages));
      conf.put(Constants.ARGS_THRPUT_NO_EMPTY_MSGS, Integer.parseInt(noEmptyMessages));
      conf.put(Constants.ARGS_THRPUT_FILENAME, throughputFile);
      conf.put(Constants.ARGS_THRPUT_SIZES, msgSizes);
      buildThroughputTopologyAck(builder, p, conf, spoutParallel);
    } else if (mode.equals("la")) {
      // we are not going to track individual messages, message loss is inherent in the decoder
      // also we cannot replay message because of the decoder
      // conf.put(Config.TOPOLOGY_ACKER_EXECUTORS, 0);
      // conf.put(com.twitter.heron.api.Config.TOPOLOGY_ENABLE_ACKING, true);
      conf.setEnableAcking(true);
      //cconf.setMaxSpoutPending(1000);

      String throughputFile = cmd.getOptionValue(Constants.ARGS_THRPUT_FILENAME);
      String noEmptyMessages = cmd.getOptionValue(Constants.ARGS_THRPUT_NO_EMPTY_MSGS);
      String noMessages = cmd.getOptionValue(Constants.ARGS_THRPUT_NO_MSGS);
      String msgSizesValues = cmd.getOptionValue(Constants.ARGS_THRPUT_SIZES);
      List<Integer> msgSizes = new ArrayList<>();
      String []split = msgSizesValues.split(",");
      for (String s : split) {
        msgSizes.add(Integer.parseInt(s));
      }
      conf.put(Constants.ARGS_THRPUT_NO_MSGS, Integer.parseInt(noMessages));
      conf.put(Constants.ARGS_THRPUT_NO_EMPTY_MSGS, Integer.parseInt(noEmptyMessages));
      conf.put(Constants.ARGS_THRPUT_FILENAME, throughputFile);
      conf.put(Constants.ARGS_THRPUT_SIZES, msgSizes);
      buildLatencyTopologyAck(builder, p, conf, spoutParallel);
    } else if (mode.equals("tna")) {
      // we are not going to track individual messages, message loss is inherent in the decoder
      // also we cannot replay message because of the decoder
      // conf.put(Config.TOPOLOGY_ACKER_EXECUTORS, 0);
      // conf.put(com.twitter.heron.api.Config.TOPOLOGY_ENABLE_ACKING, true);
      conf.setEnableAcking(false);
      //cconf.setMaxSpoutPending(1000);

      String throughputFile = cmd.getOptionValue(Constants.ARGS_THRPUT_FILENAME);
      String noEmptyMessages = cmd.getOptionValue(Constants.ARGS_THRPUT_NO_EMPTY_MSGS);
      String noMessages = cmd.getOptionValue(Constants.ARGS_THRPUT_NO_MSGS);
      String msgSizesValues = cmd.getOptionValue(Constants.ARGS_THRPUT_SIZES);
      List<Integer> msgSizes = new ArrayList<>();
      String []split = msgSizesValues.split(",");
      for (String s : split) {
        msgSizes.add(Integer.parseInt(s));
      }
      conf.put(Constants.ARGS_THRPUT_NO_MSGS, Integer.parseInt(noMessages));
      conf.put(Constants.ARGS_THRPUT_NO_EMPTY_MSGS, Integer.parseInt(noEmptyMessages));
      conf.put(Constants.ARGS_THRPUT_FILENAME, throughputFile);
      conf.put(Constants.ARGS_THRPUT_SIZES, msgSizes);
      buildThroughputTopologyNonAck(builder, p, conf);
    } else if (mode.equals("l")){
      conf.put(com.twitter.heron.api.Config.TOPOLOGY_ENABLE_ACKING, true);
      String throughputFile = cmd.getOptionValue(Constants.ARGS_THRPUT_FILENAME);
      String noEmptyMessages = cmd.getOptionValue(Constants.ARGS_THRPUT_NO_EMPTY_MSGS);
      String noMessages = cmd.getOptionValue(Constants.ARGS_THRPUT_NO_MSGS);
      String msgSizesValues = cmd.getOptionValue(Constants.ARGS_THRPUT_SIZES);
      String sendInterval = cmd.getOptionValue(Constants.ARGS_SEND_INTERVAL);
      List<Integer> msgSizes = new ArrayList<>();
      String []split = msgSizesValues.split(",");
      for (String s : split) {
        msgSizes.add(Integer.parseInt(s));
      }
      conf.put(Constants.ARGS_THRPUT_NO_MSGS, Integer.parseInt(noMessages));
      conf.put(Constants.ARGS_THRPUT_NO_EMPTY_MSGS, Integer.parseInt(noEmptyMessages));
      conf.put(Constants.ARGS_THRPUT_FILENAME, throughputFile);
      conf.put(Constants.ARGS_THRPUT_SIZES, msgSizes);
      if (sendInterval != null) {
        // convert millis to nano
        conf.put(Constants.ARGS_SEND_INTERVAL, Long.parseLong(sendInterval) * 1000000);
      } else {
        conf.put(Constants.ARGS_SEND_INTERVAL, 0);
      }

      buildLatencyTopology(builder, streamTopologyBuilder, p);
    } else if (mode.equals("lf")) {
      conf.put(Config.TOPOLOGY_ACKER_EXECUTORS, 0);
      conf.put(com.twitter.heron.api.Config.TOPOLOGY_ENABLE_ACKING, false);
      buildLatencyFixedRateTopology(builder, streamTopologyBuilder, p, conf);
    }

    // put the no of parallel tasks as a config property
    if (cmd.hasOption(Constants.ARGS_PARALLEL)) {
      conf.put(Constants.ARGS_PARALLEL, p);
    }

    // add the serializers
    addSerializers(conf);

    // we are going to deploy on a real cluster
    if (!local) {
      Properties props = System.getProperties();
      props.list(System.out);
      conf.setNumStmgrs(streamManagers);
      StormSubmitter.submitTopology(name, conf, builder.createTopology());
    } else {
      // deploy on a local cluster
      conf.setMaxTaskParallelism(120);
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("test", conf, builder.createTopology());
      Thread.sleep(120000);
      cluster.shutdown();
    }
  }

  private static void buildLatencyFixedRateTopology(TopologyBuilder builder, StreamTopologyBuilder streamTopologyBuilder,
                                                    int parallel, Config conf) {
    // first create a rabbitmq Spout
    ErrorReporter reporter = new ErrorReporter() {
      @Override
      public void reportError(Throwable throwable) {
        LOG.error("error occured", throwable);
      }
    };
    IRichSpout dataSpout;
    IRichBolt valueSendBolt;

    dataSpout = new RabbitMQSpout(new RabbitMQStaticSpoutConfigurator(0), reporter);
    valueSendBolt = new RabbitMQBolt(new RabbitMQStaticBoltConfigurator(2), reporter);
    // set the first spout
    builder.setSpout(Constants.Topology.RECEIVE_SPOUT, dataSpout, 1);
    conf.setComponentRam(Constants.Topology.RECEIVE_SPOUT, 4L * 1024 * 1024 * 1024);

    PassThroughBolt previousChainBolt = new PassThroughBolt();
    previousChainBolt.setFirst(true);
    builder.setBolt(Constants.Topology.CHAIN_BOLT + "_0", previousChainBolt, 1).
        shuffleGrouping(Constants.Topology.RECEIVE_SPOUT);
    conf.setComponentRam(Constants.Topology.CHAIN_BOLT + "_0", 4L * 1024 * 1024 * 1024);
    for (int i = 1; i < parallel; i++) {
      PassThroughBolt chainBolt = new PassThroughBolt();
      builder.setBolt(Constants.Topology.CHAIN_BOLT + "_" + i, chainBolt, 1).
          shuffleGrouping(Constants.Topology.CHAIN_BOLT + "_" + (i - 1), Constants.Fields.CHAIN_STREAM);
      conf.setComponentRam(Constants.Topology.CHAIN_BOLT + "_" + i, 4L * 1024 * 1024 * 1024);
      previousChainBolt = chainBolt;
    }
    previousChainBolt.setLast(true);
    builder.setBolt(Constants.Topology.RESULT_SEND_BOLT, valueSendBolt, 1).
        shuffleGrouping(Constants.Topology.CHAIN_BOLT + "_" + (parallel - 1), Constants.Fields.CHAIN_STREAM);
    conf.setComponentRam(Constants.Topology.CHAIN_BOLT + "_" + (parallel - 1), 4L * 1024 * 1024 * 1024);
  }

  private static void buildLatencyTopology(TopologyBuilder builder, StreamTopologyBuilder streamTopologyBuilder,
                                             int parallel) {
    IRichSpout dataSpout;
    IRichBolt lastBolt;

    dataSpout = new LatencySpout();
    lastBolt = new LatencyLastBolt();
    // set the first spout
    builder.setSpout(Constants.Topology.RECEIVE_SPOUT, dataSpout, 1);

    ThroughputPassthroughBolt previousChainBolt = new ThroughputPassthroughBolt();
    builder.setBolt(Constants.Topology.CHAIN_BOLT + "_0", previousChainBolt, 1).
        shuffleGrouping(Constants.Topology.RECEIVE_SPOUT, Constants.Fields.CHAIN_STREAM);

    for (int i = 1; i < parallel; i++) {
      ThroughputPassthroughBolt chainBolt = new ThroughputPassthroughBolt();
      builder.setBolt(Constants.Topology.CHAIN_BOLT + "_" + i, chainBolt, 1).
          shuffleGrouping(Constants.Topology.CHAIN_BOLT + "_" + (i - 1), Constants.Fields.CHAIN_STREAM);
    }

    builder.setBolt(Constants.Topology.RESULT_SEND_BOLT, lastBolt, 1).
        shuffleGrouping(Constants.Topology.CHAIN_BOLT + "_" + (parallel - 1), Constants.Fields.CHAIN_STREAM);
  }

  private static void buildThroughputTopologyNonAck(TopologyBuilder builder, int stages, Config conf) {
    ThroughputSpout spout = new ThroughputSpout();
    ThroughputLastBolt lastBolt = new ThroughputLastBolt();
    builder.setSpout(Constants.ThroughputTopology.THROUGHPUT_SPOUT, spout);
    conf.setComponentRam(Constants.ThroughputTopology.THROUGHPUT_SPOUT, 4L * 1024 * 1024 * 1024);

    builder.setBolt(Constants.ThroughputTopology.THROUGHPUT_LAST, lastBolt, stages).
        shuffleGrouping(Constants.ThroughputTopology.THROUGHPUT_SPOUT,
            Constants.Fields.CHAIN_STREAM);
    conf.setComponentRam(Constants.ThroughputTopology.THROUGHPUT_LAST, 4L * 1024 * 1024 * 1024);
  }

  private static void buildThroughputTopologyAck(TopologyBuilder builder, int stages, Config conf, int spoutParallel) {
    ThroughputAckSpout spout = new ThroughputAckSpout();
    ThroughputLastBolt lastBolt = new ThroughputLastBolt();
    builder.setSpout(Constants.ThroughputTopology.THROUGHPUT_SPOUT, spout, spoutParallel);
    conf.setComponentRam(Constants.ThroughputTopology.THROUGHPUT_SPOUT, 4L * 1024 * 1024 * 1024);

    builder.setBolt(Constants.ThroughputTopology.THROUGHPUT_LAST, lastBolt, stages).
        shuffleGrouping(Constants.ThroughputTopology.THROUGHPUT_SPOUT,
            Constants.Fields.CHAIN_STREAM);
    conf.setComponentRam(Constants.ThroughputTopology.THROUGHPUT_LAST, 4L * 1024 * 1024 * 1024);
  }

  private static void buildLatencyTopologyAck(TopologyBuilder builder, int stages, Config conf, int parallel) {
    ThroughputAckSpout spout = new ThroughputAckSpout();
    ThroughputLastBolt lastBolt = new ThroughputLastBolt();
    builder.setSpout(Constants.ThroughputTopology.THROUGHPUT_SPOUT, spout, parallel);
    conf.setComponentRam(Constants.ThroughputTopology.THROUGHPUT_SPOUT, 4L * 1024 * 1024 * 1024);

    ThroughputPassthroughBolt previousBolt = new ThroughputPassthroughBolt();
    builder.setBolt(Constants.ThroughputTopology.THROUGHPUT_PASS_THROUGH + "_0", previousBolt).
        shuffleGrouping(Constants.ThroughputTopology.THROUGHPUT_SPOUT, Constants.Fields.CHAIN_STREAM);
    conf.setComponentRam(Constants.ThroughputTopology.THROUGHPUT_PASS_THROUGH + "_0", 4L * 1024 * 1024 * 1024);

    for (int i = 1; i < stages; i++) {
      ThroughputPassthroughBolt bolt = new ThroughputPassthroughBolt();
      builder.setBolt(Constants.ThroughputTopology.THROUGHPUT_PASS_THROUGH + "_" + i, bolt, parallel).
          shuffleGrouping(Constants.ThroughputTopology.THROUGHPUT_PASS_THROUGH + "_" + (i - 1),
              Constants.Fields.CHAIN_STREAM);
      conf.setComponentRam(Constants.ThroughputTopology.THROUGHPUT_PASS_THROUGH + "_" + i, 4L * 1024 * 1024 * 1024);
    }

    builder.setBolt(Constants.ThroughputTopology.THROUGHPUT_LAST, lastBolt).
        shuffleGrouping(Constants.ThroughputTopology.THROUGHPUT_PASS_THROUGH + "_" + (stages - 1),
            Constants.Fields.CHAIN_STREAM);

    conf.setComponentRam(Constants.ThroughputTopology.THROUGHPUT_LAST, 4L * 1024 * 1024 * 1024);
  }

  private static void buildThroughputTopology2(TopologyBuilder builder, int stages, Config conf) {
    ThroughputSpout spout = new ThroughputSpout();
    ThroughputLastBolt lastBolt = new ThroughputLastBolt();
    builder.setSpout(Constants.ThroughputTopology.THROUGHPUT_SPOUT, spout);
    conf.setComponentRam(Constants.ThroughputTopology.THROUGHPUT_SPOUT, 4L * 1024 * 1024 * 1024);

    if (stages > 0) {
      ThroughputPassthroughBolt previousBolt = new ThroughputPassthroughBolt();
      builder.setBolt(Constants.ThroughputTopology.THROUGHPUT_PASS_THROUGH + "_0", previousBolt).
          shuffleGrouping(Constants.ThroughputTopology.THROUGHPUT_SPOUT, Constants.Fields.CHAIN_STREAM);
      conf.setComponentRam(Constants.ThroughputTopology.THROUGHPUT_PASS_THROUGH + "_0", 4L * 1024 * 1024 * 1024);

      for (int i = 1; i < stages; i++) {
        ThroughputPassthroughBolt bolt = new ThroughputPassthroughBolt();
        builder.setBolt(Constants.ThroughputTopology.THROUGHPUT_PASS_THROUGH + "_" + i, bolt).
            shuffleGrouping(Constants.ThroughputTopology.THROUGHPUT_PASS_THROUGH + "_" + (i - 1),
                Constants.Fields.CHAIN_STREAM);
        conf.setComponentRam(Constants.ThroughputTopology.THROUGHPUT_PASS_THROUGH + "_" + i, 4L * 1024 * 1024 * 1024);
      }

      builder.setBolt(Constants.ThroughputTopology.THROUGHPUT_LAST, lastBolt).
          shuffleGrouping(Constants.ThroughputTopology.THROUGHPUT_PASS_THROUGH + "_" + (stages - 1),
              Constants.Fields.CHAIN_STREAM);

      conf.setComponentRam(Constants.ThroughputTopology.THROUGHPUT_LAST, 4L * 1024 * 1024 * 1024);
    } else {
      builder.setBolt(Constants.ThroughputTopology.THROUGHPUT_LAST, lastBolt).
          shuffleGrouping(Constants.ThroughputTopology.THROUGHPUT_SPOUT,
              Constants.Fields.CHAIN_STREAM);
      conf.setComponentRam(Constants.ThroughputTopology.THROUGHPUT_LAST, 4L * 1024 * 1024 * 1024);
    }
  }

  private static void buildThroughputTopology(TopologyBuilder builder, int stages, Config conf) {
    ThroughputSpout spout = new ThroughputSpout();
    ThroughputLastBolt lastBolt = new ThroughputLastBolt();

    ThroughputPassthroughBolt previousBolt = new ThroughputPassthroughBolt();
    builder.setSpout(Constants.ThroughputTopology.THROUGHPUT_SPOUT, spout);
    conf.setComponentRam(Constants.ThroughputTopology.THROUGHPUT_SPOUT, 4L * 1024 * 1024 * 1024);

    builder.setBolt(Constants.ThroughputTopology.THROUGHPUT_PASS_THROUGH + "_0", previousBolt).
        shuffleGrouping(Constants.ThroughputTopology.THROUGHPUT_SPOUT, Constants.Fields.CHAIN_STREAM);
    conf.setComponentRam(Constants.ThroughputTopology.THROUGHPUT_PASS_THROUGH + "_0", 4L * 1024 * 1024 * 1024);

    for (int i = 1; i < stages; i++) {
      ThroughputPassthroughBolt bolt = new ThroughputPassthroughBolt();
      builder.setBolt(Constants.ThroughputTopology.THROUGHPUT_PASS_THROUGH + "_" + i, bolt).
          shuffleGrouping(Constants.ThroughputTopology.THROUGHPUT_PASS_THROUGH + "_" + (i - 1),
              Constants.Fields.CHAIN_STREAM);
      conf.setComponentRam(Constants.ThroughputTopology.THROUGHPUT_PASS_THROUGH + "_" + i, 4L * 1024 * 1024 * 1024);
    }

    builder.setBolt(Constants.ThroughputTopology.THROUGHPUT_LAST, lastBolt).
        shuffleGrouping(Constants.ThroughputTopology.THROUGHPUT_PASS_THROUGH + "_" + (stages - 1),
            Constants.Fields.CHAIN_STREAM);
    conf.setComponentRam(Constants.ThroughputTopology.THROUGHPUT_LAST, 4L * 1024 * 1024 * 1024);
  }

  private static void addSerializers(Config config) {
    config.registerSerialization(BTrace.class);
  }

  private static class RabbitMQStaticBoltConfigurator implements BoltConfigurator {

    int bolt;

    private RabbitMQStaticBoltConfigurator(int bolt) {
      this.bolt = bolt;
    }

    @Override
    public MessageBuilder getMessageBuilder() {
      return new DefaultRabbitMQMessageBuilder();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }

    @Override
    public int queueSize() {
      return 64;
    }

    @Override
    public Map<String, String> getProperties() {
      return new HashMap<String, String>();
    }

    @Override
    public DestinationSelector getDestinationSelector() {
      return new DestinationSelector() {
        @Override
        public String select(Tuple tuple) {
          return "test";
        }
      };
    }

    @Override
    public DestinationChanger getDestinationChanger() {
      return new StaticDestinations(bolt);
    }
  }

  private static class RabbitMQStaticSpoutConfigurator implements SpoutConfigurator {
    int spout;

    private RabbitMQStaticSpoutConfigurator(int spout) {
      this.spout = spout;
    }

    @Override
    public MessageBuilder getMessageBuilder() {
      return new DefaultRabbitMQMessageBuilder();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
      if (spout == 0) {
        outputFieldsDeclarer.declare(new Fields(Constants.Fields.BODY,
            Constants.Fields.SENSOR_ID_FIELD, Constants.Fields.TIME_FIELD));
      } else {
        outputFieldsDeclarer.declareStream(Constants.Fields.CONTROL_STREAM,
            new Fields(Constants.Fields.BODY,
                Constants.Fields.SENSOR_ID_FIELD, Constants.Fields.TIME_FIELD));
      }
    }

    @Override
    public int queueSize() {
      return 64;
    }

    @Override
    public Map<String, String> getProperties() {
      return new HashMap<String, String>();
    }

    @Override
    public DestinationChanger getDestinationChanger() {
      return new StaticDestinations(spout);
    }

    @Override
    public String getStream() {
      if (spout == 0) {
        return null;
      } else {
        return Constants.Fields.CONTROL_STREAM;
      }
    }
  }

  private static class StaticDestinations implements DestinationChanger {
    private DestinationChangeListener dstListener;

    private int sender;

    private StaticDestinations(int sender) {
      this.sender = sender;
    }

    @Override
    public void start() {
      StreamTopologyBuilder streamTopologyBuilder = new StreamTopologyBuilder();
      StreamComponents components = streamTopologyBuilder.buildComponents();
      Map conf = components.getConf();
      String url = (String) conf.get(Constants.RABBITMQ_URL);
      DestinationConfiguration configuration = new DestinationConfiguration("rabbitmq", url, "test", "test");
      configuration.setGrouped(true);
      if (sender == 0) {
        configuration.addProperty("queueName", "laser_scan");
        configuration.addProperty("routingKey", "laser_scan");
        configuration.addProperty("exchange", "simbard_laser");
      } else if (sender == 1){
        configuration.addProperty("queueName", "map");
        configuration.addProperty("routingKey", "map");
        configuration.addProperty("exchange", "simbard_map");
      } else if (sender == 2) {
        configuration.addProperty("queueName", "best");
        configuration.addProperty("routingKey", "best");
        configuration.addProperty("exchange", "simbard_best");
      } else if (sender == 3) {
        configuration.addProperty("queueName", "control");
        configuration.addProperty("routingKey", "control");
        configuration.addProperty("exchange", "simbard_control");
      }

      dstListener.addDestination("rabbitmq", configuration);
      dstListener.addPathToDestination("rabbitmq", "test");
    }

    @Override
    public void stop() {
      dstListener.removeDestination("rabbitmq");
    }

    @Override
    public void registerListener(DestinationChangeListener destinationChangeListener) {
      this.dstListener = destinationChangeListener;
    }

    @Override
    public void setTask(int i, int i2) {

    }

    @Override
    public int getTaskIndex() {
      return 0;
    }

    @Override
    public int getTotalTasks() {
      return 0;
    }
  }
}
