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
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class BroadCastTopology {
  private static Logger LOG = LoggerFactory.getLogger(BroadCastTopology.class);

  public static void main(String[] args) throws Exception {
    TopologyBuilder builder = new TopologyBuilder();

    Options options = new Options();
    options.addOption(Constants.ARGS_NAME, true, "Name of the topology");
    options.addOption(Constants.ARGS_LOCAL, false, "Weather we want run locally");
    options.addOption(Constants.ARGS_DS_MODE, true, "The distributed mode, specify 0, 1, 2, 3 etc");
    options.addOption(Constants.ARGS_PARALLEL, true, "No of parallel nodes");
    options.addOption(Constants.ARGS_IOT_CLOUD, false, "Weather we run through IoTCloud");
    options.addOption(Utils.createOption(Constants.ARGS_BINARY_TREE, false, "Binary tree", false));
    options.addOption(Utils.createOption(Constants.ARGS_PIPE_LINE, false, "Pipe line", false));
    options.addOption(Utils.createOption(Constants.ARGS_FLAT_TREE, false, "Flat tree", false));
    options.addOption(Utils.createOption(Constants.ARGS_FLAT_TREE_BRANCH, true, "Flat tree branch", false));
    options.addOption(Utils.createOption(Constants.ARGS_ASYNCHRONOUS, false, "Asynchronous", false));
    options.addOption(Utils.createOption(Constants.ARGS_PIPE_SPLIT, false, "Split pipe", false));

    CommandLineParser commandLineParser = new BasicParser();
    CommandLine cmd = commandLineParser.parse(options, args);
    String name = cmd.getOptionValue(Constants.ARGS_NAME);
    boolean local = cmd.hasOption(Constants.ARGS_LOCAL);
    String dsModeValue = cmd.getOptionValue(Constants.ARGS_DS_MODE);
    int dsMode = Integer.parseInt(dsModeValue);
    String pValue = cmd.getOptionValue(Constants.ARGS_PARALLEL);
    int p = Integer.parseInt(pValue);
    boolean iotCloud = cmd.hasOption(Constants.ARGS_IOT_CLOUD);
    boolean pipeSplit = cmd.hasOption(Constants.ARGS_PIPE_SPLIT);
    boolean asynchronous = cmd.hasOption(Constants.ARGS_ASYNCHRONOUS);

    StreamTopologyBuilder streamTopologyBuilder;
    if (dsMode == 0) {
      streamTopologyBuilder = new StreamTopologyBuilder();
      if (!asynchronous) {
        buildTestTopology(builder, streamTopologyBuilder, p, iotCloud);
      } else {
        buildTestAsyncTopology(builder, streamTopologyBuilder, p, iotCloud);
      }
    }

    Config conf = new Config();
    conf.setDebug(false);
    // we are not going to track individual messages, message loss is inherent in the decoder
    // also we cannot replay message because of the decoder
    conf.put(Config.TOPOLOGY_ACKER_EXECUTORS, 0);

    // put the no of parallel tasks as a config property
    if (cmd.hasOption(Constants.ARGS_PARALLEL)) {
      conf.put(Constants.ARGS_PARALLEL, p);
    }

    // we will not use the constants because we are going to compile agains both modified and un-modified versions
    boolean flatTree = cmd.hasOption(Constants.ARGS_FLAT_TREE);
    if (flatTree) {
      conf.put("topology.collective.use.flaTree", true);
      String branchString = cmd.getOptionValue(Constants.ARGS_FLAT_TREE_BRANCH);
      if (branchString != null) {
        int branch = Integer.parseInt(branchString);
        conf.put("topology.collective.worker.branch", branch);
      } else {
        conf.put("topology.collective.worker.branch", -1);
      }
    }
    boolean pipe = cmd.hasOption(Constants.ARGS_PIPE_LINE);
    if (pipe) {
      conf.put("topology.collective.use.pipeline", true);
      if (pipeSplit) {
        conf.put("topology.collective.use.pipeline.split", true);
      }
    }
    boolean binary = cmd.hasOption(Constants.ARGS_BINARY_TREE);
    if (binary) {
      conf.put("topology.collective.use.binary", true);
    }
    // add the serializers
    addSerializers(conf);

    // we are going to deploy on a real cluster
    if (!local) {
      conf.setNumWorkers(32);
      StormSubmitter.submitTopology(name, conf, builder.createTopology());
    } else {
      // deploy on a local cluster
      conf.setMaxTaskParallelism(120);
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("drone", conf, builder.createTopology());
      Thread.sleep(1000000);
      cluster.shutdown();
    }
  }

  private static void buildTestTopology(TopologyBuilder builder, StreamTopologyBuilder streamTopologyBuilder,
                                        int parallel, boolean iotCloud) {
    // first create a rabbitmq Spout
    ErrorReporter reporter = new ErrorReporter() {
      @Override
      public void reportError(Throwable throwable) {
        LOG.error("error occured", throwable);
      }
    };
    IRichSpout dataSpout;
    IRichSpout controlSpout;
    IRichBolt valueSendBolt;
    if (!iotCloud) {
      dataSpout = new RabbitMQSpout(new RabbitMQStaticSpoutConfigurator(0), reporter);
      controlSpout = new RabbitMQSpout(new RabbitMQStaticSpoutConfigurator(3), reporter);
      valueSendBolt = new RabbitMQBolt(new RabbitMQStaticBoltConfigurator(2), reporter);
    } else {
      StreamComponents components = streamTopologyBuilder.buildComponents();
      dataSpout = components.getSpouts().get(Constants.Topology.RECEIVE_SPOUT);
      controlSpout = components.getSpouts().get(Constants.Topology.CONTROL_SPOUT);
      valueSendBolt = components.getBolts().get(Constants.Topology.RESULT_SEND_BOLT);
    }

    WorkerBolt workerBolt = new WorkerBolt();
    GatherBolt gatherBolt = new GatherBolt();
    BroadCastBolt broadCastBolt = new BroadCastBolt();

    builder.setSpout(Constants.Topology.RECEIVE_SPOUT, dataSpout, 1);
    builder.setBolt(Constants.Topology.BROADCAST_BOLT, broadCastBolt, 1).
        shuffleGrouping(Constants.Topology.RECEIVE_SPOUT).
        shuffleGrouping(Constants.Topology.GATHER_BOLT, Constants.Fields.READY_STREAM);
    builder.setBolt(Constants.Topology.WORKER_BOLT, workerBolt, parallel).
        allGrouping(Constants.Topology.BROADCAST_BOLT, Constants.Fields.BROADCAST_STREAM);
    builder.setBolt(Constants.Topology.GATHER_BOLT, gatherBolt, 1).
        shuffleGrouping(Constants.Topology.WORKER_BOLT, Constants.Fields.GATHER_STREAM);
    builder.setBolt(Constants.Topology.RESULT_SEND_BOLT, valueSendBolt, 1).
        shuffleGrouping(Constants.Topology.GATHER_BOLT, Constants.Fields.SEND_STREAM);
  }

  private static void buildTestAsyncTopology(TopologyBuilder builder, StreamTopologyBuilder streamTopologyBuilder,
                                             int parallel, boolean iotCloud) {
    // first create a rabbitmq Spout
    ErrorReporter reporter = new ErrorReporter() {
      @Override
      public void reportError(Throwable throwable) {
        LOG.error("error occured", throwable);
      }
    };
    IRichSpout dataSpout;
    IRichSpout controlSpout;
    IRichBolt valueSendBolt;
    if (!iotCloud) {
      dataSpout = new RabbitMQSpout(new RabbitMQStaticSpoutConfigurator(0), reporter);
      controlSpout = new RabbitMQSpout(new RabbitMQStaticSpoutConfigurator(3), reporter);
      valueSendBolt = new RabbitMQBolt(new RabbitMQStaticBoltConfigurator(2), reporter);
    } else {
      StreamComponents components = streamTopologyBuilder.buildComponents();
      dataSpout = components.getSpouts().get(Constants.Topology.RECEIVE_SPOUT);
      controlSpout = components.getSpouts().get(Constants.Topology.CONTROL_SPOUT);
      valueSendBolt = components.getBolts().get(Constants.Topology.RESULT_SEND_BOLT);
    }

    WorkerBolt workerBolt = new WorkerBolt();
    GatherBolt gatherBolt = new GatherBolt();
    BroadCastBolt broadCastBolt = new BroadCastBolt();
    broadCastBolt.setSynchronous(false);
    gatherBolt.setSynchronous(false);

    builder.setSpout(Constants.Topology.RECEIVE_SPOUT, dataSpout, 1);
    //builder.setSpout(Constants.Topology.CONTROL_SPOUT, controlSpout, 1);
    builder.setBolt(Constants.Topology.BROADCAST_BOLT, broadCastBolt, 1).
        shuffleGrouping(Constants.Topology.RECEIVE_SPOUT).
        shuffleGrouping(Constants.Topology.GATHER_BOLT, Constants.Fields.READY_STREAM);
    builder.setBolt(Constants.Topology.WORKER_BOLT, workerBolt, parallel).
        allGrouping(Constants.Topology.BROADCAST_BOLT, Constants.Fields.BROADCAST_STREAM);
    builder.setBolt(Constants.Topology.GATHER_BOLT, gatherBolt, 1).
        shuffleGrouping(Constants.Topology.WORKER_BOLT, Constants.Fields.GATHER_STREAM);
    builder.setBolt(Constants.Topology.RESULT_SEND_BOLT, valueSendBolt, 1).
        shuffleGrouping(Constants.Topology.GATHER_BOLT, Constants.Fields.SEND_STREAM);
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

