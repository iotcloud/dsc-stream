package edu.indiana.soic.dsc.stream.collectives;

import com.ss.rabbitmq.ErrorReporter;
import com.ss.rabbitmq.RabbitMQSpout;
import com.ss.rabbitmq.bolt.RabbitMQBolt;
import com.twitter.heron.api.Config;
import com.twitter.heron.api.HeronSubmitter;
import com.twitter.heron.api.bolt.IRichBolt;
import com.twitter.heron.api.spout.IRichSpout;
import com.twitter.heron.api.topology.TopologyBuilder;
import com.twitter.heron.common.basics.ByteAmount;
import com.twitter.heron.simulator.Simulator;
import edu.indiana.soic.dsc.stream.collectives.rabbit.RabbitMQStaticBoltConfigurator;
import edu.indiana.soic.dsc.stream.collectives.rabbit.RabbitMQStaticSpoutConfigurator;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class CollectiveTopology {
  private static Logger LOG = LoggerFactory.getLogger(CollectiveTopology.class);
  static int megabytes = 256;
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
    options.addOption(Utils.createOption(Constants.ARGS_RATE, true, "Max pending", false));
    options.addOption(Utils.createOption(Constants.ARGS_URL, true, "URL", false));

    CommandLineParser commandLineParser = new BasicParser();
    CommandLine cmd = commandLineParser.parse(options, args);
    String name = cmd.getOptionValue(Constants.ARGS_NAME);
    boolean debug = cmd.hasOption(Constants.ARGS_DEBUG);
    boolean local = cmd.hasOption(Constants.ARGS_LOCAL);
    String pValue = cmd.getOptionValue(Constants.ARGS_PARALLEL);
    int p = Integer.parseInt(pValue);
    String mode = cmd.getOptionValue(Constants.ARGS_MODE);
    String url = cmd.getOptionValue(Constants.ARGS_URL);

    int streamManagers = Integer.parseInt(cmd.getOptionValue(Constants.ARGS_SREAM_MGRS));
    int interval = 1;
    int maxPending = 10;
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

    int rate = 0;
    if (cmd.hasOption(Constants.ARGS_RATE)) {
      rate = Integer.parseInt(cmd.getOptionValue(Constants.ARGS_RATE));
    }

    Config conf = new Config();
    conf.setDebug(false);
    conf.put(Constants.ARGS_DEBUG, debug);
    conf.put(Constants.ARGS_MODE, mode);
    conf.put(Constants.ARGS_PARALLEL, p);
    conf.put(Constants.ARGS_SPOUT_PARALLEL, spoutParallel);
    conf.put(Constants.ARGS_PRINT_INTERVAL, interval);
    conf.put(Constants.ARGS_MAX_PENDING, maxPending);
    conf.put(Constants.ARGS_RATE, rate);
    conf.put(Constants.ARGS_SREAM_MGRS, streamManagers);

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

    conf.setMaxSpoutPending(maxPending);

    conf.setEnableAcking(true);
    if (mode.equals("c")) {
      buildThroughputTopologyAck(builder, p, conf, spoutParallel);
    } else if (mode.equals("r")) {
      buildThroughputTopologyReductionAck(builder, p, conf, spoutParallel);
    } else if (mode.equals("rc")) {
      conf.setEnableAcking(false);
      buildReduceCollectiveTopology(builder, p, conf, url);
    } else if (mode.equals("rs")) {
      conf.setEnableAcking(false);
      buildReduceSerialTopology(builder, p, conf, url);
    } else if (mode.equals("arc")) {
      conf.setEnableAcking(false);
      buildAllReduceCollectiveTopology(builder, p, conf, url);
    } else if (mode.equals("ars")) {
      conf.setEnableAcking(false);
      buildAllReduceSerialTopology(builder, p, conf, url);
    } else if (mode.equals("b")) {
      conf.setEnableAcking(false);
      buildBroadCastTopology(builder, p, conf, url);
    }

    // put the no of parallel tasks as a config property
    if (cmd.hasOption(Constants.ARGS_PARALLEL)) {
      conf.put(Constants.ARGS_PARALLEL, p);
    }

    // we are going to deploy on a real cluster
    if (!local) {
      Properties props = System.getProperties();
      conf.setNumStmgrs(streamManagers);
      HeronSubmitter.submitTopology(name, conf, builder.createTopology());
    } else {
      try {
        // deploy on a local cluster
        Simulator cluster = new Simulator();
        cluster.submitTopology("test", conf, builder.createTopology());
        Thread.sleep(120000);
        cluster.shutdown();
      } catch (Throwable t) {
        LOG.error("Error", t);
      }
    }
  }

  private static void buildThroughputTopologyAck(TopologyBuilder builder, int firstParallel, Config conf, int secondParallel) {
    CollectiveAckSpout spout = new CollectiveAckSpout();
    SingleDataCollectionBolt lastBolt = new SingleDataCollectionBolt();
    DataGenerationBolt passThroughBolt = new DataGenerationBolt();

    builder.setSpout(Constants.ThroughputTopology.SPOUT, spout, secondParallel);
    conf.setComponentRam(Constants.ThroughputTopology.SPOUT, ByteAmount.fromMegabytes(megabytes));

    builder.setBolt(Constants.ThroughputTopology.DATAGEN, passThroughBolt, firstParallel).shuffleGrouping
        (Constants.ThroughputTopology.SPOUT,
            Constants.Fields.CHAIN_STREAM);

    builder.setBolt(Constants.ThroughputTopology.LAST, lastBolt, 1).reduceGrouping
      (Constants.ThroughputTopology.DATAGEN,
            Constants.Fields.CHAIN_STREAM, new CountReduceFunction());

    conf.setComponentRam(Constants.ThroughputTopology.LAST, ByteAmount.fromMegabytes(megabytes));
  }

  private static void buildThroughputTopologyReductionAck(TopologyBuilder builder, int firstParallel, Config conf, int secondParallel) {
    CollectiveAckSpout spout = new CollectiveAckSpout();
    MultiDataCollectionBolt lastBolt = new MultiDataCollectionBolt();
    DataGenerationBolt passThroughBolt = new DataGenerationBolt();

    builder.setSpout(Constants.ThroughputTopology.SPOUT, spout, secondParallel);
    conf.setComponentRam(Constants.ThroughputTopology.SPOUT, ByteAmount.fromMegabytes(megabytes));

    builder.setBolt(Constants.ThroughputTopology.DATAGEN, passThroughBolt, firstParallel).shuffleGrouping
        (Constants.ThroughputTopology.SPOUT,
            Constants.Fields.CHAIN_STREAM);

    builder.setBolt(Constants.ThroughputTopology.LAST, lastBolt, 1).shuffleGrouping
        (Constants.ThroughputTopology.DATAGEN,
              Constants.Fields.CHAIN_STREAM);
    conf.setComponentRam(Constants.ThroughputTopology.LAST, ByteAmount.fromMegabytes(megabytes));
  }

  private static void buildReduceSerialTopology(TopologyBuilder builder, int firstParallel, Config conf, String url) {
    ErrorReporter reporter = new ErrorReporter() {
      @Override
      public void reportError(Throwable throwable) {
        throwable.printStackTrace();
        LOG.error("error occured", throwable);
      }
    };
    IRichSpout dataSpout;
    IRichBolt valueSendBolt;

    dataSpout = new RabbitMQSpout(new RabbitMQStaticSpoutConfigurator(0, url), reporter);
    valueSendBolt = new RabbitMQBolt(new RabbitMQStaticBoltConfigurator(2, url), reporter);
    OriginBolt originBolt = new OriginBolt();

    MultiDataCollectionBolt lastBolt = new MultiDataCollectionBolt();
    DataGenerationBolt dataGenerationBolt = new DataGenerationBolt();
    conf.put(Constants.UPPER_COMPONENT_NAME, Constants.ThroughputTopology.DATAGEN);

    builder.setSpout(Constants.ThroughputTopology.SPOUT, dataSpout, 1);
    conf.setComponentRam(Constants.ThroughputTopology.SPOUT, ByteAmount.fromMegabytes(megabytes));

    builder.setBolt(Constants.ThroughputTopology.ORIGIN, originBolt, 1).
        shuffleGrouping(Constants.ThroughputTopology.SPOUT);
    conf.setComponentRam(Constants.ThroughputTopology.ORIGIN, ByteAmount.fromMegabytes(megabytes));

    builder.setBolt(Constants.ThroughputTopology.DATAGEN, dataGenerationBolt, firstParallel).allGrouping
        (Constants.ThroughputTopology.ORIGIN,
            Constants.Fields.CHAIN_STREAM);
    conf.setComponentRam(Constants.ThroughputTopology.DATAGEN, ByteAmount.fromMegabytes(megabytes));

    builder.setBolt(Constants.ThroughputTopology.MULTI_DATAGATHER, lastBolt, 1).shuffleGrouping
        (Constants.ThroughputTopology.DATAGEN,
            Constants.Fields.CHAIN_STREAM);
    conf.setComponentRam(Constants.ThroughputTopology.MULTI_DATAGATHER, ByteAmount.fromMegabytes(megabytes));

    builder.setBolt(Constants.ThroughputTopology.SEND, valueSendBolt, 1).shuffleGrouping(
        Constants.ThroughputTopology.MULTI_DATAGATHER, Constants.Fields.CHAIN_STREAM);
    conf.setComponentRam(Constants.ThroughputTopology.SEND, ByteAmount.fromMegabytes(megabytes));
  }

  private static void buildReduceCollectiveTopology(TopologyBuilder builder, int stages, Config conf, String url) {
    ErrorReporter reporter = new ErrorReporter() {
      @Override
      public void reportError(Throwable throwable) {
        throwable.printStackTrace();
        LOG.error("error occured", throwable);
      }
    };
    IRichSpout dataSpout;
    IRichBolt valueSendBolt;

    dataSpout = new RabbitMQSpout(new RabbitMQStaticSpoutConfigurator(0, url), reporter);
    valueSendBolt = new RabbitMQBolt(new RabbitMQStaticBoltConfigurator(2, url), reporter);
    OriginBolt originBolt = new OriginBolt();

    SingleDataCollectionBolt singleDataCollectionBolt = new SingleDataCollectionBolt();
    DataGenerationBolt passThroughBolt = new DataGenerationBolt();

    builder.setSpout(Constants.ThroughputTopology.SPOUT, dataSpout, 1);
    conf.setComponentRam(Constants.ThroughputTopology.SPOUT, ByteAmount.fromMegabytes(megabytes));

    builder.setBolt(Constants.ThroughputTopology.ORIGIN, originBolt, 1).
        shuffleGrouping(Constants.ThroughputTopology.SPOUT);
    conf.setComponentRam(Constants.ThroughputTopology.ORIGIN, ByteAmount.fromMegabytes(megabytes));

    builder.setBolt(Constants.ThroughputTopology.DATAGEN, passThroughBolt, stages).allGrouping
        (Constants.ThroughputTopology.ORIGIN,
            Constants.Fields.CHAIN_STREAM);
    conf.setComponentRam(Constants.ThroughputTopology.DATAGEN, ByteAmount.fromMegabytes(megabytes));

    builder.setBolt(Constants.ThroughputTopology.SINGLE_DATAGATHER, singleDataCollectionBolt, 1).reduceGrouping
        (Constants.ThroughputTopology.DATAGEN,
            Constants.Fields.CHAIN_STREAM, new CountReduceFunction());
    conf.setComponentRam(Constants.ThroughputTopology.SINGLE_DATAGATHER, ByteAmount.fromMegabytes(megabytes));

    builder.setBolt(Constants.ThroughputTopology.SEND, valueSendBolt, 1).shuffleGrouping(
        Constants.ThroughputTopology.SINGLE_DATAGATHER, Constants.Fields.CHAIN_STREAM);
    conf.setComponentRam(Constants.ThroughputTopology.SEND, ByteAmount.fromMegabytes(megabytes));
  }

  private static void buildAllReduceSerialTopology(TopologyBuilder builder, int stages, Config conf, String url) {
    ErrorReporter reporter = new ErrorReporter() {
      @Override
      public void reportError(Throwable throwable) {
        throwable.printStackTrace();
        LOG.error("error occured", throwable);
      }
    };
    IRichSpout dataSpout;
    IRichBolt valueSendBolt;

    dataSpout = new RabbitMQSpout(new RabbitMQStaticSpoutConfigurator(0, url), reporter);
    valueSendBolt = new RabbitMQBolt(new RabbitMQStaticBoltConfigurator(2, url), reporter);
    OriginBolt originBolt = new OriginBolt();
    DataGenerationBolt dataGenerationBolt = new DataGenerationBolt();

    SingleDataCollectionBolt singleDataCollectionBolt = new SingleDataCollectionBolt();
    MultiDataCollectionBolt multiDataCollectionBoltOne = new MultiDataCollectionBolt();
    multiDataCollectionBoltOne.setPassThrough(true);
    MultiDataCollectionBolt multiDataCollectionBoltTwo = new MultiDataCollectionBolt();

    builder.setSpout(Constants.ThroughputTopology.SPOUT, dataSpout, 1);
    conf.setComponentRam(Constants.ThroughputTopology.SPOUT, ByteAmount.fromMegabytes(megabytes));

    builder.setBolt(Constants.ThroughputTopology.ORIGIN, originBolt, 1).
        shuffleGrouping(Constants.ThroughputTopology.SPOUT);
    conf.setComponentRam(Constants.ThroughputTopology.ORIGIN, ByteAmount.fromMegabytes(megabytes));

    builder.setBolt(Constants.ThroughputTopology.DATAGEN, dataGenerationBolt, stages).allGrouping
        (Constants.ThroughputTopology.ORIGIN,
            Constants.Fields.CHAIN_STREAM);
    conf.setComponentRam(Constants.ThroughputTopology.DATAGEN, ByteAmount.fromMegabytes(megabytes));

    String multiOne = Constants.ThroughputTopology.MULTI_DATAGATHER + "_1";
    multiDataCollectionBoltOne.setUpperComponentName(Constants.ThroughputTopology.DATAGEN);
    builder.setBolt(multiOne, multiDataCollectionBoltOne, 1).shuffleGrouping
        (Constants.ThroughputTopology.DATAGEN,
            Constants.Fields.CHAIN_STREAM);
    conf.setComponentRam(multiOne, ByteAmount.fromMegabytes(megabytes));

    builder.setBolt(Constants.ThroughputTopology.SINGLE_DATAGATHER, singleDataCollectionBolt, stages).allGrouping(
        multiOne,
        Constants.Fields.CHAIN_STREAM);
    conf.setComponentRam(Constants.ThroughputTopology.SINGLE_DATAGATHER, ByteAmount.fromMegabytes(megabytes));

    String multiTwo = Constants.ThroughputTopology.MULTI_DATAGATHER + "_2";
    multiDataCollectionBoltTwo.setUpperComponentName(Constants.ThroughputTopology.SINGLE_DATAGATHER);
    builder.setBolt(multiTwo, multiDataCollectionBoltTwo, 1).shuffleGrouping
        (Constants.ThroughputTopology.SINGLE_DATAGATHER,
            Constants.Fields.CHAIN_STREAM);
    conf.setComponentRam(multiTwo, ByteAmount.fromMegabytes(megabytes));

    builder.setBolt(Constants.ThroughputTopology.SEND, valueSendBolt, 1).shuffleGrouping(
        multiTwo, Constants.Fields.CHAIN_STREAM);
    conf.setComponentRam(Constants.ThroughputTopology.SEND, ByteAmount.fromMegabytes(megabytes));
  }

  private static void buildAllReduceCollectiveTopology(TopologyBuilder builder, int stages, Config conf, String url) {
    ErrorReporter reporter = new ErrorReporter() {
      @Override
      public void reportError(Throwable throwable) {
        throwable.printStackTrace();
        LOG.error("error occured", throwable);
      }
    };
    IRichSpout dataSpout;
    IRichBolt valueSendBolt;

    dataSpout = new RabbitMQSpout(new RabbitMQStaticSpoutConfigurator(0, url), reporter);
    valueSendBolt = new RabbitMQBolt(new RabbitMQStaticBoltConfigurator(2, url), reporter);
    OriginBolt originBolt = new OriginBolt();

    SingleDataCollectionBolt singleDataCollectionBolt = new SingleDataCollectionBolt();
    DataGenerationBolt dataGenerationBolt = new DataGenerationBolt();
    MultiDataCollectionBolt multiDataCollectionBolt = new MultiDataCollectionBolt();
    conf.put(Constants.UPPER_COMPONENT_NAME, Constants.ThroughputTopology.SINGLE_DATAGATHER);

    builder.setSpout(Constants.ThroughputTopology.SPOUT, dataSpout, 1);
    conf.setComponentRam(Constants.ThroughputTopology.SPOUT, ByteAmount.fromMegabytes(megabytes));

    builder.setBolt(Constants.ThroughputTopology.ORIGIN, originBolt, 1).
        shuffleGrouping(Constants.ThroughputTopology.SPOUT);
    conf.setComponentRam(Constants.ThroughputTopology.ORIGIN, ByteAmount.fromMegabytes(megabytes));

    builder.setBolt(Constants.ThroughputTopology.DATAGEN, dataGenerationBolt, stages).allGrouping
        (Constants.ThroughputTopology.ORIGIN,
            Constants.Fields.CHAIN_STREAM);
    conf.setComponentRam(Constants.ThroughputTopology.DATAGEN, ByteAmount.fromMegabytes(megabytes));

    builder.setBolt(Constants.ThroughputTopology.SINGLE_DATAGATHER, singleDataCollectionBolt, stages).allReduceGrouping(
        Constants.ThroughputTopology.DATAGEN,
            Constants.Fields.CHAIN_STREAM, new CountReduceFunction());
    conf.setComponentRam(Constants.ThroughputTopology.SINGLE_DATAGATHER, ByteAmount.fromMegabytes(megabytes));

    builder.setBolt(Constants.ThroughputTopology.MULTI_DATAGATHER, multiDataCollectionBolt, 1).shuffleGrouping(
        Constants.ThroughputTopology.SINGLE_DATAGATHER,
        Constants.Fields.CHAIN_STREAM);
    conf.setComponentRam(Constants.ThroughputTopology.MULTI_DATAGATHER, ByteAmount.fromMegabytes(megabytes));

    builder.setBolt(Constants.ThroughputTopology.SEND, valueSendBolt, 1).shuffleGrouping(
        Constants.ThroughputTopology.MULTI_DATAGATHER, Constants.Fields.CHAIN_STREAM);
    conf.setComponentRam(Constants.ThroughputTopology.SEND, ByteAmount.fromMegabytes(megabytes));
  }

  private static void buildBroadCastTopology(TopologyBuilder builder, int stages, Config conf, String url) {
    ErrorReporter reporter = new ErrorReporter() {
      @Override
      public void reportError(Throwable throwable) {
        throwable.printStackTrace();
        LOG.error("error occured", throwable);
      }
    };
    IRichSpout dataSpout;
    IRichBolt valueSendBolt;

    dataSpout = new RabbitMQSpout(new RabbitMQStaticSpoutConfigurator(0, url), reporter);
    valueSendBolt = new RabbitMQBolt(new RabbitMQStaticBoltConfigurator(2, url), reporter);
    OriginBolt originBolt = new OriginBolt();

    MultiDataCollectionBolt multiDataCollectBolt = new MultiDataCollectionBolt();
    DataGenerationBolt dataGenerationBolt = new DataGenerationBolt();
    SingleDataCollectionBolt singleDataCollectionBolt = new SingleDataCollectionBolt();
    conf.put(Constants.UPPER_COMPONENT_NAME, Constants.ThroughputTopology.SINGLE_DATAGATHER);

    builder.setSpout(Constants.ThroughputTopology.SPOUT, dataSpout, 1);
    conf.setComponentRam(Constants.ThroughputTopology.SPOUT, ByteAmount.fromMegabytes(megabytes));

    builder.setBolt(Constants.ThroughputTopology.ORIGIN, originBolt, 1).
        shuffleGrouping(Constants.ThroughputTopology.SPOUT);
    conf.setComponentRam(Constants.ThroughputTopology.ORIGIN, ByteAmount.fromMegabytes(megabytes));

    builder.setBolt(Constants.ThroughputTopology.DATAGEN, dataGenerationBolt, 1).shuffleGrouping(
        Constants.ThroughputTopology.ORIGIN, Constants.Fields.CHAIN_STREAM);
    conf.setComponentRam(Constants.ThroughputTopology.DATAGEN, ByteAmount.fromMegabytes(megabytes));

    builder.setBolt(Constants.ThroughputTopology.SINGLE_DATAGATHER, singleDataCollectionBolt, stages).allGrouping(
        Constants.ThroughputTopology.DATAGEN, Constants.Fields.CHAIN_STREAM);
    conf.setComponentRam(Constants.ThroughputTopology.SINGLE_DATAGATHER, ByteAmount.fromMegabytes(megabytes));

    builder.setBolt(Constants.ThroughputTopology.MULTI_DATAGATHER, multiDataCollectBolt, 1).shuffleGrouping(
        Constants.ThroughputTopology.SINGLE_DATAGATHER, Constants.Fields.CHAIN_STREAM);
    conf.setComponentRam(Constants.ThroughputTopology.MULTI_DATAGATHER, ByteAmount.fromMegabytes(megabytes));

    builder.setBolt(Constants.ThroughputTopology.SEND, valueSendBolt, 1).shuffleGrouping(
        Constants.ThroughputTopology.MULTI_DATAGATHER, Constants.Fields.CHAIN_STREAM);
    conf.setComponentRam(Constants.ThroughputTopology.SEND, ByteAmount.fromMegabytes(megabytes));
  }
}
