package edu.indiana.soic.dsc.stream.collectives;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.HeronSubmitter;
import com.twitter.heron.api.topology.TopologyBuilder;
import com.twitter.heron.common.basics.ByteAmount;
import com.twitter.heron.simulator.Simulator;
import edu.indiana.soic.dsc.stream.perf.*;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class CollectiveTopology {
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
    options.addOption(Utils.createOption(Constants.ARGS_RATE, true, "Max pending", false));

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
    conf.setMaxSpoutPending(maxPending);

    conf.setEnableAcking(true);
    if (mode.equals("c")) {
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

  private static void buildThroughputTopologyAck(TopologyBuilder builder, int stages, Config conf, int spoutParallel) {
    CollectiveAckSpout spout = new CollectiveAckSpout();
    CollectiveLastBolt lastBolt = new CollectiveLastBolt();
    CollectivePassThroughBolt passThroughBolt = new CollectivePassThroughBolt();

    builder.setSpout(Constants.ThroughputTopology.THROUGHPUT_SPOUT, spout, spoutParallel);
    conf.setComponentRam(Constants.ThroughputTopology.THROUGHPUT_SPOUT, ByteAmount.fromGigabytes(4));

    builder.setBolt(Constants.ThroughputTopology.THROUGHPUT_PASS_THROUGH, passThroughBolt, stages).shuffleGrouping
        (Constants.ThroughputTopology.THROUGHPUT_SPOUT,
            Constants.Fields.CHAIN_STREAM);

    builder.setBolt(Constants.ThroughputTopology.THROUGHPUT_LAST, lastBolt, stages).reduceGrouping
      (Constants.ThroughputTopology.THROUGHPUT_PASS_THROUGH,
            Constants.Fields.CHAIN_STREAM, new CountReduceFunction());

    conf.setComponentRam(Constants.ThroughputTopology.THROUGHPUT_LAST, ByteAmount.fromGigabytes(4));
  }
}
