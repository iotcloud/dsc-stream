package edu.indiana.soic.dsc.stream.debs;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.HeronSubmitter;
import com.twitter.heron.api.bolt.BaseRichBolt;
import com.twitter.heron.api.exception.AlreadyAliveException;
import com.twitter.heron.api.exception.InvalidTopologyException;
import com.twitter.heron.api.spout.BaseRichSpout;
import com.twitter.heron.api.topology.TopologyBuilder;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.common.basics.ByteAmount;
import com.twitter.heron.simulator.Simulator;
import edu.indiana.soic.dsc.stream.debs.bolt.MedianBolt;
import edu.indiana.soic.dsc.stream.debs.bolt.OutputBolt;
import edu.indiana.soic.dsc.stream.debs.bolt.ReductionBolt;
import edu.indiana.soic.dsc.stream.debs.bolt.ReductionFunction;
import edu.indiana.soic.dsc.stream.debs.spout.FileReadingSpout;
import org.apache.commons.cli.*;

import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DebsTopology {
  private static Logger LOG = Logger.getLogger(DebsTopology.class.getName());

  private static int megabytes = 256;

  public static void main(String[] args) throws ParseException, AlreadyAliveException, InvalidTopologyException {
    TopologyBuilder builder = new TopologyBuilder();

    Options options = new Options();
    options.addOption(Constants.ARGS_NAME, true, "Name of the topology");
    options.addOption(Constants.ARGS_LOCAL, false, "Weather we want run locally");
    options.addOption(Constants.ARGS_PARALLEL, true, "No of parallel nodes");
    options.addOption(createOption(Constants.ARGS_SPOUT_PARALLEL, true, "No of parallel spout nodes", false));
    options.addOption(Constants.ARGS_STREAM_MGRS, true, "No of stream managers");
    options.addOption(createOption(Constants.ARGS_DEBUG, false, "Print debug messages", false));
    options.addOption(createOption(Constants.ARGS_PRINT_INTERVAL, true, "Print debug messages", false));
    options.addOption(createOption(Constants.ARGS_MAX_PENDING, true, "Max pending", false));
    options.addOption(createOption(Constants.ARGS_MEM, true, "Mem", false));

    CommandLineParser commandLineParser = new BasicParser();
    CommandLine cmd = commandLineParser.parse(options, args);
    String name = cmd.getOptionValue(Constants.ARGS_NAME);
    String inFile = cmd.getOptionValue(Constants.ARGS_IN_FILE);
    String outFile = cmd.getOptionValue(Constants.ARGS_OUT_FILE);
    boolean debug = cmd.hasOption(Constants.ARGS_DEBUG);
    boolean local = cmd.hasOption(Constants.ARGS_LOCAL);
    String pValue = cmd.getOptionValue(Constants.ARGS_PARALLEL);
    int p = Integer.parseInt(pValue);

    int streamManagers = Integer.parseInt(cmd.getOptionValue(Constants.ARGS_STREAM_MGRS));
    int interval = 1;
    int maxPending = 10;
    if (cmd.hasOption(Constants.ARGS_MAX_PENDING)) {
      maxPending = Integer.parseInt(cmd.getOptionValue(Constants.ARGS_MAX_PENDING));
    }

    if (cmd.hasOption(Constants.ARGS_MEM)) {
      megabytes = Integer.parseInt(cmd.getOptionValue(Constants.ARGS_MEM));
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
    conf.put(Constants.ARGS_PARALLEL, p);
    conf.put(Constants.ARGS_SPOUT_PARALLEL, spoutParallel);
    conf.put(Constants.ARGS_PRINT_INTERVAL, interval);
    conf.put(Constants.ARGS_MAX_PENDING, maxPending);
    conf.put(Constants.ARGS_STREAM_MGRS, streamManagers);
    conf.put(Constants.ARGS_IN_FILE, inFile);
    conf.put(Constants.ARGS_OUT_FILE, outFile);
    conf.setEnableAcking(false);
    if (cmd.hasOption(Constants.ARGS_PARALLEL)) {
      conf.put(Constants.ARGS_PARALLEL, p);
    }

    buildReduceSerialTopology(builder, p, spoutParallel, spoutParallel, conf);
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
        LOG.log(Level.SEVERE, "Error", t);
      }
    }
  }

  private static void buildReduceSerialTopology(TopologyBuilder builder, int medianParallel,
                                                int spoutParallel, int reductionParallel, Config conf) {
    BaseRichSpout dataSpout;

    dataSpout = new FileReadingSpout();
    MedianBolt medianBolt = new MedianBolt();
    ReductionFunction reductionFunction = new ReductionFunction();
    ReductionBolt reductionBolt = new ReductionBolt();

    builder.setSpout(Constants.TOPOLOGY_FILE_READ_SPOUT, dataSpout, spoutParallel);
    builder.setBolt(Constants.MEDIAN_BOLT, medianBolt, medianParallel).fieldsGrouping(
        Constants.TOPOLOGY_FILE_READ_SPOUT, new Fields(Constants.PLUG_ID_FILED));

    builder.setBolt(Constants.REDUCTION_BOLT, reductionBolt, reductionParallel).
        allReduceGrouping(Constants.MEDIAN_BOLT, Constants.PLUG_REDUCE_STREAM, reductionFunction);

    conf.setComponentRam(Constants.TOPOLOGY_FILE_READ_SPOUT, ByteAmount.fromMegabytes(megabytes));
    conf.setComponentRam(Constants.MEDIAN_BOLT, ByteAmount.fromMegabytes(megabytes));
    conf.setComponentRam(Constants.REDUCTION_BOLT, ByteAmount.fromMegabytes(megabytes));
  }

  public static Option createOption(String opt, boolean hasArg, String description, boolean required) {
    Option symbolListOption = new Option(opt, hasArg, description);
    symbolListOption.setRequired(required);
    return symbolListOption;
  }
}
