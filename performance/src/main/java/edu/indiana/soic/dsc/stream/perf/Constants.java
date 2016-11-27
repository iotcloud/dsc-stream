package edu.indiana.soic.dsc.stream.perf;

public class Constants {
  public static final String ARGS_NAME = "name";
  public static final String ARGS_LOCAL = "local";
  public static final String ARGS_DS_MODE = "ds_mode";
  public static final String ARGS_PARALLEL = "p";
  public static final String ARGS_SREAM_MGRS = "stmgr";
  public static final String ARGS_IOT_CLOUD = "i";
  public static final String ARGS_PARTICLES = "pt";
  public static final String ARGS_NODE_BRANCH = "nb";
  public static final String ARGS_WORKER_BRANCH= "wb";
  public static final String ARGS_BINARY_TREE= "bt";
  public static final String ARGS_PIPE_LINE = "pl";
  public static final String ARGS_FLAT_TREE = "ft";
  public static final String ARGS_FLAT_TREE_BRANCH = "fb";
  public static final String ARGS_ASYNCHRONOUS = "as";
  public static final String ARGS_PIPE_SPLIT = "pls";

  public static final String ARGS_THRPUT_SIZES = "thrSizes";
  public static final String ARGS_THRPUT_NO_MSGS = "thrN";
  public static final String ARGS_THRPUT_NO_EMPTY_MSGS = "thrNEmpty";
  public static final String ARGS_MODE = "mode";
  public static final String ARGS_THRPUT_FILENAME = "thrF";
  public static final String ARGS_SEND_INTERVAL = "int";

  // configurations
  public static final String RABBITMQ_URL = "rabbitmq_url";

  public abstract class Fields {
    public static final String BODY = "body";
    public static final String DATA_STREAM = "data";
    public static final String BROADCAST_STREAM = "broadcast";
    public static final String CHAIN_STREAM = "chain";
    public static final String GATHER_STREAM = "gather";
    public static final String SEND_STREAM = "send";
    public static final String READY_STREAM = "ready";
    public static final String CONTROL_STREAM = "control";
    public static final String TIME_FIELD = "time";
    public static final String DATA_FIELD = "data";
    public static final String SENSOR_ID_FIELD = "sensorID";
    public static final String TRACE_FIELD = "trace";
    public static final String MESSAGE_ID_FIELD = "messageID";
    public static final String MESSAGE_SIZE_FIELD = "messageSize";
    public static final String MESSAGE_INDEX_FIELD = "messageIndex";
  }

  public abstract class Topology {
    public static final String RECEIVE_SPOUT = "receive_spout";
    public static final String WORKER_BOLT = "worker_bolt";
    public static final String GATHER_BOLT = "gather_bolt";
    public static final String RESULT_SEND_BOLT = "send_bolt";
    public static final String CONTROL_SPOUT = "control_spout";
    public static final String BROADCAST_BOLT = "broadcast_bolt";
    public static final String CHAIN_BOLT = "chain_bolt";
  }

  public abstract class ThroughputTopology {
    public static final String THROUGHPUT_SPOUT = "throughputSpout";
    public static final String THROUGHPUT_LAST = "throughputLastBolt";
    public static final String THROUGHPUT_PASS_THROUGH = "throughputBolt";
  }
}
