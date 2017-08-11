package edu.indiana.soic.dsc.stream.collectives;

public class Constants {
  public static final String ARGS_NAME = "name";
  public static final String ARGS_LOCAL = "local";
  public static final String ARGS_DS_MODE = "ds_mode";
  public static final String ARGS_PARALLEL = "p";
  public static final String ARGS_SREAM_MGRS = "stmgr";
  public static final String ARGS_IOT_CLOUD = "i";
  public static final String ARGS_BINARY_TREE= "bt";
  public static final String ARGS_PIPE_LINE = "pl";
  public static final String ARGS_FLAT_TREE = "ft";
  public static final String ARGS_FLAT_TREE_BRANCH = "fb";
  public static final String ARGS_ASYNCHRONOUS = "as";
  public static final String ARGS_PIPE_SPLIT = "pls";
  public static final String ARGS_DEBUG = "g";
  public static final String ARGS_PRINT_INTERVAL = "pi";
  public static final String ARGS_GATHER = "gather";
  public static final String UPPER_COMPONENT_NAME = "upperComponentName";
  public static final String PASSTHROUGH_REMOVE_BODY = "removeBody";

  public static final String ARGS_THRPUT_SIZES = "thrSizes";
  public static final String ARGS_THRPUT_NO_MSGS = "thrN";
  public static final String ARGS_THRPUT_NO_EMPTY_MSGS = "thrNEmpty";
  public static final String ARGS_MODE = "mode";
  public static final String ARGS_RATE = "rate";
  public static final String ARGS_URL = "url";
  public static final String ARGS_THRPUT_FILENAME = "thrF";
  public static final String ARGS_SEND_INTERVAL = "int";
  public static final String ARGS_MEM = "mem";

  // configurations
  public static final String RABBITMQ_URL = "rabbitmq_url";
  public static final String ARGS_SPOUT_PARALLEL = "sp";
  public static final String ARGS_MAX_PENDING = "mp";

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
    public static final String TIME_FIELD2 = "time2";
    public static final String DATA_FIELD = "data";
    public static final String SENSOR_ID_FIELD = "sensorID";
    public static final String TRACE_FIELD = "trace";
    public static final String MESSAGE_ID_FIELD = "messageID";
    public static final String MESSAGE_SIZE_FIELD = "messageSize";
    public static final String MESSAGE_INDEX_FIELD = "messageIndex";
  }

  public abstract class ThroughputTopology {
    public static final String SPOUT = "spout";
    public static final String LAST = "lastbolt";
    public static final String DATAGEN = "datagenbolt";
    public static final String PASSTHROUGH = "passthroughbolt";
    public static final String SEND = "sendbolt";
    public static final String ORIGIN = "originbolt";
    public static final String MULTI_DATAGATHER = "multidatagather";
    public static final String SINGLE_DATAGATHER = "singledatagather";
  }
}


