package edu.indiana.soic.dsc.stream.debs;

public abstract class Constants {
  public static final String HOUSE_REDUCE_STREAM = "house_reduce_stream";
  public static final String PLUG_REDUCE_STREAM = "plug_reduce_stream";

  public static final String PLUG_FIELD = "plug_field";
  public static final String TIME_FIELD = "time_field";
  public static final String DATA_FIELD = "data_field";
  public static final String HOUSE_PLUG_FIELD = "house_plug_field";
  public static final String PLUG_ID_FILED = "plug_id_field";
  public static final String OUT_FILED = "out_field";

  public static final String ARGS_NAME = "name";
  public static final String ARGS_IN_FILE = "if";
  public static final String ARGS_OUT_FILE = "of";
  public static final String ARGS_PARALLEL = "p";
  public static final String ARGS_MAX_PLUGS = "maxPlugs";

  public static final String ARGS_STREAM_MGRS = "stmgr";
  public static final String ARGS_DEBUG = "g";
  public static final String ARGS_PRINT_INTERVAL = "pi";
  public static final String ARGS_SPOUT_PARALLEL = "sp";
  public static final String ARGS_MAX_PENDING = "mp";
  public static final String ARGS_MEM = "mem";
  public static final String ARGS_LOCAL = "local";
  public static final String ARGS_MODE = "mode";

  public static final String TOPOLOGY_FILE_READ_SPOUT = "fileReadBolt";
  public static final String TOPOLOGY_FILE_WRITE_BOLT = "fileWriteBolt";
  public static final String REDUCTION_BOLT = "reductionBolt";
  public static final String MEDIAN_BOLT = "medianBolt";

}
