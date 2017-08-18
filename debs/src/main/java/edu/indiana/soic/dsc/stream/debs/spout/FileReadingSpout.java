package edu.indiana.soic.dsc.stream.debs.spout;

import com.esotericsoftware.kryo.Kryo;
import com.twitter.heron.api.spout.BaseRichSpout;
import com.twitter.heron.api.spout.SpoutOutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Fields;
import edu.indiana.soic.dsc.stream.debs.Constants;
import edu.indiana.soic.dsc.stream.debs.DebsUtils;
import edu.indiana.soic.dsc.stream.debs.msg.DataReading;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class FileReadingSpout extends BaseRichSpout {
  private static Logger LOG = Logger.getLogger(FileReadingSpout.class.getName());

  private int taskId;

  private String fileName;

  private BufferedReader fileReader;

  private int noOfTasks;

  private SpoutOutputCollector outputCollector;

  private Kryo kryo;

  private Map<Integer, Integer> taskIdToIndex = new HashMap<>();

  private int maxPlugs;

  private List<Integer> plugIdsConsidered = new ArrayList<>();

  @Override
  public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
    String componentId = topologyContext.getThisComponentId();
    List<Integer> taskIds = topologyContext.getComponentTasks(componentId);
    noOfTasks = taskIds.size();
    this.taskId = topologyContext.getThisTaskId();
    fileName = (String) map.get(Constants.ARGS_IN_FILE);
    this.outputCollector = spoutOutputCollector;
    openFile(fileName);
    kryo = new Kryo();
    maxPlugs = (int) map.get(Constants.ARGS_MAX_PLUGS);
    DebsUtils.registerClasses(kryo);
    for (int i = 0; i < taskIds.size(); i++) {
      taskIdToIndex.put(taskId, i);
    }
  }

  @Override
  public void nextTuple() {
    try {
      while (true) {
        String line = fileReader.readLine();
        if (line == null) {
          LOG.info("End of file has reached");
          return;
        }

        DataReading reading = readLine(line);
        if (reading == null) {
          continue;
        } else {
          List<Object> emit = new ArrayList<Object>();
          emit.add(System.nanoTime());
          byte b[] = DebsUtils.serialize(kryo, reading);
          emit.add(b);
          String uid = "" + reading.houseId + "_" + reading.householdId + "_" + reading.plugId;
          emit.add(uid);
          LOG.info("Emmting reading with: " + reading.plugId);
          outputCollector.emit(emit);
        }
      }
    } catch (IOException e) {
      LOG.severe("Failed to read file");
    }
  }

  private DataReading readLine(String line) {
    String[] splits = line.split(",");
    if (splits.length != 7) {
      return null;
    }
    int id = Integer.parseInt(splits[0]);
    long timestamp = Long.parseLong(splits[1]);
    float value = Float.parseFloat(splits[2]);
    boolean property = Boolean.parseBoolean(splits[3]);
    int plugId = Integer.parseInt(splits[4]);
    int houseHoldId = Integer.parseInt(splits[5]);
    int houseId = Integer.parseInt(splits[6]);
    String uid = "" + houseId + "_" + houseHoldId + "_" + plugId;
    int pid = uid.hashCode();
    if (pid % noOfTasks == taskIdToIndex.get(taskId)) {
      if (maxPlugs > 0) {
        if (plugIdsConsidered.size() >= maxPlugs) {
          if (plugIdsConsidered.contains(pid)) {
            return new DataReading(id, timestamp, value, property, plugId, houseHoldId, houseId);
          }
        } else {
          plugIdsConsidered.add(pid);
          return new DataReading(id, timestamp, value, property, plugId, houseHoldId, houseId);
        }
      } else {
        return new DataReading(id, timestamp, value, property, plugId, houseHoldId, houseId);
      }
    }
    return null;
  }

  private void openFile(String openFile) {
    try {
      fileReader = new BufferedReader(new FileReader(openFile));
    } catch (FileNotFoundException e) {
      throw new RuntimeException("Failed to open file" + openFile, e);
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declare(new Fields(Constants.TIME_FIELD, Constants.DATA_FIELD,
        Constants.PLUG_ID_FILED));
  }
}
