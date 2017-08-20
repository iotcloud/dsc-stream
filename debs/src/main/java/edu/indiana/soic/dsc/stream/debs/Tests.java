package edu.indiana.soic.dsc.stream.debs;

import edu.indiana.soic.dsc.stream.debs.msg.DataReading;

import java.io.*;
import java.util.*;

public class Tests {
  public static void main(String[] args) throws IOException {
    List<Integer> list = new ArrayList<>();
    list.add(1);
    list.add(4);
    list.add(3);
    list.add(6);
    list.add(2);

    print(list);

    Collections.sort(list);

    print(list);
    readFile();
  }

  public static void print(List<Integer> list) {
    for (int i : list) {
      System.out.format(i + " ") ;
    }
    System.out.format("\n");
  }

  private static void readFile() throws IOException {
    Map<String, String> readingMap = new HashMap<>();
    long time = 0;
    long currentTime = 0;
    String openFile = "/home/supun/experiments/1mil.csv";
    PrintWriter writer = new PrintWriter("out.csv");
    try {
      BufferedReader fileReader = new BufferedReader(new FileReader(openFile));
      while (true) {
        String line = fileReader.readLine();
        if (line == null) {
          System.out.println("End of file has reached");
          return;
        }

        DataReading reading = readLine(line);
        if (reading == null) {
          continue;
        } else {
          currentTime = reading.timeStamp;
          if (currentTime != time) {
            time = currentTime;
            readingMap.clear();
          }
          String key = reading.houseId + "_" + reading.householdId + "_" + reading.plugId;
          if (readingMap.containsKey(key)) {
            String x = "Duplicate key: " + key + " line: " + line + " previous: " + readingMap.get(key);
            System.out.println(x);
            writeLine(x, writer);
          } else {
            readingMap.put(key, line);
          }
        }
      }
    } catch (FileNotFoundException e) {
      throw new RuntimeException("Failed to open file" + openFile, e);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static void writeLine(String line, PrintWriter writer) {
    writer.println(line);
    writer.flush();
  }

  private static DataReading readLine(String line) {
    int noOfTasks = 1;
    int taskId = 0;
    int maxPlugs = 64;
    List<Integer> plugIdsConsidered = new ArrayList<>();

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
    if (pid % noOfTasks == taskId) {
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
}
