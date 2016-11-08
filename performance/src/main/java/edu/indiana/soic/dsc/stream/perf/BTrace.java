package edu.indiana.soic.dsc.stream.perf;

import java.util.Map;

public class BTrace {
  private int taskId;

  private long time;

  public long getTime() {
    return time;
  }

  public void setTime(long time) {
    this.time = time;
  }

  public void setTaskId(int taskId) {
    this.taskId = taskId;
  }

  public int getTaskId() {
    return taskId;
  }
}

