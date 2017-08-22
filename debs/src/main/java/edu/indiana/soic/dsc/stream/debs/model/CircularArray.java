package edu.indiana.soic.dsc.stream.debs.model;

import java.util.logging.Logger;

public class CircularArray {
  private static Logger LOG = Logger.getLogger(CircularArray.class.getName());

  private float values[];
  private long times[];

  private int head;
  private int filledAmount;
  private int capacity;
  private float sum = 0;

  public CircularArray(int capacity) {
    this.capacity = capacity;
    this.head = 0;
    this.filledAmount = 0;
    LOG.info("Creating circular array of size: " + capacity);
    this.values = new float[capacity];
    this.times = new long[capacity];
  }

  public void add(float val, long time) {
    float previous = values[head];
    sum -= previous;
    sum += val;

    values[head] = val;
    times[head] = time;

    if (filledAmount < capacity) {
      filledAmount++;
    }

    // move the circular array pointer
    if (capacity == filledAmount) {
      if (head + 1 < capacity) {
        head++;
      } else {
        head = 0;
      }
    } else {
      head++;
    }
  }

  public float get(int index) {
    return values[index];
  }

  public float getFirst() {
    return values[0];
  }

  public float sum() {
    return sum;
  }

  public int noOfValues() {
    return filledAmount;
  }

  public long getStartTime() {
    if (filledAmount < capacity && filledAmount > 0) {
      return times[head];
    } else if (filledAmount == capacity) {
      if (head == 0) {
        return times[filledAmount - 1];
      } else {
        return times[head - 1];
      }
    }
    return 0;
  }

  public long getEndTime() {
    if (filledAmount < capacity && filledAmount > 0) {
      return times[filledAmount - 1];
    } else if (filledAmount == capacity) {
      return times[head];
    }
    return 0;
  }
}
