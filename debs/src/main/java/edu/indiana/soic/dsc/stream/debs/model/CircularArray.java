package edu.indiana.soic.dsc.stream.debs.model;

public class CircularArray {
  float values[];
  long times[];

  int head;
  int filledAmount;
  int capacity;
  float sum = 0;

  public CircularArray(int capacity) {
    this.capacity = capacity;
    this.head = 0;
    this.filledAmount = 0;
    this.values = new float[capacity];
  }

  public void add(float val, long time) {
    float previous = values[head];
    sum -= previous;
    sum += val;

    values[head] = val;
    times[head] = time;

    // move the circular array pointer
    if (capacity == filledAmount) {
      if (head + 1 < capacity) {
        head++;
      } else {
        head = 0;
      }
    } else {
      head++;
      filledAmount++;
    }
  }

  public float get(int index) {
    return values[index];
  }

  public float getFirst() {
    return values[0];
  }

  public float average() {
    return sum / filledAmount;
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
