package edu.indiana.soic.dsc.stream.debs.model;

public class CircularArray {
  float values[];
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

  public void add(float val) {
    float previous = values[head];
    sum -= previous;
    sum += val;

    values[head] = val;
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
}
