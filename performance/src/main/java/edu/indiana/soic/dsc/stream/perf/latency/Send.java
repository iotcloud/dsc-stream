package edu.indiana.soic.dsc.stream.perf.latency;

public class Send {
  public String id;
  public int size;
  public int index;
  public long time;

  public Send(String id, int size, int index, long time) {
    this.id = id;
    this.size = size;
    this.index = index;
    this.time = time;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    Send send = (Send) o;

    return id != null ? id.equals(send.id) : send.id == null;

  }

  @Override
  public int hashCode() {
    return id != null ? id.hashCode() : 0;
  }
}
