package edu.indiana.soic.dsc.stream.debs.bolt;

import edu.indiana.soic.dsc.stream.debs.msg.PlugMsg;

import java.util.ArrayList;
import java.util.List;

public class TaskPlugMessages {
  List<Long> times = new ArrayList<>();
  List<PlugMsg> plugMsgs = new ArrayList<>();

  void removeFirst() {
    times.remove(0);
    plugMsgs.remove(0);
  }
}
