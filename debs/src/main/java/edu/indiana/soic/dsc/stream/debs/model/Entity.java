package edu.indiana.soic.dsc.stream.debs.model;

import edu.indiana.soic.dsc.stream.debs.msg.DataReading;

public interface Entity {
  void addReading(DataReading reading);
}
