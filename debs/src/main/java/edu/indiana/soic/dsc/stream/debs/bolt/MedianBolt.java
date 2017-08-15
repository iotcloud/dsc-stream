package edu.indiana.soic.dsc.stream.debs.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import edu.indiana.soic.dsc.stream.debs.Constants;
import edu.indiana.soic.dsc.stream.debs.model.House;
import edu.indiana.soic.dsc.stream.debs.model.Plug;
import edu.indiana.soic.dsc.stream.debs.msg.DataReading;

import java.util.HashMap;
import java.util.Map;

public class MedianBolt extends BaseRichBolt {
  Map<Integer, House> plugToHouse = new HashMap<>();

  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

  }

  @Override
  public void execute(Tuple tuple) {
    Object input = tuple.getValueByField(Constants.DATA_FIELD);
    DataReading reading = (DataReading) input;

    House house;
    if (plugToHouse.containsKey(reading.houseId)) {
      house = plugToHouse.get(reading.houseId);
    } else {
      house = new House();
      plugToHouse.put(reading.houseId, house);
    }
    house.addReading(reading);

    Plug plug = house.getPlug(reading.householdId, reading.plugId);
    double averageHourly = plug.averageHourly();
    double averageDaily = plug.averageDaily();


  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declareStream(Constants.PLUG_REDUCE_STREAM,
        new Fields(Constants.TIME_FIELD, Constants.PLUG_FIELD));

    outputFieldsDeclarer.declareStream(Constants.HOUSE_REDUCE_STREAM,
        new Fields(Constants.TIME_FIELD, Constants.HOUSE_PLUG_FIELD));
  }
}
