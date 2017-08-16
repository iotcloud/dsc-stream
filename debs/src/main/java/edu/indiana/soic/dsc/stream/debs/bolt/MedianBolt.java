package edu.indiana.soic.dsc.stream.debs.bolt;

import com.esotericsoftware.kryo.Kryo;
import com.twitter.heron.api.bolt.BaseRichBolt;
import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Tuple;
import edu.indiana.soic.dsc.stream.debs.Constants;
import edu.indiana.soic.dsc.stream.debs.DebsUtils;
import edu.indiana.soic.dsc.stream.debs.model.House;
import edu.indiana.soic.dsc.stream.debs.model.Plug;
import edu.indiana.soic.dsc.stream.debs.msg.DataReading;
import edu.indiana.soic.dsc.stream.debs.msg.PlugMsg;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MedianBolt extends BaseRichBolt {
  private Map<Integer, House> plugToHouse = new HashMap<>();
  private OutputCollector outputCollection;
  private Kryo kryo;

  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    this.outputCollection = outputCollector;
    kryo = new Kryo();
    DebsUtils.registerClasses(kryo);
  }

  @Override
  public void execute(Tuple tuple) {
    Object input = tuple.getValueByField(Constants.DATA_FIELD);
    Object time = tuple.getValueByField(Constants.TIME_FIELD);

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
    float averageHourly = plug.averageHourly();
    float averageDaily = plug.averageDaily();
    long hourlyStart = plug.hourlyStartTime();
    long hourlyEnd = plug.hourlyEndTime();
    long dailyStart = plug.dailyStartTime();
    long dailyEnd = plug.dailyEndTime();
    ArrayList<Integer> aggrs = new ArrayList<>();
    aggrs.add(reading.plugId);

    ArrayList<Object> list = new ArrayList<Object>();
    list.add(time);
    PlugMsg plugMsg = new PlugMsg(plug.id, averageHourly, averageDaily, hourlyStart, hourlyEnd, dailyStart, dailyEnd, aggrs);
    byte[] b = DebsUtils.serialize(kryo, plugMsg);
    list.add(b);

    outputCollection.emit(Constants.PLUG_REDUCE_STREAM, list);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declareStream(Constants.PLUG_REDUCE_STREAM,
        new Fields(Constants.TIME_FIELD, Constants.PLUG_FIELD));

    outputFieldsDeclarer.declareStream(Constants.HOUSE_REDUCE_STREAM,
        new Fields(Constants.TIME_FIELD, Constants.HOUSE_PLUG_FIELD));
  }
}
