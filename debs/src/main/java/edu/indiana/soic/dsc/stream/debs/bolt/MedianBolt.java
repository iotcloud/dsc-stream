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
import edu.indiana.soic.dsc.stream.debs.model.Calculation;
import edu.indiana.soic.dsc.stream.debs.model.House;
import edu.indiana.soic.dsc.stream.debs.msg.DataReading;
import edu.indiana.soic.dsc.stream.debs.msg.PlugMsg;
import edu.indiana.soic.dsc.stream.debs.msg.PlugValue;

import java.util.*;
import java.util.logging.Logger;

public class MedianBolt extends BaseRichBolt {
  private static Logger LOG = Logger.getLogger(MedianBolt.class.getName());

  private Map<Integer, House> houses;

  private OutputCollector outputCollector;

  private Kryo kryo;
  
  private int thisTaskId;

  private boolean debug;

  private int pi;

  private int count = 0;

  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    houses = new HashMap<>();
    this.outputCollector = outputCollector;
    kryo = new Kryo();
    DebsUtils.registerClasses(kryo);
    thisTaskId = topologyContext.getThisTaskId();
    this.debug = (boolean) map.get(Constants.ARGS_DEBUG);
    this.pi = (int) map.get(Constants.ARGS_PRINT_INTERVAL);
  }

  @Override
  public void execute(Tuple tuple) {
    Object input = tuple.getValueByField(Constants.DATA_FIELD);
    Object time = tuple.getValueByField(Constants.TIME_FIELD);
    String plugId = (String)tuple.getValueByField(Constants.PLUG_ID_FILED);

    DataReading reading = (DataReading) DebsUtils.deSerialize(kryo, (byte [])input, DataReading.class);
    House house;
    if (houses.containsKey(reading.houseId)) {
      house = houses.get(reading.houseId);
    } else {
      house = new House(reading.houseId);
      houses.put(reading.houseId, house);
    }
    house.addReading(reading);

    processTaskPlugMessages();
    count++;
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declareStream(Constants.PLUG_REDUCE_STREAM,
        new Fields(Constants.TIME_FIELD, Constants.PLUG_FIELD));

    outputFieldsDeclarer.declareStream(Constants.HOUSE_REDUCE_STREAM,
        new Fields(Constants.TIME_FIELD, Constants.HOUSE_PLUG_FIELD));
  }

  private void processTaskPlugMessages() {
    PlugMsg plugMsg = reduceMessages();
    byte[] b = DebsUtils.serialize(kryo, plugMsg);

    List<Object> emit = new ArrayList<>();
    emit.add(System.nanoTime());
    emit.add(b);

    outputCollector.emit(Constants.PLUG_REDUCE_STREAM, emit);
  }

  private PlugMsg reduceMessages() {
    PlugMsg aggrPlugMsg;
    int totalDaily = 0;
    int totalHourly = 0;
    float valueDaily = 0;
    float valueHourly = 0;
    ArrayList<PlugValue> hourlyPlugValues = new ArrayList<>();
    ArrayList<PlugValue> dailyPlugValues = new ArrayList<>();

    for (Map.Entry<Integer, House> e : houses.entrySet()) {
      House house = e.getValue();
      Calculation dailyCalculation = house.calculateDaily();
      Calculation hourlyCalculation = house.calculateHourly();

      totalDaily += dailyCalculation.noOfItems;
      totalHourly += hourlyCalculation.noOfItems;
      valueDaily += dailyCalculation.value;
      valueHourly += hourlyCalculation.value;
      hourlyPlugValues.addAll(e.getValue().getHourlyPlugValues());
      dailyPlugValues.addAll(e.getValue().getDailyPlugValues());
    }

//    LOG.info("Hourly plug values size: " + hourlyPlugValues.size() + " daily: " + dailyPlugValues.size());
    aggrPlugMsg = new PlugMsg(thisTaskId, valueHourly, valueDaily, totalHourly,
        totalDaily, hourlyPlugValues, dailyPlugValues);
    return aggrPlugMsg;
  }
}
