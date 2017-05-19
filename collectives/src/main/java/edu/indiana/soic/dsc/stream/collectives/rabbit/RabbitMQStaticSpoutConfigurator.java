package edu.indiana.soic.dsc.stream.collectives.rabbit;

import com.ss.commons.DestinationChanger;
import com.ss.commons.MessageBuilder;
import com.ss.commons.SpoutConfigurator;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.tuple.Fields;
import edu.indiana.soic.dsc.stream.collectives.Constants;

import java.util.HashMap;
import java.util.Map;

public class RabbitMQStaticSpoutConfigurator implements SpoutConfigurator {
  int spout;
  String url;

  public RabbitMQStaticSpoutConfigurator(int spout, String url) {
    this.spout = spout;
    this.url = url;
  }

  @Override
  public MessageBuilder getMessageBuilder() {
    return new DefaultRabbitMQMessageBuilder();
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    if (spout == 0) {
      outputFieldsDeclarer.declare(new Fields(Constants.Fields.BODY,
          Constants.Fields.SENSOR_ID_FIELD, Constants.Fields.TIME_FIELD));
    } else {
      outputFieldsDeclarer.declareStream(Constants.Fields.CONTROL_STREAM,
          new Fields(Constants.Fields.BODY,
              Constants.Fields.SENSOR_ID_FIELD, Constants.Fields.TIME_FIELD));
    }
  }

  @Override
  public int queueSize() {
    return 64;
  }

  @Override
  public Map<String, String> getProperties() {
    return new HashMap<String, String>();
  }

  @Override
  public DestinationChanger getDestinationChanger() {
    return new StaticDestinations(spout, url);
  }

  @Override
  public String getStream() {
    if (spout == 0) {
      return null;
    } else {
      return Constants.Fields.CONTROL_STREAM;
    }
  }
}
