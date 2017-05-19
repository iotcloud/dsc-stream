package edu.indiana.soic.dsc.stream.collectives.rabbit;

import com.ss.commons.BoltConfigurator;
import com.ss.commons.DestinationChanger;
import com.ss.commons.DestinationSelector;
import com.ss.commons.MessageBuilder;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

public class RabbitMQStaticBoltConfigurator implements BoltConfigurator {
  int bolt;
  String url;

  public RabbitMQStaticBoltConfigurator(int bolt, String url) {
    this.bolt = bolt;
    this.url = url;
  }

  @Override
  public MessageBuilder getMessageBuilder() {
    return new DefaultRabbitMQMessageBuilder();
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
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
  public DestinationSelector getDestinationSelector() {
    return new DestinationSelector() {
      @Override
      public String select(Tuple tuple) {
        return "test";
      }
    };
  }

  @Override
  public DestinationChanger getDestinationChanger() {
    return new StaticDestinations(bolt, url);
  }
}