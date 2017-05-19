package edu.indiana.soic.dsc.stream.collectives.rabbit;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import cgl.sensorstream.core.rabbitmq.DefaultRabbitMQMessageBuilder;
import com.ss.commons.BoltConfigurator;
import com.ss.commons.DestinationChanger;
import com.ss.commons.DestinationSelector;
import com.ss.commons.MessageBuilder;

import java.util.HashMap;
import java.util.Map;

public class RabbitMQStaticBoltConfigurator implements BoltConfigurator {
  int bolt;

  public RabbitMQStaticBoltConfigurator(int bolt) {
    this.bolt = bolt;
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
    return new StaticDestinations(bolt);
  }
}