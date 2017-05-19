package edu.indiana.soic.dsc.stream.collectives.rabbit;


import cgl.sensorstream.core.StreamComponents;
import cgl.sensorstream.core.StreamTopologyBuilder;
import com.ss.commons.DestinationChangeListener;
import com.ss.commons.DestinationChanger;
import com.ss.commons.DestinationConfiguration;
import edu.indiana.soic.dsc.stream.perf.Constants;

import java.util.Map;

public class StaticDestinations implements DestinationChanger {
  private DestinationChangeListener dstListener;

  private int sender;

  public StaticDestinations(int sender) {
    this.sender = sender;
  }

  @Override
  public void start() {
    StreamTopologyBuilder streamTopologyBuilder = new StreamTopologyBuilder();
    StreamComponents components = streamTopologyBuilder.buildComponents();
    Map conf = components.getConf();
    String url = (String) conf.get(Constants.RABBITMQ_URL);
    DestinationConfiguration configuration = new DestinationConfiguration("rabbitmq", url, "test", "test");
    configuration.setGrouped(true);
    if (sender == 0) {
      configuration.addProperty("queueName", "laser_scan");
      configuration.addProperty("routingKey", "laser_scan");
      configuration.addProperty("exchange", "simbard_laser");
    } else if (sender == 1){
      configuration.addProperty("queueName", "map");
      configuration.addProperty("routingKey", "map");
      configuration.addProperty("exchange", "simbard_map");
    } else if (sender == 2) {
      configuration.addProperty("queueName", "best");
      configuration.addProperty("routingKey", "best");
      configuration.addProperty("exchange", "simbard_best");
    } else if (sender == 3) {
      configuration.addProperty("queueName", "control");
      configuration.addProperty("routingKey", "control");
      configuration.addProperty("exchange", "simbard_control");
    }

    dstListener.addDestination("rabbitmq", configuration);
    dstListener.addPathToDestination("rabbitmq", "test");
  }

  @Override
  public void stop() {
    dstListener.removeDestination("rabbitmq");
  }

  @Override
  public void registerListener(DestinationChangeListener destinationChangeListener) {
    this.dstListener = destinationChangeListener;
  }

  @Override
  public void setTask(int i, int i2) {

  }

  @Override
  public int getTaskIndex() {
    return 0;
  }

  @Override
  public int getTotalTasks() {
    return 0;
  }
}
