package edu.indiana.soic.dsc.stream.collectives.rabbit;

import com.ss.commons.DestinationChangeListener;
import com.ss.commons.DestinationChanger;
import com.ss.commons.DestinationConfiguration;

public class StaticDestinations implements DestinationChanger {
  private DestinationChangeListener dstListener;

  private int sender;
  private String url;
  public StaticDestinations(int sender, String url) {
    this.sender = sender;
    this.url = url;
  }

  @Override
  public void start() {
    DestinationConfiguration configuration = new DestinationConfiguration("rabbitmq", url, "test", "test");
    configuration.setGrouped(true);
    if (sender == 0) {
      configuration.addProperty("queueName", "laser_scan");
      configuration.addProperty("routingKey", "laser_scan");
      configuration.addProperty("exchange", "simbard_laser");
    } else if (sender == 2) {
      configuration.addProperty("queueName", "best");
      configuration.addProperty("routingKey", "best");
      configuration.addProperty("exchange", "simbard_best");
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
