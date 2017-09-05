package edu.indiana.soic.dsc.stream.collectives;

import com.esotericsoftware.kryo.Kryo;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.proto.system.HeronTuples;
import org.apache.commons.cli.*;

import java.util.ArrayList;
import java.util.List;

public class SerializationTest {
  public static void main(String[] args) throws ParseException, InvalidProtocolBufferException {
    Options options = new Options();
    options.addOption("size", true, "Name of the topology");
    options.addOption("itr", true, "Number of iterations");
    CommandLineParser commandLineParser = new BasicParser();
    CommandLine cmd = commandLineParser.parse(options, args);

    int size = Integer.parseInt(cmd.getOptionValue("size"));
    int itr = Integer.parseInt(cmd.getOptionValue("itr"));

    byte []data = Utils.generateData(size);
    Kryo kryo = new Kryo();
    long start = System.currentTimeMillis();
    run(kryo, itr, data);
    System.out.println((System.currentTimeMillis() - start));
  }

  private static void run(Kryo kryo, int iterations, byte[] buffers) throws InvalidProtocolBufferException {
    int total = 0;
    int r = 0;
    int id = 0;
    for (int i = 0; i < iterations; i++) {
      List<Object> tuple = new ArrayList<>();
      tuple.add(buffers);
      tuple.add("SensorID");
      tuple.add(System.currentTimeMillis());

      HeronTuples.HeronDataTupleSet.Builder currentDataTuple = HeronTuples.HeronDataTupleSet.newBuilder();
      HeronTuples.HeronDataTuple.Builder bldr = HeronTuples.HeronDataTuple.newBuilder();
      bldr.setKey(id++);
      bldr.setSourceTask(12546);
      for (Object obj : tuple) {
        byte[] b = Utils.serialize(kryo, obj);
        ByteString bstr = ByteString.copyFrom(b);
        bldr.addValues(bstr);
      }
      currentDataTuple.addTuples(bldr);
      TopologyAPI.StreamId.Builder stream = TopologyAPI.StreamId.newBuilder();
      stream.setComponentName("test");
      stream.setId("id");

      currentDataTuple.setStream(stream);

      HeronTuples.HeronDataTupleSet set = currentDataTuple.build();
      byte[] serialized = set.toByteArray();
      byte []b = new byte[serialized.length];
      System.arraycopy(serialized, 0, b, 0, serialized.length);
      byte va = b[10];
      r += new Byte(va).intValue();
      total += b.length;

      HeronTuples.HeronDataTupleSet tupleSet = HeronTuples.HeronDataTupleSet.parseFrom(b);
      List<HeronTuples.HeronDataTuple> object = tupleSet.getTuplesList();
    }
    System.out.println(total);
    System.out.println(r);
  }
}
