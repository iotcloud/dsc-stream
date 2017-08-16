package edu.indiana.soic.dsc.stream.debs;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import edu.indiana.soic.dsc.stream.debs.msg.DataReading;
import edu.indiana.soic.dsc.stream.debs.msg.HouseMsg;
import edu.indiana.soic.dsc.stream.debs.msg.PlugMsg;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Random;

public class DebsUtils {
  /**
   * Serialize an object using kryo and return the bytes
   * @param kryo instance of kryo
   * @param object the object to be serialized
   * @return the serialized bytes
   */
  public static byte[] serialize(Kryo kryo, Object object) {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    Output output = new Output(byteArrayOutputStream);
    kryo.writeObject(output, object);
    output.flush();
    return byteArrayOutputStream.toByteArray();
  }

  /**
   * De Serialize bytes using kryo and return the object
   * @param kryo instance of kryo
   * @param b the byte to be de serialized
   * @return the serialized bytes
   */
  public static Object deSerialize(Kryo kryo, byte []b, Class e) {
    return kryo.readObject(new Input(new ByteArrayInputStream(b)), e);
  }

  public static byte[] generateData(int size) {
    byte b[] = new byte[size];
    new Random().nextBytes(b);
    return b;
  }

  public static void registerClasses(Kryo kryo) {
    kryo.register(DataReading.class);
    // kryo.register(ChainTrace.class);
    kryo.register(HouseMsg.class);
    kryo.register(PlugMsg.class);
  }
}
