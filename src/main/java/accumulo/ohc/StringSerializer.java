package accumulo.ohc;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.caffinitas.ohc.CacheSerializer;

public class StringSerializer implements CacheSerializer<String>{

  @Override
  public void serialize(String value, ByteBuffer buf) {
    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    buf.putInt(bytes.length);
    buf.put(bytes);
  }

  @Override
  public String deserialize(ByteBuffer buf) {
    int len = buf.getInt();
    byte[] bytes = new byte[len];
    buf.get(bytes);
    return new String(bytes, StandardCharsets.UTF_8);
  }

  @Override
  public int serializedSize(String value) {
    return value.getBytes(StandardCharsets.UTF_8).length + 4;
  }

}
