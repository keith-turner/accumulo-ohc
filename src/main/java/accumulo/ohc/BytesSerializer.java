package accumulo.ohc;

import java.nio.ByteBuffer;

import org.caffinitas.ohc.CacheSerializer;

public class BytesSerializer implements CacheSerializer<byte[]> {

  @Override
  public void serialize(byte[] value, ByteBuffer buf) {
    buf.putInt(value.length);
    buf.put(value);
  }

  @Override
  public byte[] deserialize(ByteBuffer buf) {
    int len = buf.getInt();
    byte[] bytes = new byte[len];
    buf.get(bytes);
    return bytes;
  }

  @Override
  public int serializedSize(byte[] value) {
    return value.length + 4;
  }

}
