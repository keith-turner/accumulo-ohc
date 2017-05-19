package accumulo.ohc;

import org.apache.accumulo.core.file.blockfile.cache.CacheEntry;

public class OhcCacheEntry implements CacheEntry {

  private byte[] buf;
  private Object index;
  private boolean allowIndex;


  OhcCacheEntry(byte[] buf, boolean allowIndex){
    this.buf = buf;
    this.allowIndex = allowIndex;
  }

  @Override
  public byte[] getBuffer() {
    return buf;
  }

  @Override
  public Object getIndex() {
    return index;
  }

  @Override
  public void setIndex(Object idx) {
    if(allowIndex) {
      this.index = idx;
    }
  }
}
