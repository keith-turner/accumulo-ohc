package accumulo.ohc;

import java.util.function.Supplier;

import org.apache.accumulo.core.file.blockfile.cache.CacheEntry;
import org.apache.accumulo.core.file.blockfile.cache.SoftIndexCacheEntry;

public class OhcCacheEntry extends SoftIndexCacheEntry implements CacheEntry {

  private boolean allowIndex;

  OhcCacheEntry(byte[] buf, boolean allowIndex) {
    super(buf);
    this.allowIndex = allowIndex;
  }

  @Override
  public <T> T getIndex(Supplier<T> supplier) {
    if (allowIndex) {
      return super.getIndex(supplier);
    }

    return null;
  }
}
