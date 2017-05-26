package accumulo.ohc;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.file.blockfile.cache.BlockCacheManager.Configuration;
import org.apache.accumulo.core.file.blockfile.cache.CacheType;

public class OhcCacheConfiguration {

  public static final String PROPERTY_PREFIX = "ohc";

  public static final String ON_HEAP_PREFIX = "on-heap.";
  public static final String OFF_HEAP_PREFIX = "off-heap.";

  private final Map<String,String> offHeapProps;
  private final Map<String,String> onHeapProps;

  private final long maxSize;
  private final long blockSize;

  // TODO it would be best if CacheManager API didn't use AccumuloConfiguration
  public OhcCacheConfiguration(Configuration conf, CacheType type) {

    Map<String,String> allProps = conf.getProperties(PROPERTY_PREFIX, type);

    Map<String,String> onHeapProps = new HashMap<>();
    Map<String,String> offHeapProps = new HashMap<>();

    allProps.forEach((k, v) -> {
      if (k.startsWith(ON_HEAP_PREFIX)) {
        onHeapProps.put(k.substring(ON_HEAP_PREFIX.length()), v);
      } else if (k.startsWith(OFF_HEAP_PREFIX)) {
        offHeapProps.put(k.substring(OFF_HEAP_PREFIX.length()), v);
      }
    });

    this.offHeapProps = Collections.unmodifiableMap(offHeapProps);
    this.onHeapProps = Collections.unmodifiableMap(onHeapProps);

    this.maxSize = conf.getMaxSize(type);
    this.blockSize = conf.getBlockSize();
  }

  public Map<String,String> getOffHeapProperties() {
    return offHeapProps;
  }

  public Map<String,String> getOnHeapProperties() {
    return onHeapProps;
  }

  @Override
  public String toString() {
    return super.toString() + "OnHeapProps:" + onHeapProps + "  offHeapProps:" + offHeapProps;
  }

  public long getMaxSize() {
    return maxSize;
  }

  public long getBlockSize() {
    return blockSize;
  }
}
