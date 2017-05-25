package accumulo.ohc;

import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.file.blockfile.cache.BlockCacheConfiguration;
import org.apache.accumulo.core.file.blockfile.cache.CacheType;

public class OhcCacheConfiguration extends BlockCacheConfiguration {

  public static final String PROPERTY_PREFIX = "ohc";
  public static final String OFF_HEAP_CACHE_SIZE = "off-heap.size";
  public static final String ON_HEAP_EXPIRATION_TIME = "on-heap.expiration.time";
  public static final String ON_HEAP_EXPIRATION_TIME_UNITS = "on-heap.expiration.time.units";
  public static final String OFF_HEAP_EXPIRATION_TIME = "off-heap.expiration.time";
  public static final String OFF_HEAP_EXPIRATION_TIME_UNITS = "off-heap.expiration.time.units";

  private static final long DEFAULT_OFF_HEAP_CACHE_SIZE = 0;
  private static final int DEFAULT_ON_HEAP_CACHE_TIME = 1;
  private static final TimeUnit DEFAULT_ON_HEAP_CACHE_TIME_UNIT = TimeUnit.HOURS;
  private static final int DEFAULT_OFF_HEAP_CACHE_TIME = 1;
  private static final TimeUnit DEFAULT_OFF_HEAP_CACHE_TIME_UNIT = TimeUnit.HOURS;
  
  private final long maxOffHeapSize;
  private final int onHeapExpirationTime;
  private final TimeUnit onHeapExpirationTimeUnit;
  private final int offHeapExpirationTime;
  private final TimeUnit offHeapExpirationTimeUnit;
  
  
  public OhcCacheConfiguration(AccumuloConfiguration conf, CacheType type) {
    super(conf, type, PROPERTY_PREFIX);
    
    this.maxOffHeapSize = get(OFF_HEAP_CACHE_SIZE).map(Long::valueOf).filter(f -> f > 0).orElse(DEFAULT_OFF_HEAP_CACHE_SIZE);
    this.onHeapExpirationTime = get(ON_HEAP_EXPIRATION_TIME).map(Integer::valueOf).filter(f -> f > 0).orElse(DEFAULT_ON_HEAP_CACHE_TIME);
    this.onHeapExpirationTimeUnit = get(ON_HEAP_EXPIRATION_TIME_UNITS).map(TimeUnit::valueOf).filter(f -> f != null).orElse(DEFAULT_ON_HEAP_CACHE_TIME_UNIT);
    this.offHeapExpirationTime = get(OFF_HEAP_EXPIRATION_TIME).map(Integer::valueOf).filter(f -> f > 0).orElse(DEFAULT_OFF_HEAP_CACHE_TIME);
    this.offHeapExpirationTimeUnit = get(OFF_HEAP_EXPIRATION_TIME_UNITS).map(TimeUnit::valueOf).filter(f -> f != null).orElse(DEFAULT_OFF_HEAP_CACHE_TIME_UNIT);
  }

  public long getMaxOffHeapSize() {
	return maxOffHeapSize;
  }
	
  public int getOnHeapExpirationTime() {
	return onHeapExpirationTime;
  }

  public TimeUnit getOnHeapExpirationTimeUnit() {
	return onHeapExpirationTimeUnit;
  }

  public int getOffHeapExpirationTime() {
	return offHeapExpirationTime;
  }
	
  public TimeUnit getOffHeapExpirationTimeUnit() {
    return offHeapExpirationTimeUnit;
  }

  @Override
  public String toString() {
	return super.toString()
			+ ", offHeapEnabled: " + (0 != this.maxOffHeapSize)
			+ ", maxOffHeapSize: " + this.maxOffHeapSize
			+ ", onHeapExpirationTime: " + this.onHeapExpirationTime
			+ ", onHeapExpriationTimeUnits " + this.onHeapExpirationTimeUnit
			+ ", offHeapExpirationTime: " + this.offHeapExpirationTime
			+ ", offHeapExpirationTimeUnits: " + this.offHeapExpirationTimeUnit;
}

}
