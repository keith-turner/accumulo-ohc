package accumulo.ohc;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.file.blockfile.cache.BlockCacheConfiguration;
import org.apache.accumulo.core.file.blockfile.cache.CacheType;

public class OhcCacheConfiguration extends BlockCacheConfiguration {

  public static final String PROPERTY_PREFIX = "ohc";
  public static final String ON_HEAP_EXPIRATION_TIME = "on-heap.expiration.time";
  public static final String ON_HEAP_EXPIRATION_TIME_UNITS = "on-heap.expiration.time.units";

  public static final String OFF_HEAP_PREFIX = ".off-heap.";

  private static final int DEFAULT_ON_HEAP_CACHE_TIME = 1;
  private static final TimeUnit DEFAULT_ON_HEAP_CACHE_TIME_UNIT = TimeUnit.HOURS;

  private final int onHeapExpirationTime;
  private final TimeUnit onHeapExpirationTimeUnit;
  private final HashMap<String,String> ohcProps;

  // TODO it would be best if CacheManager API didn't use AccumuloConfiguration
  public OhcCacheConfiguration(AccumuloConfiguration conf, CacheType type) {
    super(conf, type, PROPERTY_PREFIX);

    this.onHeapExpirationTime = get(ON_HEAP_EXPIRATION_TIME).map(Integer::valueOf).filter(f -> f > 0).orElse(DEFAULT_ON_HEAP_CACHE_TIME);
    this.onHeapExpirationTimeUnit = get(ON_HEAP_EXPIRATION_TIME_UNITS).map(TimeUnit::valueOf).filter(f -> f != null).orElse(DEFAULT_ON_HEAP_CACHE_TIME_UNIT);

    String defaultOhp = getDefaultPrefix(PROPERTY_PREFIX) + OFF_HEAP_PREFIX;
    String ctOhp = getPrefix(type, PROPERTY_PREFIX) + OFF_HEAP_PREFIX;

    ohcProps = new HashMap<>();

    Map<String,String> genProps = conf.getAllPropertiesWithPrefix(Property.GENERAL_ARBITRARY_PROP_PREFIX);

    genProps.forEach((k, v) -> {
      if (k.startsWith(defaultOhp)) {
        ohcProps.put(k.substring(defaultOhp.length(), k.length()), v);
      }
    });

    genProps.forEach((k, v) -> {
      if (k.startsWith(ctOhp)) {
        ohcProps.put(k.substring(ctOhp.length(), k.length()), v);
      }
    });
  }

  public int getOnHeapExpirationTime() {
    return onHeapExpirationTime;
  }

  public TimeUnit getOnHeapExpirationTimeUnit() {
    return onHeapExpirationTimeUnit;
  }

  public Map<String,String> getOffHeapProperties() {
    return ohcProps;
  }

  @Override
  public String toString() {
    return super.toString() + "onHeapExpirationTime: " + this.onHeapExpirationTime + ", onHeapExpriationTimeUnits " + this.onHeapExpirationTimeUnit;
  }
}
