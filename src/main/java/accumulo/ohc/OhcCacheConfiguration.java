package accumulo.ohc;

import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.file.blockfile.cache.BlockCacheConfiguration;
import org.apache.accumulo.core.file.blockfile.cache.CacheType;

public class OhcCacheConfiguration extends BlockCacheConfiguration {

  public static final String PROPERTY_PREFIX = "ohc";

  public static final String ON_HEAP_PREFIX = ".on-heap.";
  public static final String OFF_HEAP_PREFIX = ".off-heap.";

  private final Map<String,String> offHeapProps;
  private final Map<String,String> onHeapProps;

  private static Map<String,String> getAllWithPrefix(Map<String,String> genProps, CacheType type, String prefixSuffix) {
    String defaultPrefix = getDefaultPrefix(PROPERTY_PREFIX) + prefixSuffix;
    String typePrefix = getPrefix(type, PROPERTY_PREFIX) + prefixSuffix;

    HashMap<String,String> props = new HashMap<>();

    genProps.forEach((k, v) -> {
      if (k.startsWith(defaultPrefix)) {
        props.put(k.substring(defaultPrefix.length(), k.length()), v);
      }
    });

    genProps.forEach((k, v) -> {
      if (k.startsWith(typePrefix)) {
        props.put(k.substring(typePrefix.length(), k.length()), v);
      }
    });

    return props;

  }

  // TODO it would be best if CacheManager API didn't use AccumuloConfiguration
  public OhcCacheConfiguration(AccumuloConfiguration conf, CacheType type) {
    super(conf, type, PROPERTY_PREFIX);

    Map<String,String> genProps = conf.getAllPropertiesWithPrefix(Property.GENERAL_ARBITRARY_PROP_PREFIX);

    offHeapProps = getAllWithPrefix(genProps, type, OFF_HEAP_PREFIX);
    onHeapProps = getAllWithPrefix(genProps, type, ON_HEAP_PREFIX);

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
}
