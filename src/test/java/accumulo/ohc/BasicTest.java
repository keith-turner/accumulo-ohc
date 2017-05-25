package accumulo.ohc;

import static accumulo.ohc.OhcCacheConfiguration.OFF_HEAP_PREFIX;
import static accumulo.ohc.OhcCacheConfiguration.PROPERTY_PREFIX;

import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.file.blockfile.cache.BlockCacheConfiguration;
import org.apache.accumulo.core.file.blockfile.cache.CacheType;
import org.junit.Test;

public class BasicTest {
  @Test
  public void testConfig(){
    ConfigurationCopy cc = new ConfigurationCopy();
    DefaultConfiguration.getInstance().forEach(e -> cc.set(e.getKey(), e.getValue()));

    String prefix = BlockCacheConfiguration.getPrefix(CacheType.INDEX, PROPERTY_PREFIX);

    cc.set(prefix+OFF_HEAP_PREFIX+"capacity", "400000000");
    cc.set("tserver.cache.index.size", "4000000");

    OhcCacheConfiguration config = new OhcCacheConfiguration(cc, CacheType.INDEX);
    OhcBlockCache obc = new OhcBlockCache(config);

    System.out.println(obc.getMaxHeapSize());
    System.out.println(obc.getMaxSize());

    obc.stop();
  }
}
