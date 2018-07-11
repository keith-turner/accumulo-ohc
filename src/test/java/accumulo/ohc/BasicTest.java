package accumulo.ohc;

import static accumulo.ohc.OhcCacheConfiguration.OFF_HEAP_PREFIX;
import static accumulo.ohc.OhcCacheConfiguration.ON_HEAP_PREFIX;

import org.apache.accumulo.core.spi.cache.CacheType;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class BasicTest {

  @Test
  public void testConfig() {

    TestConfiguration config = new TestConfiguration(CacheType.INDEX, 4000000, ImmutableMap.of(OFF_HEAP_PREFIX + "capacity", "400000000"));

    OhcBlockCache obc = new OhcBlockCache(new OhcCacheConfiguration(config, CacheType.INDEX));

    Assert.assertEquals(4000000, obc.getMaxHeapSize());
    Assert.assertEquals(404000000, obc.getMaxSize());

    obc.stop();
  }

  @Test
  public void testCaffineConfig() {

    TestConfiguration config = new TestConfiguration(CacheType.INDEX, 5000000,
        ImmutableMap.of(OFF_HEAP_PREFIX + "capacity", "400000000", ON_HEAP_PREFIX + "maximumWeight", "4000000"));
    OhcBlockCache obc = new OhcBlockCache(new OhcCacheConfiguration(config, CacheType.INDEX));

    Assert.assertEquals(4000000, obc.getMaxHeapSize());
    Assert.assertEquals(404000000, obc.getMaxSize());

    obc.stop();
  }
}
