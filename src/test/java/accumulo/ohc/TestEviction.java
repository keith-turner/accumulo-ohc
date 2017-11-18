package accumulo.ohc;

import static accumulo.ohc.OhcCacheConfiguration.OFF_HEAP_PREFIX;
import static accumulo.ohc.OhcCacheConfiguration.ON_HEAP_PREFIX;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.accumulo.core.file.blockfile.cache.CacheEntry;
import org.apache.accumulo.core.file.blockfile.cache.CacheEntry.Weighbable;
import org.apache.accumulo.core.file.blockfile.cache.CacheType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class TestEviction {

  private OhcBlockCache obc;

  static class TestIndex implements Weighbable {

    AtomicInteger counter = new AtomicInteger(0);

    @Override
    public int weight() {
      return 64;
    }
  }

  @Before
  public void setupCache() {
    TestConfiguration config = new TestConfiguration(CacheType.DATA, 5000000,
        ImmutableMap.of(OFF_HEAP_PREFIX + "capacity", "10000000", ON_HEAP_PREFIX + "maximumWeight", "1000000"));

    obc = new OhcBlockCache(new OhcCacheConfiguration(config, CacheType.DATA));

    Assert.assertEquals(1000000, obc.getMaxHeapSize());
    Assert.assertEquals(11000000, obc.getMaxSize());
  }

  @After
  public void tearDownCache() {
    obc.stop();
  }

  @Test
  public void testLoad() {

    Map<String,byte[]> blocks = new HashMap<>();
    Random rand = new Random();

    // put more in cache than can fit in on-heap cache, but not more than can fit in off-heap cache
    for (int i = 0; i < 500; i++) {
      String id = String.format("block-%06x", i);
      byte[] block = new byte[10_000];
      rand.nextBytes(block);

      obc.cacheBlock(id, block);
      blocks.put(id, block);
    }

    for (Entry<String,byte[]> entry : blocks.entrySet()) {
      CacheEntry ce = obc.getBlock(entry.getKey());
      Assert.assertNotNull(ce);
      Assert.assertArrayEquals(entry.getValue(), ce.getBuffer());
    }

    Assert.assertTrue(obc.getOnHeapStats().loadCount() > 0);
    Assert.assertTrue(obc.getOnHeapStats().loadFailureCount() == 0);
    Assert.assertTrue(obc.getOnHeapStats().evictionCount() > 0);
    Assert.assertTrue(obc.getOnHeapStats().hitCount() > 0);
    Assert.assertTrue(obc.getOnHeapStats().missCount() > 0);

    Assert.assertTrue(obc.getOffHeapStats().getEvictionCount() == 0);
    Assert.assertTrue(obc.getOffHeapStats().getHitCount() > 0);
    Assert.assertTrue(obc.getOffHeapStats().getMissCount() == 0);
  }

  private void accessFrequent(Set<String> frequent) {
    for (String fid : frequent) {
      CacheEntry ce = obc.getBlock(fid);
      Assert.assertNotNull(ce);
      TestIndex idx = ce.getIndex(() -> new TestIndex());
      idx.counter.incrementAndGet();
    }
  }

  @Test
  public void testFrequent() {
    // This test puts more than can fit on on-heap cache. While adding to the cache a few blocks are frequently accessed. These blocks should not move to
    // off-help.

    Map<String,byte[]> blocks = new HashMap<>();
    Random rand = new Random();

    Function<Integer,String> fmtFunc = i -> String.format("block-%06x", i);

    Set<String> frequent = Stream.of(2, 3, 5, 7, 11, 13, 19, 23, 29).map(fmtFunc).collect(Collectors.toSet());

    // put more in cache than can fit in on-heap cache, but not more than can fit in off-heap cache
    int fcount = 0;
    for (int i = 0; i < 500; i++) {
      String id = fmtFunc.apply(i);
      byte[] block = new byte[10_000];
      rand.nextBytes(block);

      obc.cacheBlock(id, block);
      blocks.put(id, block);

      if (i > 29 && i % 13 == 0) {
        accessFrequent(frequent);
        fcount++;
      }
    }

    for (String fid : frequent) {
      CacheEntry ce = obc.getBlock(fid);
      Assert.assertNotNull(ce);
      TestIndex idx = ce.getIndex(() -> null);
      Assert.assertNotNull(idx);
      Assert.assertEquals(fcount, idx.counter.get());
    }

    for (Entry<String,byte[]> entry : blocks.entrySet()) {
      CacheEntry ce = obc.getBlock(entry.getKey());
      Assert.assertNotNull(ce);
      Assert.assertArrayEquals(entry.getValue(), ce.getBuffer());
    }

    Assert.assertTrue(obc.getOnHeapStats().loadCount() > 0);
    Assert.assertTrue(obc.getOnHeapStats().loadFailureCount() == 0);
    Assert.assertTrue(obc.getOnHeapStats().evictionCount() > 0);
    Assert.assertTrue(obc.getOnHeapStats().hitCount() > 0);
    Assert.assertTrue(obc.getOnHeapStats().missCount() > 0);

    Assert.assertTrue(obc.getOffHeapStats().getEvictionCount() == 0);
    Assert.assertTrue(obc.getOffHeapStats().getHitCount() > 0);
    Assert.assertTrue(obc.getOffHeapStats().getMissCount() == 0);
  }
}
