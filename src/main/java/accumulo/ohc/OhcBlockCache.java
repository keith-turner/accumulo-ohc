package accumulo.ohc;

import java.io.IOException;
import java.util.concurrent.atomic.LongAdder;

import org.apache.accumulo.core.file.blockfile.cache.BlockCache;
import org.apache.accumulo.core.file.blockfile.cache.CacheEntry;
import org.caffinitas.ohc.OHCache;
import org.caffinitas.ohc.OHCacheBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.CacheWriter;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;

public class OhcBlockCache implements BlockCache {

  private static final Logger LOG = LoggerFactory.getLogger(OhcBlockCache.class);

  private final Cache<String,CacheEntry> onHeapCache;
  private final OHCache<String,byte[]> offHeapCache;

  private LongAdder misses;
  private LongAdder onHeapHits;
  private LongAdder offHeapHits;

  private final long onHeapSize;

  OhcBlockCache(final OhcCacheConfiguration config) {

    misses = new LongAdder();
    onHeapHits = new LongAdder();
    offHeapHits = new LongAdder();

    config.getOffHeapProperties().forEach((k, v) -> {
      System.setProperty(OHCacheBuilder.SYSTEM_PROPERTY_PREFIX + k, v);
    });

    OHCacheBuilder<String,byte[]> builder = OHCacheBuilder.newBuilder();
    offHeapCache = builder.keySerializer(new StringSerializer()).valueSerializer(new BytesSerializer()).build();

    this.onHeapSize = config.getMaxSize();
    onHeapCache = Caffeine.newBuilder().initialCapacity((int) Math.ceil(1.2 * config.getMaxSize() / config.getBlockSize()))
        .expireAfterAccess(config.getOnHeapExpirationTime(), config.getOnHeapExpirationTimeUnit()).maximumSize(config.getMaxSize())
        .writer(new CacheWriter<String,CacheEntry>() {
          @Override
          public void write(String key, CacheEntry value) {}

          @Override
          public void delete(String key, CacheEntry block, RemovalCause cause) {
            // this is called before the entry is actually removed from the cache
            if (cause.wasEvicted()) {
              offHeapCache.put(key, block.getBuffer());
            }
          }
        }).build();
  }

  @Override
  public CacheEntry cacheBlock(String blockName, byte[] buf, boolean inMemory) {
    OhcCacheEntry ce = new OhcCacheEntry(buf, true);
    onHeapCache.put(blockName, ce);
    return ce;
  }

  @Override
  public CacheEntry cacheBlock(String blockName, byte[] buf) {
    return cacheBlock(blockName, buf, false);
  }

  @Override
  public CacheEntry getBlock(String blockName) {

    CacheEntry ce = onHeapCache.getIfPresent(blockName);
    if (ce != null) {
      onHeapHits.increment();
      return ce;
    }

    byte[] buffer = offHeapCache.get(blockName);
    if (buffer == null) {
      misses.increment();
      return null;
    }

    LOG.trace("Promoting block {} to on-heap cache", blockName);
    ce = new OhcCacheEntry(buffer, true);
    onHeapCache.put(blockName, ce);

    offHeapHits.increment();
    return ce;
  }

  @Override
  public long getMaxSize() {
    return offHeapCache.capacity() + onHeapSize;
  }

  @Override
  public Stats getStats() {

    long onHits = onHeapHits.sum();
    long offHits = offHeapHits.sum();
    long misses = this.misses.sum();

    return new Stats() {

      @Override
      public long hitCount() {
        return onHits + offHits;
      }

      @Override
      public long requestCount() {
        return onHits + offHits + misses;
      }

    };
  }

  public void stop() {
    onHeapCache.invalidateAll();
    onHeapCache.cleanUp();
    System.out.println(onHeapCache.stats());
    if (null != this.offHeapCache) {
      System.out.println(offHeapCache.stats());
    }

    long onHits = onHeapHits.sum();
    long offHits = offHeapHits.sum();
    long misses = this.misses.sum();

    LOG.trace("onHits:  %,d offHits: %,d request: %,d\n", onHits, offHits, onHits + offHits + misses);
    if (null != this.offHeapCache) {
      try {
        offHeapCache.close();
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }

  @Override
  public long getMaxHeapSize() {
    return this.onHeapSize;
  }

}
