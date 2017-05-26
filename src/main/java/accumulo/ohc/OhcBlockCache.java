package accumulo.ohc;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.LongAdder;

import org.apache.accumulo.core.file.blockfile.cache.BlockCache;
import org.apache.accumulo.core.file.blockfile.cache.CacheEntry;
import org.caffinitas.ohc.OHCache;
import org.caffinitas.ohc.OHCacheBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.CacheWriter;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;

public class OhcBlockCache implements BlockCache {

  private static final Logger LOG = LoggerFactory.getLogger(OhcBlockCache.class);

  private final Cache<String,CacheEntry> onHeapCache;
  private final OHCache<String,byte[]> offHeapCache;

  private LongAdder misses;
  private LongAdder hits;

  private final long onHeapSize;

  OhcBlockCache(final OhcCacheConfiguration config) {

    // TODO are the redundant? will they always be the same as onheap stats?
    misses = new LongAdder();
    hits = new LongAdder();

    config.getOffHeapProperties().forEach((k, v) -> {
      System.setProperty(OHCacheBuilder.SYSTEM_PROPERTY_PREFIX + k, v);
    });

    OHCacheBuilder<String,byte[]> builder = OHCacheBuilder.newBuilder();
    offHeapCache = builder.keySerializer(new StringSerializer()).valueSerializer(new BytesSerializer()).build();

    this.onHeapSize = config.getMaxSize();
    onHeapCache = Caffeine.newBuilder().initialCapacity((int) Math.ceil(1.2 * config.getMaxSize() / config.getBlockSize()))
        .expireAfterAccess(config.getOnHeapExpirationTime(), config.getOnHeapExpirationTimeUnit()).maximumSize(config.getMaxSize()).recordStats()
        .writer(new CacheWriter<String,CacheEntry>() {
          @Override
          public void write(String key, CacheEntry value) {
            // don't write to the off-heap cache
          }

          @Override
          public void delete(String key, CacheEntry block, RemovalCause cause) {
            // this is called before the entry is actually removed from the cache
            if (cause.wasEvicted()) {
              LOG.trace("Block {} evicted from on-heap cache, putting to off-heap cache", key);
              offHeapCache.put(key, block.getBuffer());
            }
          }
        }).build(new CacheLoader<String,CacheEntry>() {
          @Override
          public CompletableFuture<CacheEntry> asyncLoad(String arg0, Executor arg1) {
            return null;
          }

          @Override
          public CacheEntry load(String key) throws Exception {
            byte[] buffer = offHeapCache.get(key);
            if (buffer == null) {
              return null;
            }
            LOG.trace("Promoting block {} to on-heap cache", key);
            return new OhcCacheEntry(buffer, true);
          }
        });
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
      hits.increment();
      return ce;
    }

    misses.increment();
    return ce;
  }

  @Override
  public long getMaxSize() {
    return offHeapCache.capacity() + onHeapSize;
  }

  @Override
  public Stats getStats() {

    long hits = this.hits.sum();
    long misses = this.misses.sum();

    return new Stats() {

      @Override
      public long hitCount() {
        return hits;
      }

      @Override
      public long requestCount() {
        return hits + misses;
      }

    };
  }

  public void stop() {
    onHeapCache.invalidateAll();
    onHeapCache.cleanUp();
    System.out.println(onHeapCache.stats());
    System.out.println(offHeapCache.stats());

    LOG.trace("hits:  %,d misses : %,d\n", hits.sum(), misses.sum());

    try {
      offHeapCache.close();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  @Override
  public long getMaxHeapSize() {
    return this.onHeapSize;
  }

}
