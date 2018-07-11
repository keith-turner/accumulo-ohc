package accumulo.ohc;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.accumulo.core.file.blockfile.cache.impl.ClassSize;
import org.apache.accumulo.core.spi.cache.BlockCache;
import org.apache.accumulo.core.spi.cache.CacheEntry;
import org.caffinitas.ohc.OHCache;
import org.caffinitas.ohc.OHCacheBuilder;
import org.caffinitas.ohc.OHCacheStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.CacheWriter;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.Weigher;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.google.common.annotations.VisibleForTesting;

public class OhcBlockCache implements BlockCache {

  private static final Logger LOG = LoggerFactory.getLogger(OhcBlockCache.class);

  private final LoadingCache<String,Block> onHeapCache;
  private final OHCache<String,byte[]> offHeapCache;

  private final long onHeapSize;

  OhcBlockCache(final OhcCacheConfiguration config) {

    config.getOffHeapProperties().forEach((k, v) -> {
      System.setProperty(OHCacheBuilder.SYSTEM_PROPERTY_PREFIX + k, v);
    });

    OHCacheBuilder<String,byte[]> builder = OHCacheBuilder.newBuilder();
    offHeapCache = builder.keySerializer(new StringSerializer()).valueSerializer(new BytesSerializer()).build();

    Map<String,String> onHeapProps = new HashMap<>(config.getOnHeapProperties());
    onHeapProps.computeIfAbsent("maximumWeight", k -> config.getMaxSize() + "");
    onHeapProps.computeIfAbsent("initialCapacity", k -> "" + (int) Math.ceil(1.2 * config.getMaxSize() / config.getBlockSize()));
    // Allow setting recordStats to true/false, a deviation from CaffineSpec. This is done because a default of 'on' is wanted, but want to allow user to
    // override.
    onHeapProps.compute("recordStats", (k, v) -> v == null || v.equals("") || v.equals("true") ? "" : null);

    this.onHeapSize = Long.valueOf(onHeapProps.get("maximumWeight"));

    String specification = onHeapProps.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).collect(Collectors.joining(","));

    Weigher<String,Block> weigher = (blockName, block) -> {
      int keyWeight = ClassSize.align(blockName.length()) + ClassSize.STRING;
      return keyWeight + block.weight();
    };
    onHeapCache = Caffeine.from(specification).weigher(weigher).writer(new CacheWriter<String,Block>() {
      @Override
      public void write(String key, Block value) {
        // don't write to the off-heap cache
      }

      @Override
      public void delete(String key, Block block, RemovalCause cause) {
        // this is called before the entry is actually removed from the cache
        if (cause.wasEvicted()) {
          LOG.trace("Block {} evicted from on-heap cache, putting to off-heap cache", key);
          offHeapCache.put(key, block.getBuffer());
        }
      }
    }).build(new CacheLoader<String,Block>() {
      @Override
      public Block load(String key) throws Exception {
        byte[] buffer = offHeapCache.get(key);
        if (buffer == null) {
          return null;
        }
        LOG.trace("Promoting block {} to on-heap cache", key);
        return new Block(buffer);
      }
    });
  }

  @Override
  public CacheEntry cacheBlock(String blockName, byte[] buf) {
    return wrap(blockName, onHeapCache.asMap().computeIfAbsent(blockName, k -> new Block(buf)));
  }

  @Override
  public CacheEntry getBlock(String blockName) {
    return wrap(blockName, onHeapCache.get(blockName));
  }

  private CacheEntry wrap(String cacheKey, Block block) {
    if (block != null) {
      return new TlfuCacheEntry(cacheKey, block);
    }

    return null;
  }

  private class TlfuCacheEntry implements CacheEntry {

    private final String cacheKey;
    private final Block block;

    TlfuCacheEntry(String k, Block b) {
      this.cacheKey = k;
      this.block = b;
    }

    @Override
    public byte[] getBuffer() {
      return block.getBuffer();
    }

    @Override
    public <T extends Weighable> T getIndex(Supplier<T> supplier) {
      return block.getIndex(supplier);
    }

    @Override
    public void indexWeightChanged() {
      if (block.indexWeightChanged()) {
        // update weight
        onHeapCache.put(cacheKey, block);
      }
    }
  }

  private Block load(String key, Loader loader, Map<String,byte[]> resolvedDeps) {

    byte[] data = offHeapCache.get(key);

    if (data == null) {
      data = loader.load((int) Math.min(Integer.MAX_VALUE, this.onHeapSize), resolvedDeps);
      if (data == null) {
        return null;
      }
    }
    return new Block(data);
  }

  private Map<String,byte[]> resolveDependencies(Map<String,Loader> deps) {
    if (deps.size() == 1) {
      Entry<String,Loader> entry = deps.entrySet().iterator().next();
      CacheEntry ce = getBlock(entry.getKey(), entry.getValue());
      if (ce == null) {
        return null;
      }
      return Collections.singletonMap(entry.getKey(), ce.getBuffer());
    } else {
      HashMap<String,byte[]> resolvedDeps = new HashMap<>();
      for (Entry<String,Loader> entry : deps.entrySet()) {
        CacheEntry ce = getBlock(entry.getKey(), entry.getValue());
        if (ce == null) {
          return null;
        }
        resolvedDeps.put(entry.getKey(), ce.getBuffer());
      }
      return resolvedDeps;
    }
  }

  @Override
  public CacheEntry getBlock(String blockName, Loader loader) {
    Map<String,Loader> deps = loader.getDependencies();
    Block block;
    if (deps.size() == 0) {
      block = onHeapCache.get(blockName, k -> load(blockName, loader, Collections.emptyMap()));
    } else {
      // This code path exist to handle the case where dependencies may need to be loaded. Loading dependencies will access the cache. Cache load functions
      // should not access the cache.
      block = onHeapCache.getIfPresent(blockName);

      if (block == null) {
        // Load dependencies outside of cache load function.
        Map<String,byte[]> resolvedDeps = resolveDependencies(deps);
        if (resolvedDeps == null) {
          return null;
        }

        // Use asMap because it will not increment stats, getIfPresent recorded a miss above. Use computeIfAbsent because it is possible another thread loaded
        // the data since this thread called getIfPresent.
        block = onHeapCache.asMap().computeIfAbsent(blockName, k -> load(blockName, loader, resolvedDeps));
      }
    }

    return wrap(blockName, block);
  }

  @Override
  public long getMaxSize() {
    return offHeapCache.capacity() + onHeapSize;
  }

  @Override
  public Stats getStats() {

    CacheStats stats = onHeapCache.stats();

    return new Stats() {

      @Override
      public long hitCount() {
        return stats.hitCount();
      }

      @Override
      public long requestCount() {
        return stats.requestCount();
      }

    };
  }

  public void stop() {
    onHeapCache.invalidateAll();
    onHeapCache.cleanUp();

    LOG.info("On Heap Cache Stats {}", onHeapCache.stats());
    LOG.info("Off Heap Cache Stats {}", offHeapCache.stats());

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

  @VisibleForTesting
  CacheStats getOnHeapStats() {
    return onHeapCache.stats();
  }

  @VisibleForTesting
  OHCacheStats getOffHeapStats() {
    return offHeapCache.stats();
  }
}
