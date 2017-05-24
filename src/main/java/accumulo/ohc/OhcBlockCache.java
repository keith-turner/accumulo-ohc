package accumulo.ohc;

import java.io.IOException;

import org.apache.accumulo.core.file.blockfile.cache.BlockCache;
import org.apache.accumulo.core.file.blockfile.cache.CacheEntry;
import org.caffinitas.ohc.Eviction;
import org.caffinitas.ohc.OHCache;
import org.caffinitas.ohc.OHCacheBuilder;
import org.caffinitas.ohc.OHCacheStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.stats.CacheStats;

public class OhcBlockCache implements BlockCache {
	
  private static final Logger LOG = LoggerFactory.getLogger(OhcBlockCache.class);

  private final Cache<String, CacheEntry> onHeapCache;
  private final OHCache<String,byte[]> offHeapCache;

  //TODO remove
  private int onHeapMiss;
  private int offHeapMiss;
  private int request;
  private final long onHeapSize;

  OhcBlockCache(final OhcCacheConfiguration config) {
	if (config.getMaxOffHeapSize() > 0) {
	  OHCacheBuilder<String, byte[]> builder = OHCacheBuilder.newBuilder();
	  offHeapCache = builder.capacity(config.getMaxOffHeapSize())
			  .segmentCount(128)
			  .keySerializer(new StringSerializer())
			  .valueSerializer(new BytesSerializer())
			  .eviction(Eviction.W_TINY_LFU)
			  .unlocked(true)
			  .build();
	} else {
	  offHeapCache = null;
	}
	this.onHeapSize = config.getMaxSize();
	onHeapCache = Caffeine.newBuilder()
			  .initialCapacity((int) Math.ceil(1.2 * config.getMaxSize() / config.getBlockSize()))
			  .expireAfterAccess(config.getOnHeapExpirationTime(), config.getOnHeapExpirationTimeUnit())
			  .removalListener((String key, CacheEntry block, RemovalCause cause) -> {
				  if (null != offHeapCache && cause.wasEvicted()) {
					LOG.info("Block {} evicted from on-heap cache, putting to off-heap cache", key);
					offHeapCache.put(key, block.getBuffer());
				  }
			  })
			  .maximumSize(config.getMaxSize())
			  .recordStats()
			  .build();
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
    request++;

    CacheEntry ce = onHeapCache.getIfPresent(blockName);
    if(ce != null) {
      return ce;
    }

    onHeapMiss++;

    if (null != this.offHeapCache) {
	    byte[] buffer = offHeapCache.get(blockName);
	    if(buffer == null) {
	      offHeapMiss++;
	      return null;
	    }
	
	    LOG.info("Promoting block {} to on-heap cache", blockName);
	    ce = new OhcCacheEntry(buffer, true);
	    onHeapCache.put(blockName, ce);
    }

    return ce;
  }

  @Override
  public long getMaxSize() {
	if (null == this.offHeapCache) {
	  return 0;
	}
    return offHeapCache.capacity();
  }

  @Override
  public Stats getStats() {

    CacheStats heapStats = onHeapCache.stats();
    OHCacheStats ohcStats = offHeapCache.stats();

    return new Stats(){

      @Override
      public long hitCount() {
        //TODO
        return ohcStats.getHitCount() + heapStats.hitCount();
      }

      @Override
      public long requestCount() {
        //TODO
        return ohcStats.getHitCount() + ohcStats.getMissCount() + heapStats.requestCount();
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
    System.out.printf("onMiss:  %,d offMiss: %,d request: %,d\n", onHeapMiss, offHeapMiss, request);
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
