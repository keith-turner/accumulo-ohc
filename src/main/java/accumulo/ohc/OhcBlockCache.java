package accumulo.ohc;

import java.io.IOException;

import org.apache.accumulo.core.file.blockfile.cache.BlockCache;
import org.apache.accumulo.core.file.blockfile.cache.CacheEntry;
import org.apache.accumulo.core.file.blockfile.cache.CacheType;
import org.caffinitas.ohc.OHCache;
import org.caffinitas.ohc.OHCacheBuilder;
import org.caffinitas.ohc.OHCacheStats;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Weigher;
import com.github.benmanes.caffeine.cache.stats.CacheStats;

public class OhcBlockCache implements BlockCache {

  private Cache<String, CacheEntry> onHeapCache;
  private OHCache<String,byte[]> cache;
  private CacheType type;

  //TODO remove
  private int onHeapMiss;
  private int offHeapMiss;
  private int request;



  OhcBlockCache(long size, CacheType type){
    this.type = type;
    OHCacheBuilder<String, byte[]> builder = OHCacheBuilder.newBuilder();
    cache = builder.keySerializer(new StringSerializer())
        .valueSerializer(new BytesSerializer())
        .capacity(size)
        .build();

    Weigher<String,CacheEntry> weigher = (k,v) -> v.getBuffer().length + k.length();
    onHeapCache = Caffeine.newBuilder().maximumWeight(size/10).weigher(weigher).recordStats().build();
  }

  @Override
  public CacheEntry cacheBlock(String blockName, byte[] buf, boolean inMemory) {
    OhcCacheEntry ce = new OhcCacheEntry(buf, true);
    onHeapCache.put(blockName, ce);
    cache.putIfAbsent(blockName, buf);
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

    byte[] buffer = cache.get(blockName);
    if(buffer == null) {
      offHeapMiss++;
      return null;
    }

    ce = new OhcCacheEntry(buffer, true);
    onHeapCache.put(blockName, ce);

    return ce;
  }

  @Override
  public long getMaxSize() {
    return cache.capacity();
  }

  @Override
  public Stats getStats() {

    CacheStats heapStats = onHeapCache.stats();
    OHCacheStats ohcStats = cache.stats();

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
    System.out.println(cache.stats());
    System.out.printf("onMiss:  %,d offMiss: %,d request: %,d\n", onHeapMiss, offHeapMiss, request);
    try {
      cache.close();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

}
