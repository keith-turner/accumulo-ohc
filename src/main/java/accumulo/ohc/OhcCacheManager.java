package accumulo.ohc;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.file.blockfile.cache.BlockCache;
import org.apache.accumulo.core.file.blockfile.cache.BlockCacheManager;
import org.apache.accumulo.core.file.blockfile.cache.CacheType;

public class OhcCacheManager extends BlockCacheManager {

  @Override
  protected BlockCache createCache(AccumuloConfiguration conf, CacheType type) {
    OhcCacheConfiguration config = new OhcCacheConfiguration(conf, type);
    return new OhcBlockCache(config.getMaxSize(), type);
  }

  @Override
  public void stop(){
    for (CacheType type : CacheType.values()) {
      OhcBlockCache obc = (OhcBlockCache)getBlockCache(type);
      System.out.println("stopping "+type);
      obc.stop();
    }

    super.stop();
  }

}
