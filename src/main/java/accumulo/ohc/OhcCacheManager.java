package accumulo.ohc;

import org.apache.accumulo.core.file.blockfile.cache.BlockCache;
import org.apache.accumulo.core.file.blockfile.cache.BlockCacheManager;
import org.apache.accumulo.core.file.blockfile.cache.CacheType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OhcCacheManager extends BlockCacheManager {

  private static final Logger LOG = LoggerFactory.getLogger(OhcCacheManager.class);

  @Override
  protected BlockCache createCache(Configuration conf, CacheType type) {
    OhcCacheConfiguration cc = new OhcCacheConfiguration(conf, type);
    LOG.info("Creating {} cache with configuration {}", type, cc);
    return new OhcBlockCache(cc);
  }

  @Override
  public void stop() {
    for (CacheType type : CacheType.values()) {
      OhcBlockCache obc = (OhcBlockCache) getBlockCache(type);
      obc.stop();
    }

    super.stop();
  }

}
