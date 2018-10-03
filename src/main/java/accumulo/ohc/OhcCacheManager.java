package accumulo.ohc;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.spi.cache.BlockCache;
import org.apache.accumulo.core.spi.cache.BlockCacheManager;
import org.apache.accumulo.core.spi.cache.CacheType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OhcCacheManager extends BlockCacheManager {

  private static final Logger LOG = LoggerFactory.getLogger(OhcCacheManager.class);

  private Timer timer = null;

  @Override
  public void start(Configuration conf) {
    super.start(conf);

    for (CacheType type : CacheType.values()) {
      OhcCacheConfiguration cc = new OhcCacheConfiguration(conf, type);
      Long interval = cc.getLogInterval(TimeUnit.MILLISECONDS);
      if(interval != null) {
        if(timer == null) {
          timer = new Timer(true);
        }

        OhcBlockCache obc = (OhcBlockCache) getBlockCache(type);
        TimerTask task =new TimerTask() {
          @Override
          public void run() {
            obc.logStats();
          }
        };

        timer.scheduleAtFixedRate(task, interval, interval);
      }
    }
  }

  @Override
  protected BlockCache createCache(Configuration conf, CacheType type) {
    OhcCacheConfiguration cc = new OhcCacheConfiguration(conf, type);
    LOG.info("Creating {} cache with configuration {}", type, cc);
    return new OhcBlockCache(cc);
  }

  @Override
  public void stop() {
    if(timer != null) {
      timer.cancel();
    }

    for (CacheType type : CacheType.values()) {
      OhcBlockCache obc = (OhcBlockCache) getBlockCache(type);
      obc.stop();
    }

    super.stop();
  }

}
