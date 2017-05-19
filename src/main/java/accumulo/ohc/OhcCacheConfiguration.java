package accumulo.ohc;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.file.blockfile.cache.BlockCacheConfiguration;
import org.apache.accumulo.core.file.blockfile.cache.CacheType;

public class OhcCacheConfiguration extends BlockCacheConfiguration {

  public OhcCacheConfiguration(AccumuloConfiguration conf, CacheType type) {
    super(conf, type, "ohc");
  }

}
