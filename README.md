# accumulo-ohc


```bash
mvn clean package dependency:copy-dependencies
cp target/*.jar target/dependency/*.jar $ACCUMULO_HOME/lib/ext
```

```
config -s tserver.cache.manager.class=accumulo.ohc.OhcCacheManager
config -s tserver.cache.ohc.data.on-heap.maximumWeight=1000000000
config -s tserver.cache.ohc.data.off-heap.capacity=10000000000
config -s tserver.cache.ohc.index.on-heap.maximumWeight=100000000
config -s tserver.cache.ohc.index.off-heap.capacity=1000000000
config -s tserver.cache.ohc.summary.on-heap.maximumWeight=100000000
config -s tserver.cache.ohc.summary.off-heap.capacity=10000000000
```

```xml
  <property>
    <name>tserver.cache.manager.class</name>
    <value>accumulo.ohc.OhcCacheManager</value>
  </property>
  <property>
    <name>tserver.cache.ohc.data.on-heap.maximumWeight</name>
    <value>1000000000</value>
  </property>
  <property>
    <name>tserver.cache.ohc.data.off-heap.capacity</name>
    <value>10000000000</value>
  </property>
  <property>
    <name>tserver.cache.ohc.index.on-heap.maximumWeight</name>
    <value>100000000</value>
  </property>
  <property>
    <name>tserver.cache.ohc.index.off-heap.capacity</name>
    <value>1000000000</value>
  </property>
  <property>
    <name>tserver.cache.ohc.summary.on-heap.maximumWeight</name>
    <value>100000000</value>
  </property>
  <property>
    <name>tserver.cache.ohc.summary.off-heap.capacity</name>
    <value>10000000000</value>
  </property>
```
