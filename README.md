# accumulo-ohc


```bash
mvn clean package dependency:copy-dependencies
cp target/*.jar target/dependency/*.jar $ACCUMULO_HOME/lib/ext
```

```xml
  <property>
    <name>tserver.cache.manager.class</name>
    <value>accumulo.ohc.OhcCacheManager</value>
  </property>
  <property>
    <name>general.custom.cache.ohc.data.on-heap.maximumWeight</name>
    <value>1000000000</value>
  </property>
  <property>
    <name>general.custom.cache.ohc.data.off-heap.capacity</name>
    <value>10000000000</value>
  </property>
  <property>
    <name>general.custom.cache.ohc.index.on-heap.maximumWeight</name>
    <value>100000000</value>
  </property>
  <property>
    <name>general.custom.cache.ohc.index.off-heap.capacity</name>
    <value>1000000000</value>
  </property>
  <property>
    <name>general.custom.cache.ohc.summary.on-heap.maximumWeight</name>
    <value>100000000</value>
  </property>
  <property>
    <name>general.custom.cache.ohc.summary.off-heap.capacity</name>
    <value>10000000000</value>
  </property>
```
