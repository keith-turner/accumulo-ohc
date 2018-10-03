# accumulo-ohc

Accumulo-OHC is a custom cache implementation for Accumulo that builds on
[Caffeine][1] and [OHC][2].  Caffeine is used for caching frequently accessed
blocks on the Java heap.  OHC is used for caching less frequently accessed
blocks in a larger off Java heap cache.  The reason to do this to avoid the
Java garbage collector.  This cache has been test with 1 billion key values all
stored in a 4GB on-heap + 40GB off-heap cache.

Accumulo-OHC builds uses a new caching SPI introduced in Accumulo
2.0.0-SNAPSHOT.  

Below is an example of how to build this project and place the needed jars in
Accumulo's `lib/ext` directory.  This would need to be done on each tablet
server.

```bash
mvn clean package dependency:copy-dependencies
cp target/*.jar target/dependency/*.jar $ACCUMULO_HOME/lib/ext
```

Below is example of configuring this cache in the Accumulo shell.  After doing
this, would need to restart tablet servers.

```
config -s tserver.cache.manager.class=accumulo.ohc.OhcCacheManager
config -s tserver.cache.config.ohc.default.logInterval=30
config -s tserver.cache.config.ohc.data.on-heap.maximumWeight=1000000000
config -s tserver.cache.config.ohc.data.off-heap.capacity=10000000000
config -s tserver.cache.config.ohc.index.on-heap.maximumWeight=100000000
config -s tserver.cache.config.ohc.index.off-heap.capacity=1000000000
config -s tserver.cache.config.ohc.summary.on-heap.maximumWeight=100000000
config -s tserver.cache.config.ohc.summary.off-heap.capacity=10000000000
```

An alternative to configuring this cache in the shell is setting properties in
`accumulo.properties`.  Below is an example of that.

```
tserver.cache.manager.class=accumulo.ohc.OhcCacheManager
tserver.cache.config.ohc.default.logInterval=30
tserver.cache.config.ohc.data.on-heap.maximumWeight=1000000000
tserver.cache.config.ohc.data.off-heap.capacity=10000000000
tserver.cache.config.ohc.index.on-heap.maximumWeight=100000000
tserver.cache.config.ohc.index.off-heap.capacity=1000000000
tserver.cache.config.ohc.summary.on-heap.maximumWeight=100000000
tserver.cache.config.ohc.summary.off-heap.capacity=10000000000
```

This cache uses a pass through strategy for configuration, properties are
passed through to Caffeine and OHC.  Properties are of the form 

```
tserver.cache.config.ohc.[default|data|index|summary].[off-heap|on-heap].<pass through prop>
```  

The options `default|data|index|summary` determines the Accumulo cache
instance. The options `off-heap|on-heap` determines if the `pass through prop`
goes to Caffeine or OHC.


For example, the following property

```
tserver.cache.config.ohc.data.on-heap.maximumWeight=1000000000
```
 will pass `maximumWeight=1000000000` to Caffeine for the Accumulo data cache.  

As another example,the following property  
```
tserver.cache.config.ohc.data.off-heap.capacity=10000000000
```

 will pass `capacity=10000000000` to OHC. 

For documentation on what properties each cache implementation accepts see
[OHCCacheBuilder][3] and [CaffeineSpec][4].

Additionally a property can be set to periodcally log stats for each cache.
The value for this is seconds. By default no logging is done.  The property is
of the form :

```
tserver.cache.config.ohc.[default|data|index|summary].logInterval=<seconds>
```


[1]: https://github.com/ben-manes/caffeine
[2]: https://github.com/snazy/ohc
[3]: https://static.javadoc.io/org.caffinitas.ohc/ohc-core/0.7.0/org/caffinitas/ohc/OHCacheBuilder.html
[4]: https://static.javadoc.io/com.github.ben-manes.caffeine/caffeine/2.6.2/com/github/benmanes/caffeine/cache/CaffeineSpec.html
