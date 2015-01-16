export HBASE_CLASSPATH=`echo $HBASE_CLASSPATH | sed -e "s|$ZOOKEEPER_CONF:||"`
export HBASE_OPTS="-Xms268435456 -Xmx268435456 -XX:+HeapDumpOnOutOfMemoryError -XX:+UseConcMarkSweepGC -XX:-CMSConcurrentMTEnabled -XX:+CMSIncrementalMode $HBASE_OPTS"
