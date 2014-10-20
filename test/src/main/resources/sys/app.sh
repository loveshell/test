最后附上几种修改缓存时间的方法：
1.	jvm启动参数里面配置-Dsun.net.inetaddr.ttl=value
2.	修改 配置文件JAVA_HOME/jre/lib/security/java.security相应的参数networkaddress.cache.ttl=value
3.	代码里直接设置：java.security.Security.setProperty(”networkaddress.cache.ttl” , “value”);
java.security.Security.setProperty("networkaddress.cache.ttl", "-1");

1.静态内部类跟静态方法一样，只能访问静态的成员变量和方法，不能访问非静态的方法和属性，但是普通内部类可以访问任意外部类的成员变量和方法
hbase:    split 'regionName', 'splitKey'  
split 'crawldb1,0,1409282923319.7b4d78091f202c8a2d24048fbac9fe6f.', '0a'
count 'crawldb1', INTERVAL =>10000
scan 'crawldb1', {COLUMNS => ['cf1'], LIMIT => 1000, STARTROW => '0', STOPROW => '1'}
    
hdfs:snn.stop, replication=2
hbase:memory
mapreduce:memory, map.tasks, reduce.tasks
nutch:total=2400000;topn=80000;hostmax=5000,hostThreads=30,totalThreads=150, map=4

#nutch中http请求超时
socket.setSoTimeout(http.getTimeout());
this.timeout = conf.getInt("http.timeout", 10000);

-d64 -Xms2g -Xmx5g -XX:MaxPermSize=512M -XX:+UseParallelGC -XX:-UseGCOverheadLimit -Dfile.encoding=UTF-8 -Duser.language=zh -Duser.region=CN 
JAVA_OPTS='-server -d64 -Xms2g -Xmx20g -XX:PermSize=1g -XX:MaxPermSize=4g -XX:-UseGCOverheadLimit'
这个是JDK6新添的错误类型。是发生在GC占用大量时间为释放很小空间的时候发生的，是一种保护机制。解决方案是，关闭该功能，使用—— -XX:-UseGCOverheadLimit

scp hbase.tar.gz imsrvtest1:/data/
ssh imsrvtest1 "cd /data;tar -zxf /data/hbase.tar.gz"
scp /data/nutch_new/hbasetest.jar imsrvtest1:/data/hbase-0.98.3-hadoop1/lib/;scp /data/nutch_new/hbasetest.jar imsrvtest2:/data/hbase-0.98.3-hadoop1/lib/;scp /data/nutch_new/hbasetest.jar skytest2:/data/hbase-0.98.3-hadoop1/lib/;scp /data/nutch_new/hbasetest.jar skytest1:/data/hbase-0.98.3-hadoop1/lib/;
/data/hbase-0.98.3-hadoop1/bin/stop-hbase.sh
/data/hbase-0.98.3-hadoop1/bin/start-hbase.sh 
./hbase-daemon.sh start regionserver
hadoop fs -rm -r -skipTrash /data
hadoop fs -ls -R /hbase
mapred job -kill job_201408061337_0069
hadoop conf对象中中文乱码

#map输出的收集
org.apache.hadoop.mapred.MapTask: createSortingCollector ==> MapOutputBuffer.class 
runner.run(in, new OldOutputCollector(collector, conf), reporter); 
collector.flush();

# ScannerTimeoutException when a scan enables caching
 I have some code that does a scan over a table, and for each row returned some work to verify the data...
I set the scan up like so :
  byte[] family = Bytes.toBytes("mytable");
  Scan scan = new Scan();
  scan.setCaching(2000);
  scan.addFamily(family);
and then scan using a fairly normal looking loop:
  ResultScanner scanner = table.getScanner(scan);
  for (Result userInfoResult : scanner) {
      // do some work that takes about half a second
  }
After this code runs for 60 seconds, I get the exception below:Exception in thread "main" java.lang.RuntimeException:
org.apache.hadoop.hbase.client.ScannerTimeoutException: 78850ms passed since the last invocation, timeout is currently set to 60000

I think this is expected.  The caching means that you only get blocks of 2000 rows.  And if
you go for longer than 60 seconds between blocks, then the scanner will time out.  You could
try tuning your caching down to 100 to see if that works for a bit (although, due to variance
in the time you take for processing, you might want to give yourself a bit more cushion than
that).

#at org.apache.hadoop.util.ReflectionUtils.newInstance(ReflectionUtils.java:131) 
your Mapper has no default constructor defined for it. 
it's likely because your map/red are defined at inner classes yet they are not static 
and thus cannot be used without their enclosing class


#cm:+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
select dt0(bytes_receive_network_interface_sum)-(dt0(bytes_transmit_network_interface_sum)) where category = CLUSTER and clusterId = $CLUSTERID
select  dt0(bytes_receive)-(dt0(bytes_transmit)) where category=NETWORK_INTERFACE and iface=eth0

 平均 主机 主机 CPU 使用率
(cpu_percent_host_avg) 
平均 网络接口 传送的字节数
(bytes_transmit_network_interface_avg_rate) 
平均 网络接口 接收的字节数
(bytes_receive_network_interface_avg_rate) 
总计 网络接口 传送的字节数
(bytes_transmit_network_interface_sum) 
总计 网络接口 接收的字节数
(bytes_receive_network_interface_sum) 
最大 网络接口 传送的字节数
(bytes_transmit_network_interface_max_rate) 
最大 网络接口 接收的字节数
(bytes_receive_network_interface_max_rate) 
最小 网络接口 传送的字节数
(bytes_transmit_network_interface_min_rate) 
最小 网络接口 接收的字节数
(bytes_receive_network_interface_min_rate) 