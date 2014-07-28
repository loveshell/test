export LANG="zh_CN.UTF-8"
export LC_ALL="zh_CN.UTF-8" 
mv nohup.out baknohup.out;mv nutchhbase.log baknutchhbase.log;
hadoop fs -rm -r -skipTrash /data;
java -Xmx1024m -jar test.urlinsert-1.0.0-jar-with-dependencies.jar --tableInit;
nohup java -jar urlidsrv-1.0.0-jar-with-dependencies.jar &
nohup hadoop jar apache-nutch-1.7.job org.apache.nutch.wind.WindCrawlHbase /url  -dir /data  -threads 200 -depth 2 -topN 80000  >nutchhbase.log &
