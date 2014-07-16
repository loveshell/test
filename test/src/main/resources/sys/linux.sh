swapoff -a
swapon 

scp hbase.tar.gz imsrvtest1:/data/
ssh imsrvtest1 "cd /data;tar -zxf /data/hbase.tar.gz"

scp /data/nutch_new/hbasetest.jar imsrvtest1:/data/hbase-0.98.3-hadoop1/lib/;scp /data/nutch_new/hbasetest.jar imsrvtest2:/data/hbase-0.98.3-hadoop1/lib/;scp /data/nutch_new/hbasetest.jar skytest2:/data/hbase-0.98.3-hadoop1/lib/;scp /data/nutch_new/hbasetest.jar skytest1:/data/hbase-0.98.3-hadoop1/lib/;
/data/hbase-0.98.3-hadoop1/bin/stop-hbase.sh
/data/hbase-0.98.3-hadoop1/bin/start-hbase.sh 

./hbase-daemon.sh start regionserver

# hwclock --hctosys        hwclock --show
# hwclock --systohc  
cat /etc/sysconfig/clock
date -s 20110303 修改日期
date -s 15:39:11 修改时间


 /etc/profile   　注：修改文件后要想马上生效还要运行# source /etc/profile不然只能在下次重进此用户时生效。
 在用户目录下的.bash_profile文件中增加变量【对单一用户生效(永久的)】