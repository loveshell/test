#core================================================================================================
netstat -nlpt

swapoff -a
swapon 

cache释放：说明，释放前最好sync一下，防止丢数据。
To free pagecache:
echo 1 > /proc/sys/vm/drop_caches
To free dentries and inodes:
echo 2 > /proc/sys/vm/drop_caches
To free pagecache, dentries and inodes:
echo 3 > /proc/sys/vm/drop_caches


# hwclock --hctosys        hwclock --show
# hwclock --systohc  
cat /etc/sysconfig/clock
date -s 20110303 修改日期
date -s 15:39:11 修改时间

/etc/profile   　注：修改文件后要想马上生效还要运行# source /etc/profile不然只能在下次重进此用户时生效。
在用户目录下的.bash_profile文件中增加变量【对单一用户生效(永久的)】
cat /etc/resolv.conf
en_US.UTF-8：你说英语，你在美国，字符集是utf-8
zh_CN.UTF-8：你说中文，你在中国，字符集是utf-8 
如果你的LANG环境变量是zh_CN.UTF-8，那么系统的菜单、程序的工具栏语言、输入法默认语言就都是中文的。
cat /etc/sysconfig/language
查看编码的方法
方法一：file filename
方法二：在Vim中可以直接查看文件编码
　　:set fileencoding
如果你只是想查看其它编码格式的文件或者想解决用Vim查看文件乱码的问题，那么你可以在
　　~/.vimrc 文件中添加以下内容：
　　set encoding=utf-8 fileencodings=ucs-bom,utf-8,cp936
　　这样，就可以让vim自动识别文件编码（可以自动识别UTF-8或者GBK编码的文件）
　　:set fileencoding=utf-8
优先级的关系：LC_ALL > LC_* > LANG

在linux系统中可以通过ulimit–n查看每个进程限制的最大句柄数，通过ulimit –HSn 10240修改进程的最大句柄数。当句柄数目达到限制后，就回出现”too many files open”。
查看进程占用的句柄数有几种办法：
1） 通过cat/proc/pid/fd可以查看线程pid号打开的线程；
2） 通过lsof命令， /usr/sbin/lsof-p 21404 命令结果如下：
 
ln -s a b 中的 a 就是源文件，b是链接文件名,其作用是当进入b目录，实际上是链接进入了a目录
删除软链接：   rm -rf  b  注意不是rm -rf  b/
ln  a b 是建立硬链接 
du -sh


# ================================================================================================
#查看CPU信息（型号）  #(看到有8个逻辑CPU, 也知道了CPU型号)
 cat /proc/cpuinfo | grep name | cut -f2 -d: | uniq -c
      8  Intel(R) Xeon(R) CPU            E5410   @ 2.33GHz

#(说明实际上是两颗4核的CPU)
 cat /proc/cpuinfo | grep physical | uniq -c
      4 physical id      : 0
      4 physical id      : 1

 getconf LONG_BIT
   32
#(说明当前CPU运行在32bit模式下, 但不代表CPU不支持64bit)

 cat /proc/cpuinfo | grep flags | grep ' lm ' | wc -l
   8
#(结果大于0, 说明支持64bit计算. lm指long mode, 支持lm则是64bit)


#再完整看cpu详细信息, 不过大部分我们都不关心而已.
 dmidecode | grep 'Processor Information'

#查看内 存信息
 cat /proc/meminfo

 uname -a
Linux euis1 2.6.9-55.ELsmp #1 SMP Fri Apr 20 17:03:35 EDT 2007 i686 i686 i386 GNU/Linux
#(查看当前操作系统内核信息)

 cat /etc/issue | grep Linux
Red Hat Enterprise Linux AS release 4 (Nahant Update 5)
#(查看当前操作系统发行版信息)

#查看机器型号
 dmidecode | grep "Product Name" 

#查看网卡信息
 dmesg | grep -i eth
 
 
# ================================================================================================
 
  系统

# uname -a               # 查看内核/操作系统/CPU信息
# head -n 1 /etc/issue   # 查看操作系统版本
# cat /proc/cpuinfo      # 查看CPU信息
# hostname               # 查看计算机名
# lspci -tv              # 列出所有PCI设备
# lsusb -tv              # 列出所有USB设备
# lsmod                  # 列出加载的内核模块
# env                    # 查看环境变量

资源

# free -m                # 查看内存使用量和交换区使用量
# df -h                  # 查看各分区使用情况
# du -sh <目录名>        # 查看指定目录的大小
# grep MemTotal /proc/meminfo   # 查看内存总量
# grep MemFree /proc/meminfo    # 查看空闲内存量
# uptime                 # 查看系统运行时间、用户数、负载
# cat /proc/loadavg      # 查看系统负载

磁盘和分区

# mount | column -t      # 查看挂接的分区状态
# fdisk -l               # 查看所有分区
# swapon -s              # 查看所有交换分区
# hdparm -i /dev/hda     # 查看磁盘参数(仅适用于IDE设备)
# dmesg | grep IDE       # 查看启动时IDE设备检测状况

网络

# ifconfig               # 查看所有网络接口的属性
# iptables -L            # 查看防火墙设置
# route -n               # 查看路由表
# netstat -lntp          # 查看所有监听端口
# netstat -antp          # 查看所有已经建立的连接
# netstat -s             # 查看网络统计信息

进程

# ps -ef                 # 查看所有进程
# top                    # 实时显示进程状态

用户

# w                      # 查看活动用户
# id <用户名>            # 查看指定用户信息
# last                   # 查看用户登录日志
# cut -d: -f1 /etc/passwd   # 查看系统所有用户
# cut -d: -f1 /etc/group    # 查看系统所有组
# crontab -l             # 查看当前用户的计划任务

服务

# chkconfig --list       # 列出所有系统服务
# chkconfig --list | grep on    # 列出所有启动的系统服务

程序

# rpm -qa                # 查看所有安装的软件包
