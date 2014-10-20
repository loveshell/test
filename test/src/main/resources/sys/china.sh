#!/bin/sh
#Get ip China
#by polly
#get the newest delegated-apnic-latest

#apnic|AU|ipv4|1.0.0.0|256|20110811|assigned
#apnic|CN|ipv4|1.0.1.0|256|20110414|allocated
#apnic|CN|ipv4|1.0.2.0|512|20110414|allocated
#apnic|AU|ipv4|1.0.4.0|1024|20110412|allocated
#apnic|CN|ipv4|1.0.8.0|2048|20110412|allocated

rm -f delegated-apnic-latest;
wget http://ftp.apnic.net/apnic/stats/apnic/delegated-apnic-latest;
cat /dev/null > chinaip.txt;

queryIp()
{   
grep $1 delegated-apnic-latest | cut -f 4,5 -d '|' | tr '|' ' ' | while read ip cnt
do
mask=$(bc<<END | tail -1
pow=32;
define log2(x) {
if (x<=1) return (pow);
pow--;
return(log2(x/2));
}
log2($cnt);
END
)
echo $ip/$mask >> chinaip.txt
done
}

queryIp 'apnic|CN|ipv4'
queryIp 'apnic|TW|ipv4'
queryIp 'apnic|HK|ipv4'
queryIp 'apnic|MO|ipv4'
