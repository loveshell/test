nohup hadoop jar apache-nutch-1.7.job org.apache.nutch.wind.WindCrawl /nutch/urls -dir /nutch/data  -threads 50 -depth 5 -topN 5000 >nutch.log &
