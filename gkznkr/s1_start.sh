daynow=`date -d '-1 day' +%Y%m%d`
/usr/bin/spark-shell -i <s1.scala > /var/lib/hadoop-hdfs/log/s1_"$daynow"_log.txt 2>&1
