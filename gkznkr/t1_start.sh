daynow=`date -d '-1 day' +%Y%m%d`
/usr/bin/spark-shell -i <t1.scala > /var/lib/hadoop-hdfs/log/t1_"$daynow"_log.txt 2>&1
