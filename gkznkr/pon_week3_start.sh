daynow=`date -d '-1 day' +%Y%m%d`
./pon_week3.sh > /var/lib/hadoop-hdfs/log/pon_week3_"$daynow"_log.txt 2>&1
