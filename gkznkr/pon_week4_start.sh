daynow=`date -d '-1 day' +%Y%m%d`
./pon_week4.sh > /var/lib/hadoop-hdfs/log/pon_week4_"$daynow"_log.txt 2>&1
