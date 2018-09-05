daynow=`date -d '-1 day' +%Y%m%d`
./pon_week5.sh > /var/lib/hadoop-hdfs/log/pon_week5_"$daynow"_log.txt 2>&1
