daynow=`date -d '-1 day' +%Y%m%d`
./pon_week2.sh > /var/lib/hadoop-hdfs/log/pon_week2_"$daynow"_log.txt 2>&1
