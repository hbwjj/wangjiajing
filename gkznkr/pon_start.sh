daynow=`date -d '-1 day' +%Y%m%d`
./pon.sh > /var/lib/hadoop-hdfs/log/pon_"$daynow"_log.txt 2>&1
