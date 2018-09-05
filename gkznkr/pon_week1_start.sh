daynow=`date -d '-1 day' +%Y%m%d`
./pon_week1.sh > /var/lib/hadoop-hdfs/log/pon_week1_"$daynow"_log.txt 2>&1
