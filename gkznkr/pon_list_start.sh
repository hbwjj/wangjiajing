daynow=`date -d '-1 day' +%Y%m%d`
./pon_list.sh > /var/lib/hadoop-hdfs/log/pon_list_"$daynow"_log.txt 2>&1
