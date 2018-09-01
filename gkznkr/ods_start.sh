daynow=`date -d '-1 day' +%Y%m%d`
./ods.sh > /var/lib/hadoop-hdfs/log/ods_"$daynow"_log.txt 2>&1
