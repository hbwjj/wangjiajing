daynow=`date -d '-1 day' +%Y%m%d`
./ta.sh > /var/lib/hadoop-hdfs/log/ta_"$daynow"_log.txt 2>&1
