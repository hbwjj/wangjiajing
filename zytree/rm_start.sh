daynow=`date -d '-1 day' +%Y%m%d`
./rm.sh > /var/lib/hadoop-hdfs/log/rm_"$daynow"_log.txt 2>&1
