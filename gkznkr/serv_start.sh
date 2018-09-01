daynow=`date -d '-1 day' +%Y%m%d`
./serv.sh > /var/lib/hadoop-hdfs/log/serv_"$daynow"_log.txt 2>&1
