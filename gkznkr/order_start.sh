daynow=`date -d '-1 day' +%Y%m%d`
./order.sh > /var/lib/hadoop-hdfs/log/order_"$daynow"_log.txt 2>&1
