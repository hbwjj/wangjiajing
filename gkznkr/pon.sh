daynow=`date -d '-1 day' +%Y%m%d`
time1=`date`
echo "============================================= $time1 start============================================="
#表只存放当天数据
hadoop fs -rm /user/hive/warehouse/gkznkr.db/pon_flux_infor/*
hive -e "load data local inpath '/var/lib/hadoop-hdfs/pon/pon_infor_$daynow.txt' into table gkznkr.pon_flux_infor;"
time1=`date`
echo "============================================= $time1 end============================================="
