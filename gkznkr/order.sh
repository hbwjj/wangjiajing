daynow=`date -d '-1 day' +%Y%m%d`
time1=`date`
echo "============================================= $time1 start============================================="
hadoop fs -rm /user/hive/warehouse/gkznkr.db/prd_order_day/ptday=$daynow/*
hadoop fs -rmdir /user/hive/warehouse/gkznkr.db/prd_order_day/ptday=$daynow
hive -e "alter table gkznkr.prd_order_day drop if exists partition (ptday=$daynow);"
hive -e "alter table gkznkr.prd_order_day add partition (ptday=$daynow);"
sqoop import --append --connect jdbc:oracle:thin:@133.0.85.183:1522/ods1 --username hbdx_noc --password noc_0803 --table  PRD_ORDER_DAY_V -m 1 --target-dir /user/hive/warehouse/gkznkr.db/prd_order_day/ptday=$daynow
time1=`date`
echo "============================================= $time1 end============================================="
