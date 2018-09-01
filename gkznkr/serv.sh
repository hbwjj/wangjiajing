daynow=`date -d '-1 day' +%Y%m%d`
time1=`date`
echo "============================================= $time1 start============================================="
for i in {1001,1003,1004,1005,1006,1007,1008,1009,1010,1011,1012,1013,1014,1015,1016,1017,1018}
do 
hadoop fs -rm /user/hive/warehouse/gkznkr.db/serv_bill_day/ptday=$daynow/city=$i/*
hadoop fs -rmdir /user/hive/warehouse/gkznkr.db/serv_bill_day/ptday=$daynow/city=$i
hadoop fs -rm /user/hive/warehouse/gkznkr.db/serv_bill_day/ptday=$daynow/*
hadoop fs -rmdir /user/hive/warehouse/gkznkr.db/serv_bill_day/ptday=$daynow
hive -e "alter table gkznkr.serv_bill_day drop if exists partition (ptday=$daynow,city=$i);"
hive -e "alter table gkznkr.serv_bill_day add partition (ptday=$daynow,city=$i);"
sqoop import --append --connect jdbc:oracle:thin:@133.0.85.183:1522/ods1 --username hbdx_noc --password noc_0803 --target-dir /user/hive/warehouse/gkznkr.db/serv_bill_day/ptday=$daynow/city=$i -m 1 --query "select * from serv_bill_day_v where latn_id=$i and \$CONDITIONS"
echo "######################### $i import is done!#########################"
done
time1=`date`
echo "============================================= $time1 end============================================="
