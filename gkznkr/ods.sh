daynow=`date -d '-1 day' +%Y%m%d`
time1=`date`
echo "============================================= $time1 start============================================="
for i in {1001,1003,1004,1005,1006,1007,1008,1009,1010,1011,1012,1013,1014,1015,1016,1017,1018}
do 
hadoop fs -rm /user/hive/warehouse/gkznkr.db/ods_infor_day/ptday=$daynow/city=$i/*
hadoop fs -rmdir /user/hive/warehouse/gkznkr.db/ods_infor_day/ptday=$daynow/city=$i
hadoop fs -rm /user/hive/warehouse/gkznkr.db/ods_infor_day/ptday=$daynow/*
hadoop fs -rmdir /user/hive/warehouse/gkznkr.db/ods_infor_day/ptday=$daynow
hive -e "alter table gkznkr.ods_infor_day drop if exists partition (ptday=$daynow,city=$i);"
hive -e "alter table gkznkr.ods_infor_day add partition (ptday=$daynow,city=$i);"
sqoop import --append --connect jdbc:oracle:thin:@133.0.186.48:11521/odsodb --username jk_odso --password jk_ods2018 --target-dir /user/hive/warehouse/gkznkr.db/ods_infor_day/ptday=$daynow/city=$i -m 1 --query "select * from ods_infor_day_v where local_id=$i and \$CONDITIONS"
echo "######################### $i import is done!#########################"
done
time1=`date`
echo "============================================= $time1 end============================================="
