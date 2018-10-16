daynow=20181011
for i in {1001,1003,1004,1005,1006,1007,1008,1009,1010,1011,1012,1013,1014,1015,1016,1017,1018}
do
hadoop fs -rm /user/hive/warehouse/huijiucuo.db/ta_infor_day/ptday=$daynow/city=$i/*
hadoop fs -rmdir /user/hive/warehouse/huijiucuo.db/ta_infor_day/ptday=$daynow/city=$i
hadoop fs -rm /user/hive/warehouse/huijiucuo.db/ta_infor_day/ptday=$daynow/*
hadoop fs -rmdir /user/hive/warehouse/huijiucuo.db/ta_infor_day/ptday=$daynow
hive -e "alter table huijiucuo.ta_infor_day drop if exists partition (ptday=$daynow,city=$i);"
done

