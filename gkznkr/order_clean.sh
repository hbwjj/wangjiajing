hadoop fs -rm /user/hive/warehouse/gkznkr.db/prd_order_day/ptday=20180812/*
hadoop fs -rmdir /user/hive/warehouse/gkznkr.db/prd_order_day/ptday=20180812
hive -e "alter table gkznkr.prd_order_day drop if exists partition (ptday=20180812);"
