daynow=`date -d '-1 day' +%Y%m%d`
out_file=/data9/hjc/hjchz$daynow.txt
hive -e "select a.* from huijiucuo.wls_t a;" > $out_file
