month=`date -d '-1 day' +%Y%m`
day=`date -d '-1 day' +%Y%m%d`
info='AnissPonSplit'$day
hive -e "drop table gkznkr.pon_split_infor_week_tt;"
hive -e "drop table gkznkr.pon_split_infor_week_t;"
hive -e "create table gkznkr.pon_split_infor_week_tt as select week,local_name,area_name,olt_ip,olt_name,site_name,olt_type,olt_pon_type,pon_id,substr(ponfree,1,250) ponfree,type,speed,flag_free,out_avg,in_avg,outper_avg,inper_avg,out_add,in_add,out_incease,in_incease,usr_avg,usr_add,arpu_avg,200m_avg,500m_avg,zq_avg,times,lista,listb,listc,listd,level,advise,db_no,db_result,db_time,db_check,db_times,ptmon,row_number() over (order by olt_ip,pon_id) id,'$info' infos  from gkznkr.pon_split_infor_week where ptmon=$month and week='5';"
hive -e "create table gkznkr.pon_split_infor_week_t as select week,local_name,area_name,olt_ip,olt_name,site_name,olt_type,olt_pon_type,pon_id,ponfree,type,speed,flag_free,out_avg,in_avg,outper_avg,inper_avg,out_add,in_add,out_incease,in_incease,usr_avg,usr_add,arpu_avg,200m_avg,500m_avg,zq_avg,times,lista,listb,listc,listd,level,advise,db_no,db_result,db_time,db_check,db_times,ptmon,concat(infos,lpad(id,6,0)) infor from gkznkr.pon_split_infor_week_tt;"
sqoop export --table pon_split_infor_week --connect jdbc:oracle:thin:@133.0.194.15:1521:aniam --username odso --password odso_123 --export-dir /user/hive/warehouse/gkznkr.db/pon_split_infor_week_t --columns WEEK,LOCAL_NAME,AREA_NAME,OLT_IP,OLT_NAME,SITE_NAME,OLT_TYPE,OLT_PON_TYPE,PON_ID,PONFREE,TYPE,SPEED,FLAG_FREE,OUT_AVG,IN_AVG,OUTPER_AVG,INPER_AVG,OUT_ADD,IN_ADD,OUT_INCEASE,IN_INCEASE,USR_AVG,USR_ADD,ARPU_AVG,M200_AVG,M500_AVG,ZQ_AVG,TIMES,LISTA,LISTB,LISTC,LISTD,LEVEL1,ADVISE1,DB_NO,DB_RESULT,DB_TIME,DB_CHECK,DB_TIMES,PTMON,RESERVE1 --input-fields-terminated-by '\001' --input-lines-terminated-by '\n' --input-null-string '\\N' --input-null-non-string '\\N' 

