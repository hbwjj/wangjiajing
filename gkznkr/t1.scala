import org.apache.spark.sql.hive.HiveContext
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
val hiveContext = new HiveContext(sc)
val cal = Calendar.getInstance 
cal.add(Calendar.DATE, -1) 
val time: Date = cal.getTime 
val daynow: String = new SimpleDateFormat("yyyyMMdd").format(time) 
val ptmon: String = new SimpleDateFormat("yyyyMM").format(time) 
val citylist = List(1001,1003,1004,1005,1006,1007,1008,1009,1010,1011,1012,1013,1014,1015,1016,1017,1018)

hiveContext.sql(s"insert overwrite table gkznkr.ods_infor_day_t select * from gkznkr.ods_infor_day where ptday=${daynow}")
hiveContext.sql(s"insert overwrite table gkznkr.serv_bill_day_t select * from gkznkr.serv_bill_day where ptday=${daynow}")
hiveContext.sql(s"insert overwrite table gkznkr.prd_order_day_t select * from gkznkr.prd_order_day where ptday=${daynow}")

hiveContext.sql("insert overwrite table gkznkr.ods_serv_t select a.ptday,a.city,a.olt_ip,a.olt_name,a.olt_pon_type,a.pon_id,a.olt_pon_code,a.sn_code,a.pvlan,a.cvlan,a.site_name,a.obd_device_code,a.obd_device_name,a.obd_device_addr,a.up_obd_code,a.up_obd_name,a.up_obd_add,a.prod_id,a.prod_type,a.prod_spec_id,a.prod_spec_name,a.acc_types,a.acc_nbr1,a.acc_nbr2,a.fzj_code,a.area_id,a.area_name,a.local_id,a.local_name,a.zy_gird_id,a.zy_gird_name,a.yx_gird_id,a.yx_gird_name,b.serv_id,b.cust_id,b.star_month_id,b.vip_class,b.product_id,b.product_name,b.product_offer_id,b.offer_name,b.rate,b.billing_cycle_id,b.inv_amt,b.inv_bill_amt from gkznkr.ods_infor_day_t a inner join gkznkr.serv_bill_day_t b on a.city = b.city and a.prod_id = b.serv_id and a.ptday = b.ptday")

hiveContext.sql("insert overwrite table gkznkr.ods_serv_order_t select a.ptday,a.city,a.olt_ip,a.olt_name,a.olt_pon_type,a.pon_id,a.olt_pon_code,a.sn_code,a.pvlan,a.cvlan,a.site_name,a.obd_device_code,a.obd_device_name,a.obd_device_addr,a.up_obd_code,a.up_obd_name,a.up_obd_add,a.prod_id,a.prod_type,a.prod_spec_id,a.prod_spec_name,a.acc_types,a.acc_nbr1,a.acc_nbr2,a.fzj_code,a.area_id,a.area_name,a.local_id,a.local_name,a.zy_gird_id,a.zy_gird_name,a.yx_gird_id,a.yx_gird_name,a.serv_id,a.cust_id,a.star_month_id,a.vip_class,a.product_id,a.product_name,a.product_offer_id,a.offer_name,a.rate,a.billing_cycle_id,a.inv_amt,a.inv_bill_amt,b.open_date,b.close_date,b.pd_inst_state,b.strategy_segment,case when rate>=200 then 1 else 0 end flag_200m,case when rate=500 then 1 else 0 end flag_500m,case when strategy_segment='政企' then 1 else 0 end flag_zq from gkznkr.ods_serv_t a inner join gkznkr.prd_order_day_t b on a.serv_id = b.serv_id and a.ptday = b.ptday")

for(i<-citylist){
hiveContext.sql(s"insert overwrite table gkznkr.ods_serv_order_day partition (ptday=${daynow},city=${i}) select a.olt_ip,a.olt_name,a.olt_pon_type,a.pon_id,a.olt_pon_code,a.sn_code,a.pvlan,a.cvlan,a.site_name,a.obd_device_code,a.obd_device_name,a.obd_device_addr,a.up_obd_code,a.up_obd_name,a.up_obd_add,a.prod_id,a.prod_type,a.prod_spec_id,a.prod_spec_name,a.acc_types,a.acc_nbr1,a.acc_nbr2,a.fzj_code,a.area_id,a.area_name,a.local_id,a.local_name,a.zy_gird_id,a.zy_gird_name,a.yx_gird_id,a.yx_gird_name,a.serv_id,a.cust_id,a.star_month_id,a.vip_class,a.product_id,a.product_name,a.product_offer_id,a.offer_name,a.rate,a.billing_cycle_id,a.inv_amt,a.inv_bill_amt,a.open_date,a.close_date,a.pd_inst_state,a.strategy_segment,a.flag_200m,a.flag_500m,a.flag_zq from gkznkr.ods_serv_order_t a where a.city=${i} ")
}

hiveContext.sql(s"insert overwrite table gkznkr.pon_ods_serv_order_day partition (ptday=${daynow}) select a.month,a.week,b.olt_ip,b.olt_name,b.olt_pon_type,b.pon_id,b.olt_pon_code,a.name,a.ponfree,a.olt_type,a.type,a.speed,a.out,a.in,a.outper,a.inper,a.out_last,a.in_last,a.usrnum,a.usrnum_last,b.sn_code,b.pvlan,b.cvlan,b.site_name,b.obd_device_code,b.obd_device_name,b.obd_device_addr,b.up_obd_code,b.up_obd_name,b.up_obd_add,b.prod_id,b.prod_type,b.prod_spec_id,b.prod_spec_name,b.acc_types,b.acc_nbr1,b.acc_nbr2,b.fzj_code,b.area_id,b.area_name,b.local_id,b.local_name,b.zy_gird_id,b.zy_gird_name,b.yx_gird_id,b.yx_gird_name,b.serv_id,b.cust_id,b.star_month_id,b.vip_class,b.product_id,b.product_name,b.product_offer_id,b.offer_name,b.rate,b.billing_cycle_id,b.inv_amt,b.inv_bill_amt,b.open_date,b.close_date,b.pd_inst_state,b.strategy_segment,b.flag_200m,b.flag_500m,b.flag_zq,case when a.ponfree='' then 0 else 1 end flag_free from gkznkr.pon_flux_infor a inner join gkznkr.ods_serv_order_t b on a.ptday = b.ptday and a.city = b.city and a.olt_ip = b.olt_ip and a.pon_id = b.pon_id ")

hiveContext.sql(s"insert overwrite table gkznkr.pon_split_infor_t1 select ptday,month,week,local_id,area_id,olt_ip,olt_name,site_name,olt_type,olt_pon_type,pon_id,ponfree,type,speed,out,in,outper,inper,out_last,in_last,usrnum,usrnum_last,sn_code,case when inv_amt='null' then '-1' else inv_amt/100 end inv_amt,case when rate='null' then '-1' else rate end rate,flag_zq,flag_free from gkznkr.pon_ods_serv_order_day a where a.ptday=${daynow} and sn_code<>'null' ")

hiveContext.sql(s"insert overwrite table gkznkr.pon_split_infor_day partition (ptday=${daynow}) select month,week,local_id,area_id,olt_ip,olt_name,site_name,olt_type,olt_pon_type,pon_id,ponfree,type,speed,out,in,outper,inper,out_last,in_last,usrnum,usrnum_last,arpu,num_200m,num_500m,num_zq,flag_free from gkznkr.pon_split_infor_t4")


hiveContext.sql(s"insert overwrite table gkznkr.pon_split_infor_week_t1 select * from gkznkr.pon_split_infor_day where month = ${ptmon}")

hiveContext.sql(s"insert overwrite table gkznkr.pon_split_infor_week partition (ptmon=${ptmon}) select week,local_id,b.sg,a.area_id,olt_ip,olt_name,site_name,olt_type,olt_pon_type,pon_id,ponfree,type,speed,flag_free,out_avg,in_avg,outper_avg,inper_avg,out_add,in_add,out_incease,in_incease,usr_avg,usr_add,arpu_avg,200m_avg,500m_avg,zq_avg,times,lista,listb,listc,listd,level,advise,reserve4,reserve5,reserve6 from gkznkr.pon_split_infor_week_t8 a left join gkznkr.area_sg_t b on a.area_id=b.area")
