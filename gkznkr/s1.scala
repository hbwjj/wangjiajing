import org.apache.spark.sql.hive.HiveContext
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
val hiveContext = new HiveContext(sc)
val cal = Calendar.getInstance 
cal.add(Calendar.DATE, -1) 
val time: Date = cal.getTime 
val daynow: String = new SimpleDateFormat("yyyyMMdd").format(time) 
val citylist = List(1001,1003,1004,1005,1006,1007,1008,1009,1010,1011,1012,1013,1014,1015,1016,1017,1018)

for (x <- citylist)  
hiveContext.sql(s"insert overwrite table huijiucuo.rm_infor_day partition (ptday=${daynow},city=${x}) select distinct prod_id,case when instr(acc_nbr2,'@')>0 then Lower(substr(acc_nbr2,1,instr(acc_nbr2,'@')-1)) when instr(acc_nbr2,'@')=0 then Lower(acc_nbr2) end acc_nbr,area_id,area_name,local_id,local_name,managed_port_id,managed_port_code,managed_device_id,managed_device_code,sn_code,pvlan,cvlan,obd_port_id,obd_port_code,obd_device_id,obd_device_code,up_obd_id,up_obd_code,up_obd_name,olt_pon_id,olt_pon_code,pon_id,olt_id,olt_code,olt_name,olt_ip,sw_downport,sw_device_name,sw_id,sw_upport,bas_port,bas_ip,bas_name,obd_device_name,up_obd_port_id,up_obd_add,m_opath_code,m_opath_name,z_opath_code,z_opath_name,m_opath_route,z_opath_route from gkznkr.ods_infor_day where olt_ip<>'NULL' and acc_types='FTTH' and ptday=${daynow} and city=${x}")

hiveContext.sql(s"insert overwrite table huijiucuo.rm_infor_day_t select * from huijiucuo.rm_infor_day where ptday=${daynow}")

hiveContext.sql(s"insert overwrite table huijiucuo.ta_infor_day_t select * from huijiucuo.ta_infor_day where ptday=${daynow}")

for (x <- citylist) 
hiveContext.sql(s"insert overwrite table huijiucuo.rm_ta_infor_day partition (ptday=${daynow},city=${x}) select distinct serv_id,acc_nbr,area_id,area_name,local_id,local_name,managed_port_id,managed_port_code,managed_device_id,managed_device_code,sn_code,pvlan,cvlan,obd_port_id,obd_port_code,obd_device_id,obd_device_code,up_obd_id,up_obd_code,up_obd_name,olt_pon_id,olt_pon_code,pon_id,olt_id,olt_code,olt_name,olt_ip,sw_downport,sw_device_name,sw_id,sw_upport,bas_port,bas_ip,bas_name,obd_device_name,up_obd_port_id,up_obd_add,m_opath_code,m_opath_name,z_opath_code,z_opath_name,m_opath_route,z_opath_route,area,oltname,oltip,oltslport,switchname,switchip,switchxlportname1,switchxlportname2,switchslportname1,switchslportname2,basname,basip,basxlportname1,basxlportname2,eponcircuitname,fibersection1,relaycircuitname,fibersection2,transcircuitname from huijiucuo.rm_infor_day_t a left join huijiucuo.ta_infor_day_t b on a.olt_ip = b.oltip where a.city = ${x}")

hiveContext.sql(s"insert overwrite table huijiucuo.rm_ta_infor_day_t select * from huijiucuo.rm_ta_infor_day where ptday=${daynow}")

hiveContext.sql(s"insert overwrite table huijiucuo.ljzys2_t select * from huijiucuo.ljzys2 where time=${daynow}")

hiveContext.sql("insert overwrite table huijiucuo.crm_t select distinct serv_id,case when instr(acc_nbr,'@')>0 then Lower(substr(acc_nbr,1,instr(acc_nbr,'@')-1)) when instr(acc_nbr,'@')=0 then Lower(acc_nbr) end acc_nbr,latn_id from gkznkr.serv_bill_day_t")

hiveContext.sql("insert overwrite table huijiucuo.ljzys2_crm_t select distinct a.acc,a.basip,a.ptype,a.port,a.pvlan,a.cvlan,a.city,a.bname,a.suport,a.swip,a.swname,a.sdport,a.ouport,a.oltip,a.oname,a.time,b.serv_id,b.acc_nbr crm_acc,b.latn_id from huijiucuo.ljzys2_t a left join huijiucuo.crm_t b on a.acc=b.acc_nbr and a.city=b.latn_id")

hiveContext.sql(s"insert overwrite table huijiucuo.wls_t select distinct a.acc,a.basip,a.ptype,a.port,a.pvlan,a.cvlan,a.city,a.bname,a.suport,a.swip,a.swname,a.sdport,a.ouport,a.oltip,a.oname,a.time,b.area_id,b.olt_ip,b.olt_name,b.oltslport,b.olt_pon_id,b.pvlan,b.cvlan,b.switchname,b.switchip,b.switchslportname2,b.switchxlportname2,b.basname,b.basip,b.basxlportname2,a.serv_id from huijiucuo.ljzys2_crm_t a left join huijiucuo.rm_ta_infor_day_t b on a.acc = b.acc_nbr and a.city=b.city")



