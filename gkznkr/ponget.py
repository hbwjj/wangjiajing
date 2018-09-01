import time
import sys
import numpy as np
import pandas as pd
import os
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
import redis
import json
import codecs

filetime = sys.argv[1]
filename ='/data9/output/pon'+ filetime +'.txt'

df = pd.read_table(filename,encoding='utf8',sep=',')

df['pon_id']=df.apply(lambda row: '-'.join(i.zfill(2) for i in row['port'].split('-')[-2:]), axis =1)

pool1 = redis.ConnectionPool(host='127.0.0.1',password='Dtsgx@2018333', port=6380, db=3)
r1 = redis.Redis(connection_pool=pool1)	
df['usrnum']=	df.apply(lambda row:'' if r1.get(row['ip']+'|'+row['pon_id']) is None else r1.get(row['ip']+'|'+row['pon_id']).decode('utf-8'),axis=1)
	
pool2 = redis.ConnectionPool(host='127.0.0.1',password='Dtsgx@2018333', port=6380, db=4)
r2 = redis.Redis(connection_pool=pool2)	
df['ponfree']=df.apply(lambda row:'' if r2.get(row['ip']) is None else ';'.join(json.loads(r2.get(row['ip']).decode('utf-8')).get('pon')),axis=1)

pool3 = redis.ConnectionPool(host='127.0.0.1',password='Dtsgx@2018333', port=6380, db=5)
r3 = redis.Redis(connection_pool=pool3)	
df['olt_type']=df.apply(lambda row: '' if r3.get(row['ip']) is None else r3.get(row['ip']).decode('utf-8'),axis=1)

df['city']=df.apply(lambda row: row['area'].split('-').pop(), axis =1)
df['city_id']=df['city'].replace({'WH':'1001','XY':'1003','HG':'1004','YC':'1005','XG':'1006','EZ':'1007','XN':'1008','SY':'1009'
,'JM':'1010','HS':'1011','SZ':'1012','ES':'1013','XT':'1014','TM':'1015','QJ':'1016','LQ':'1017','JZ':'1018'})

pon_infor=df.groupby(['ip','pon_id','port','city_id','name','type','speed','day','usrnum','ponfree','olt_type']).agg({'out':'max','in':'max','outper':'max','inper':'max'}).reset_index()

file1 = codecs.open('/var/lib/hadoop-hdfs/output/week.txt','r','utf-8')	
adict1 = {}	
for line in file1:
    line1 = line.strip().split('\t')
    key = line1[0]+'|'+ '-'.join(i.zfill(2) for i in line1[1].split('-')[-2:])
    value = [float(line1[4]),float(line1[3])]
    adict1[key] = value
  
pon_infor['keys']=pon_infor.apply(lambda row: row['ip']+'|'+row['pon_id'], axis =1)
keycl=list(pon_infor.iloc[:,-1])
out_last=[]
in_last=[]

for i in keycl:
    for key in adict1:
        if key==i :
            x=adict1[key][0]
            y=adict1[key][1]
            break
        else :
            x=0
            y=0
    out_last.append(x)
    in_last.append(y)

out_last=pd.Series(out_last)
in_last=pd.Series(in_last)

pon_infor = pd.concat([pon_infor, out_last], axis=1)
pon_infor.rename(columns={0:'out_last'}, inplace = True)
pon_infor = pd.concat([pon_infor, in_last], axis=1)
pon_infor.rename(columns={0:'in_last'}, inplace = True)

file2 = codecs.open('/var/lib/hadoop-hdfs/output/onucnt_week.txt','r','utf-8')
adict2 = {}	
for line in file2:
    line1 = line.strip().split('\t')
    key = line1[0]+'|'+ '-'.join(i.zfill(2) for i in line1[1].split('-')[-2:])
    value =line1[2].split('.')[0]    
    adict2[key] = value

usrnum_last=[]
for i in keycl:
    for key in adict2:
        if key==i :
            x=adict2[key]
            break
        else :
            x=0
    usrnum_last.append(x)

usrnum_last=pd.Series(usrnum_last)

pon_infor = pd.concat([pon_infor, usrnum_last], axis=1)
pon_infor.rename(columns={0:'usrnum_last'}, inplace = True)

pon_infor['month']=pon_infor.apply(lambda row:str(row['day'])[0:6],axis=1)
pon_infor['week1']=pon_infor.apply(lambda row:str(row['day'])[-2:],axis=1)	

time=pon_infor.iloc[:,-1]
week=[]

for i in time:
    day = int(i)
    if day <=7 : j=1
    if day >=8 and day <=14 : j=2
    if day >=15 and day <=21 : j=3
    if day >=22 and day <=28 : j=4
    if day >=29 : j=5
    week.append(j)

week=pd.Series(week)
pon_infor = pd.concat([pon_infor, week], axis=1)
pon_infor.rename(columns={0:'week'}, inplace = True)

data=pon_infor[['day','month','week','ip','pon_id','port','city_id','name','type','speed','ponfree','olt_type','out','in','out_last','in_last','outper','inper','usrnum','usrnum_last']]
filename1 ='/var/lib/hadoop-hdfs/pon/pon_infor_'+ filetime +'.txt'
data.to_csv(filename1,sep=',',index=False,header=0)
