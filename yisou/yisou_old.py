# -*- coding: utf-8 -*-
import fnmatch
import codecs
import time
import oracle_pool
import sys
import IPy
import re
#出现除了数字字母@符号的字符
pattern=re.compile(r'[^A-Za-z0-9@]')
import pymysql
#避免中文乱码（系统环境变量配置）
import os
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

#解决Python2.7的UnicodeEncodeError: ‘ascii’ codec can’t encode异常错误
reload(sys)
sys.setdefaultencoding('utf-8')

#传入前一天时间的命令行参数，格式为20180108
filetime = sys.argv[1]

#切换到数据目录,每次处理前一天的数据
os.chdir('/data/logs/sys/'+ filetime)
filedir = os.getcwd()
filenames = os.listdir(filedir)
#rtb_creative和rtb_click都在rtb_log_crit_201801080900.log文件里面 
filelike = 'rtb_log_crit_*.log'
filenames = fnmatch.filter(filenames, filelike)  

#提取日志内容到对应文件
file1 ='/root/yisou/rtb_creative.txt'
f1 = codecs.open(file1,'w')

file2 ='/root/yisou/rtb_click.txt'
f2 = codecs.open(file2,'w')

#日志监控
file0 ='/root/yisou/rtb_log_'+ filetime +'.txt'
f0 = codecs.open(file0,'w')
now = time.strftime("%Y-%m-%d %H:%M:%S")
testinfo = now + ': start=======================================================' + '\n'
f0.write(testinfo)  

#读取原始日志文件
for filename in filenames:
    #以二进制读取文件
    for line in codecs.open(filedir+'/'+filename,'rb'):
        #二进制串在经过str()函数转化之后开头都带有“b’，要替换掉
        line = str(line) 
        filename1 = str(filename) 
        line = line.replace("b'",'') 
        line = line.strip()
        #以rtb_creative开头的记录
        if line.startswith('rtb_creative'):   
            line1 = line.split('\x01')     
            #时间 5 广告ID 7 pushid  8 ip 9 推送区域 11 用户手机号 15 设备终端类型 17 用户受访域名 23
            line1 = [line1[i] for i in (4, 6, 7, 8, 10, 14, 16, 22)]
            #尽量不要插入表头，不然会挪动列表元素
            #列表尾部追加推送次数（默认为1）,点击次数（默认为0）,推送日期,来源日志文件
            line1.append('1')
            line1.append('0')
            line1.append(filetime)
            line1.append(filename1)
            #广告ID是否为整型
            if line1[1].isdigit() == False:
                line1[1] = '-1'
            #检测输入的IP是否合法 & ip数字化
            try : IPy.IP(line1[3]).version()
            #没有指定异常类型，捕获任意异常
            except :        
                line1.append('-1')       
                f0.write('wrong ip:' + line1[3] +'\n') 
            #没有触发异常时，执行的语句块
            else: 
                #IPy模块包含IP类，通过version方法来区分出IPv4和IPv6    
                if IPy.IP(line1[3]).version() == 4 :
                    #转换为整型格式
                    line1.append(str(IPy.IP(line1[3]).int()))  
                else :
                    line1.append('-1')            
            #推送区域是否为整型
            if line1[4].isdigit() == False:
                line1[4] = '-1'  
            #orale数据库设置acc_nbr varchar2(32)，提示插入值太大，可能是号码有部分乱码导致截取失败
            #用户号码太长只取前30位
            line1[5] = line1[5].strip()
            if len(line1[5]) > 30 :
                t = line1[5]
                line1[5] = t[:30]   
            #号码包含除了字母数字@之外的字符判断为乱码         
            s = line1[5]
            x=re.search(pattern,s)
            if x:
                line1[5] = '-1'          
            #终端类型判断 0:PC  1:手机
            a = line1[6]
            try :  int(float(a))
            except :
                f0.write('wrong terminal:' + a +'\n') 
            else : 
                if int(float(a)) == 0: 
                    line1[6] = 'PC'
                elif int(float(a)) == 1: 
                    line1[6] = 'M'
                else : line1[6] = '-1'              
            line1 = '|'.join(line1)
            f1.write(line1 + '\n')
        #以rtb_click开头的记录
        if line.startswith('rtb_click'): 
            line = line.split('\x01')   
            line2 = line[7]
            f2.write(line2 + '\n')

now = time.strftime("%Y-%m-%d %H:%M:%S")
testinfo = now + ': get log information !' + '\n' 
f0.write(testinfo) 

f1.close()
f2.close()

#提取用户推送内容文件和网址推送内容入库
f2 = codecs.open(file2,'r')

#提取用户推送内容文件和网址推送内容入库
#引用连接池oracle包
orcl = oracle_pool.oracle('yisou','Dtsgx2018111','133.0.193.216','orcl')

#插入推送临时数据
#采用将文件/root/yisou/rtb_creative.txt传送到oracle服务器，使用sqlldr导入
os.system("scp /root/yisou/rtb_creative.txt root@133.0.193.216:/home/oracle/")
os.system("sh /root/oracle.sh")

#插入条数
insum1=0

sql1 = "select count(*) from push_test1"
orcl.execute(sql1)
insum1=orcl.fetchone()
insum1=insum1[0]

now = time.strftime("%Y-%m-%d %H:%M:%S")
testinfo = now + ': insert push_test1 ' + str(insum1) + '\n'
f0.write(testinfo)  


#插入点击临时数据
sql2 = "delete from push_test2"
orcl.execute(sql2)

#插入条数
insum2 = 0

for line2 in f2:
    line2 = line2.strip()
    sql2 = "insert into push_test2(push_id) values('{0}')".format(line2)
    try:
         orcl.execute(sql2)
         insum2 = insum2 + 1
    except :
         f0.write('ERROR2 :' + '\n' + sql2 + '\n')    
    if insum2 % 100000 == 0 :
        now = time.strftime("%Y-%m-%d %H:%M:%S")
        testinfo = now + ': insert push_test2 ' + str(insum2) + '\n'
        f0.write(testinfo)          

now = time.strftime("%Y-%m-%d %H:%M:%S")
testinfo = now + ': insert push_test2 ' + str(insum2) + '\n'
f0.write(testinfo)   

f2.close()

#关联匹配
sql3 = "update push_test1 a set click_times=1 where exists(select 1 from push_test2 b where a.push_id = b.push_id)"
orcl.execute(sql3)

now = time.strftime("%Y-%m-%d %H:%M:%S")
testinfo = now + ': combine db information !' + '\n' 
f0.write(testinfo) 

#推送记录正式入库
sql4 = "insert into push_user_infor select yyyymmdd,push_time,ad_id,push_id,ip,area_id,acc_nbr,terminal,push_times,click_times,infor_source,numip from push_test1"
orcl.execute(sql4)

sql5 = "insert into push_url_infor select yyyymmdd,ad_id,push_id,terminal,website,push_times,click_times from push_test1"
orcl.execute(sql5)

now = time.strftime("%Y-%m-%d %H:%M:%S")
testinfo = now + ': insert db finish !' + '\n' 
f0.write(testinfo) 

orcl.close() 
f0.close() 
