daynow=`date -d '-1 day' +%Y%m%d`
/root/anaconda3/bin/python /root/ponget.py $daynow > /data9/log/pongget_"daynow".txt
