ssh root@133.0.193.216 <<EOF
su - oracle
sqlldr userid=yisou/Dtsgx2018111 control=loadtxt.ctl
EOF
