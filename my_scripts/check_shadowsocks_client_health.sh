#!/bin/bash

content=`ps aux | grep sslocal | grep -v grep`
if [[ $content == '' ]];then
    echo "shadowsocks client process has gone with the wind, pull it up!"
    echo `date '+%F %H:%M:%S'` pull up sslocal process | tee /var/log/shadowsocks_supervisor.log
    /usr/bin/python3 /usr/bin/sslocal -c /etc/shadowsocks.json -d start --log-file /var/log/shadowsocks.log
fi

