#!/bin/bash

command=$1

if [[ ${command}x = 'onx' ]]; then
    iptables -t nat -A PREROUTING -d 127.0.0.0/24 -j RETURN
    iptables -t nat -A PREROUTING -d 192.168.0.0/16 -j RETURN
    iptables -t nat -A PREROUTING -d 10.42.0.0/16 -j RETURN
    iptables -t nat -A PREROUTING -p tcp -s 10.42.0.0/16 -j REDIRECT --to-ports 12345
    echo 'laptop wifi use redsocks redirecting packages to shadowsocks-client!'
elif [[ ${command}x = 'offx' ]]; then
    iptables -t nat -D PREROUTING -d 127.0.0.0/24 -j RETURN
    iptables -t nat -D PREROUTING -d 192.168.0.0/16 -j RETURN
    iptables -t nat -D PREROUTING -d 10.42.0.0/16 -j RETURN
    iptables -t nat -D PREROUTING -p tcp -s 10.42.0.0/16 -j REDIRECT --to-ports 12345
    echo 'laptop wifi stop redirecting packages to shadowsocks-client!'
fi

