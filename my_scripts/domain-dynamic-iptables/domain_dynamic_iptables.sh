#!/bin/bash

proxy_domain_list='./proxy_domain_list'

generated_proxy_ip_list='./proxy_ip_list'

function rm_timeout_iptables_redirect() {
    for ip in `cat ${generated_proxy_ip_list}`; do
        echo "remove timeout redirect ip: ${ip}"
        sudo iptables -t nat -D OUTPUT -d ${ip} -p tcp -j REDIRECT --to-ports 12345
    done
}

function wipe_timeout_ips() {
    echo > ${generated_proxy_ip_list}
}

function fetch_current_ips_under_certain_domains() {
    for domain in `cat ${proxy_domain_list}`; do
	echo "dig domain: ${domain}"
	for ip in `dig +short ${domain}`; do
	    echo "dig find ip: ${ip}"
	    # 需要判断是否是一个合法的 ip
	    if [[ ${ip} =~ ^[0-9.]*$ ]]; then
		echo "------ redirect ip: ${ip} ------"
		echo ${ip} >> ${generated_proxy_ip_list}
		sudo iptables -t nat -I OUTPUT -d ${ip} -p tcp -j REDIRECT --to-ports 12345
	    fi
	done
    done
}

# main process

rm_timeout_iptables_redirect

wipe_timeout_ips

fetch_current_ips_under_certain_domains

