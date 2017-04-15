#! /bin/bash
set -e

#上传文件
function importFile(){
	mypath=/home/q/wireless_log/$4
	androidFile=${mypath}/android.$1$2$3.$4.txt
	iphoneFile=${mypath}/iphone.$1$2$3.$4.txt
	
	#remoteUrl=http://transfile.liyun.rocks/dfsImport/wirelesslog/history/
	
	touch ${androidFile}
	touch ${iphoneFile}
	find ${mypath}/* -name 'android*' ! -name '*.gz' ! -name "*.txt"  -type f -exec cat {} \;>${androidFile}
	find ${mypath}/* -name 'iphone*' ! -name '*.gz' ! -name "*.txt"  -type f -exec cat {} \;>${iphoneFile}
	lzop -Uv ${androidFile} ${iphoneFile}

	for f in `find ${mypath} -iname '*.lzo'`;
	do {
       	echo "begin to upload file :$f"
       	upload_ret=`rsync -avh ${f} --port=35280 123.59.183.83::wireless_log_file/history/${year}${month}/`	
	   	echo "upload file :${f} ,result :${upload_ret}"
	}
	done
	#curl  ${remoteUrl} > /dev/null
}

#下载文件
function handleFile(){
	year=$1;
	month=$2;
	day=$3;
	domain=$4
	mypath=/home/q/wireless_log/${domain}
	##同步文件
	rm -rf  ${mypath}/*
	rsync -av --password-file /home/q/rsync.passowrd root@l-mclt1.ops.cn2::CN2_GLFS_WAP_CN2_wap_poster_agent/${domain}/${year}${month}/androidfc.txt.${year}-${month}-${day}*.gz  ${mypath}/ &
	rsync -av --password-file /home/q/rsync.passowrd root@l-mclt1.ops.cn2::CN2_GLFS_WAP_CN2_wap_poster_agent/${domain}/${year}${month}/iphone.txt.${year}-${month}-${day}*.gz  ${mypath}/ &
	wait
	#如果没有文件,则跳过
	if [ 0 = `ls ${mypath}/ |wc -l` ]
	then
		return
	fi
 	echo "开始合并数据"
	ls -l ${mypath}/*.gz | awk -v file_size=0 '{ if ( $5!=file_size ) print  $9}' |xargs sudo gzip -d
    	importFile ${year} ${month} ${day} ${domain}
    	rm -rf  ${mypath}/*
}


year=$(date -d "2 days ago" +%Y)
month=$(date -d "2 days ago" +%m)
day=$(date -d "2 days ago" +%d)

for i in 1 2 3 4 
do {
	domain="l-pitcher${i}.wap.cn2"
	echo "开始下载文件:${year} ${month} ${day} ${domain}"
	handleFile ${year} ${month} ${day} ${domain} 
}
done
	
