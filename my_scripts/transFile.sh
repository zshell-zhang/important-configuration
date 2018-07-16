#! /bin/bash  
set -e

first=$1  
second=$2  
domain=$3
mypath=/home/q/wireless_log/${domain}

#上传文件
function importFile(){
	
	androidFile=${mypath}/android.$1$2$3.${domain}.txt
	iphoneFile=${mypath}/iphone.$1$2$3.${domain}.txt
	remoteUrl=http://transfile.liyun.rocks/dfsImport/wirelesslog/history/
	
	touch ${androidFile}
	touch ${iphoneFile}
	find ${mypath}/* -name 'android*' ! -name '*.gz' ! -name "*.txt"  -type f -exec cat {} \;>${androidFile}
	find ${mypath}/* -name 'iphone*' ! -name '*.gz' ! -name "*.txt"  -type f -exec cat {} \;>${iphoneFile}
	
	#重新压缩
	lzop -Uv ${androidFile} ${iphoneFile}

	#上传文件
	for f in `find ${mypath} -iname '*.lzo'`;
	do {
       	echo "begin to upload file :$f"
       	upload_ret=`rsync -z --bwlimit=2000  -avh ${f} --port=35280 123.59.183.83::wireless_log_file/history/${year}${month}/`		
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
	##同步文件
	rsync -av --password-file /home/q/rsync.passowrd root@l-mclt1.ops.cn2::CN2_GLFS_WAP_CN2_wap_poster_agent/${domain}/${year}${month}/androidfc.txt.${year}-${month}-${day}*.gz  ${mypath}/ &
	rsync -av --password-file /home/q/rsync.passowrd root@l-mclt1.ops.cn2::CN2_GLFS_WAP_CN2_wap_poster_agent/${domain}/${year}${month}/iphone.txt.${year}-${month}-${day}*.gz  ${mypath}/ &
	wait
	
	
	#解压缩
	ls -l ${mypath}/*.gz | awk -v file_size=0 '{ if ( $5!=file_size ) print  $9}' |xargs sudo gzip -d
	
   	#写入hdsf
    	importFile ${year} ${month} ${day}

    	rm -rf  ${mypath}/*
}

#入口
rm -rf  ${mypath}/*
while [ "$first" -lt "$second" ]
do
    echo "handle day:${first}" 
	month=$(date -d ${first} +%m)
	if [ ${month} -eq  6 ] ; then
		let first=$(date -d "-1 months ago ${first}" "+%Y%m%d")
		continue
	fi
	day=$(date -d ${first} +%d)
	handleFile 2016 ${month} ${day}
	let first=$(date -d "-1 days ago ${first}" "+%Y%m%d")
done 

