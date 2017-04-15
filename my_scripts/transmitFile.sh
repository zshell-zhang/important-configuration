#!/bin/bash

function TraversalCopy(){
   j=0
   file_array=()
   for i in `sudo -u insdev bin/hadoop fs -ls $1 | awk '{print $8}'`
   do
     file_array[j]=$i
     j=`expr $j + 1`
   done
   size=0
   totalSize=0
   i=0
   while [ $i -lt $j ]
   do 
      file=${file_array[$i]}
      size=`sudo -u insdev bin/hadoop fs -ls $file | awk {'print $5'}`
      totalSize=`expr $totalSize + $size`
      if [ $totalSize -gt $maxSize ]
      then
        printf "content over size, index: <%s> \n" $file | tee -a ~/log
        processFile
        size=0
        totalSize=0
      else
        sudo -u insdev bin/hadoop fs -get $file /home/q/sync_data/
        printf "get file from hdfs complete : <%s> \n" $file | tee -a ~/log
        let i+=1
      fi
   done 
   processFile
}

function processFile(){
   printf "begin process file ------------------- \n" | tee -a ~/log
   printf "process file: <%s> \n" `ls /home/q/sync_data/` | tee -a ~/log
   sudo rsync -zavh --port=35280 /home/q/sync_data/* 123.59.183.83::wireless_log_file/device_info/ods_username_uid
   printf "process file rsync complete \n" | tee -a ~/log
   sudo rm /home/q/sync_data/*
   printf "process file rm complete \n" | tee -a ~/log
   printf "end process file ------------------\n" | tee -a ~/log
}

url="/home/q/hadoop/hadoop-2.5.0-cdh5.2.0/"
urlOne="hdfs://qunarcluster/user/ins/device_info/raw_data/new_user_lastest/20170116202842"
urlTwo="hdfs://qunarcluster/user/ins/device_info/raw_data/ods_client_ios_idfa_new/20170116202842"
urlThree1="hdfs://qunarcluster/user/ins/device_info/raw_data/ods_device_info/20170116202842/platform=adr"
urlThree2="hdfs://qunarcluster/user/ins/device_info/raw_data/ods_device_info/20170116202842/platform=ios"
urlFour="hdfs://qunarcluster/user/ins/device_info/raw_data/ods_username_uid/20170116202842"
urlTest="/home/"
maxSize=`expr 1073741824 \* 10`

for loop in $urlFour
do
cd $url
TraversalCopy $loop
printf "-------------finish this directory : <%s> , oh yeah! \n" $loop | tee -a ~/log
done