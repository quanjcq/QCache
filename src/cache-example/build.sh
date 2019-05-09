#!/usr/bin/env bash
for((i = 1;i<=$1;i++));
do
	if [ ! -d "./artifacts_$i" ];then
		mkdir  ./artifacts_$i/
	fi
	cp -r  ./artifacts/QCache_jar ./artifacts_$i/
	if [ ! -d "./artifacts_$i/conf" ];then
		mkdir  ./artifacts_$i/conf
	fi
	if [ ! -d "./artifacts_$i/logs" ];then
		mkdir  ./artifacts_$i/logs
	fi
	if [ ! -d "./artifacts_$i/snaphot" ];then
		mkdir  ./artifacts_$i/snaphot
	fi
	echo "myid=$i">./artifacts_$i/conf/q.cfg
	for((j=1;j<=$1;j++));
	do 
	     echo "server.$j=127.0.0.1:808$j:909$j">>./artifacts_$i/conf/q.cfg
        done
	nohup java -jar ./artifacts_$i/QCache_jar/QCache.jar runServer >/dev/null 2>&1 &
done
#删除原文件的内容
#rm -rf artifacts
if (($? == 0));then
	echo "start success"
else 
	echo "error"
fi

#

