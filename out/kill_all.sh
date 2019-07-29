#!/usr/bin/env bash
for((i = 1;i<=$1;i++));
do
	path="./artifacts_$i/pid"
	file=$path
	{
		read pid
	}<$file 
	
	kill -9 $pid
	
	#rm -rf ./artifacts_$i
	rm -rf ./artifacts_$i/pid
	#清空异常信息
	#rm -rf ./nohup.out
	#清空日志
	#rm -rf /usr/local/logs/debug.log
done

if (($? == 0));then
	echo "stop success"
else 
	echo "error"
fi
