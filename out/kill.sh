#!/usr/bin/env bash
path="./artifacts_$1/logs/pid"
file=$path
{
	read pid
}<$file 
kill -9 $pid
rm -rf ./artifacts_$1/logs/pid

if (($? == 0));then
	echo "stop success"
else 
	echo "error"
fi
