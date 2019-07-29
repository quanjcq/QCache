#!/usr/bin/env bash
path="./artifacts_$1/pid"
file=$path
{
	read pid
}<$file 
kill -9 $pid
rm -rf ./artifacts_$1/pid

if (($? == 0));then
	echo "stop success"
else 
	echo "error"
fi
