#!/usr/bin/env bash
if [ ! -d "./artifacts_$i" ];then
    echo "只能开启之前build后，自己关闭的节点"
    exit;
fi
nohup java -jar ./artifacts_$1/QCache_jar/QCache.jar runServer >/dev/null 2>&1 &

if (($? == 0));then
	echo "stop success"
else 
	echo "error"
fi
