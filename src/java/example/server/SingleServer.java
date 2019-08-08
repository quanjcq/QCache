package server;

import cache.CacheServer;
import common.Node;
import raft.ConsistentHash;
import recycle.MarkLRU;
import recycle.RecycleService;
import store.AofLogService;
import store.CacheFileGroup;
import store.checkpoint.CheckPoint;

import java.util.ArrayList;
import java.util.List;

/**
 * 单节点程序,非集群
 */
public class SingleServer {
    public static void main(String[] args) {
        Node node = new Node.Builder()
                .setNodeId((short)1)
                .setIp("127.0.0.1")
                .setListenClientPort(9001)
                .setListenHeartbeatPort(8001)
                .build();
        //集群信息
        List<Node> nodes = new ArrayList<Node>();
        nodes.add(node);

        ConsistentHash consistentHash = new ConsistentHash(1);
        CacheServer server = new CacheServer();

        //缓存文件
        String basePath = "/home/jcq/store/cache/";
        int cacheFileSize = 1024 * 1024 * 50;
        CacheFileGroup cacheFileGroup = new CacheFileGroup(basePath,cacheFileSize);

        //checkPoint
        String checkPointPath = "/home/jcq/store/checkpoint/0";
        CheckPoint checkPoint = new CheckPoint(checkPointPath);

        //recycle
        RecycleService recycleService = new RecycleService(new MarkLRU(cacheFileGroup),
                cacheFileGroup,
                server.getCanWrite(),server.getCanRead());

        String aofLogPath = "/home/jcq/store/aof/0";
        int aofLogSize = 1024 * 1204;
        AofLogService aofLogService = new AofLogService(aofLogPath,aofLogSize);


        //自身节点
        server.setMyNode(node);
        server.setNodes(nodes);
        //一致性hash
        server.setConsistentHash(consistentHash);
        server.setCacheFileGroup(cacheFileGroup);
        server.setCheckPoint(checkPoint);
        server.setRecycleService(recycleService);
        server.setAofLogService(aofLogService);



        server.start();


    }
}
