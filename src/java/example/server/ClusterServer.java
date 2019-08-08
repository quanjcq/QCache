package server;

import cache.CacheServer;
import common.Node;
import constant.CacheOptions;
import raft.ConsistentHash;
import recycle.MarkExpire;
import recycle.RecycleService;
import store.*;
import store.checkpoint.CheckPoint;

import java.util.LinkedList;
import java.util.List;

public class ClusterServer {
    public static void main(String[] args) {
        List<Node> nodes = new LinkedList<Node>();
        for (int i = 0;i<3;i++) {
            Node node = new Node.Builder()
                    .setNodeId((short)(i + 1))
                    .setIp("127.0.0.1")
                    .setListenClientPort(9010 + i)
                    .setListenHeartbeatPort(8010 + i)
                    .build();
            nodes.add(node);
        }
        Node myNode = nodes.get(0);
        ConsistentHash consistentHash = new ConsistentHash(CacheOptions.numberOfReplicas);

        //checkPoint
        String checkPointFilePath = "/home/jcq/store/0/0";
        CheckPoint checkPoint = new CheckPoint(checkPointFilePath);

        //raftLog
        String raftLogFilePath = "/home/jcq/store/0/00";
        int raftLogFileSize = 1024 * 1024; //1m
        RaftLogMessageService raftLogMessageService = new RaftLogMessageService(raftLogFilePath,raftLogFileSize);

        //state machine
        String raftStateFilePath = "/home/jcq/store/0/000";
        int raftStateFileSize = 1024 * 1024;//1m
        RaftStateMachineService raftStateMachineService = new RaftStateMachineService(raftStateFileSize,raftStateFilePath,consistentHash);
        raftLogMessageService.setRaftStateMachineService(raftStateMachineService);


        CacheServer server = new CacheServer();

        //缓存文件
        int cacheFileSize = 1024 * 1024 * 50;
        String cacheFilesPath = "/home/jcq/store/0/cacheFiles/";
        CacheFileGroup cacheFileGroup = new CacheFileGroup(cacheFilesPath,cacheFileSize);

        //recycle
        RecycleService recycleService = new RecycleService(new MarkExpire(cacheFileGroup),
                cacheFileGroup,
                server.getCanWrite(),
                server.getCanRead());

        String aofLogServicePath = "/home/jcq/store/0/0000";
        int cacheAofLogSize = 1024 * 1204;
        AofLogService aofLogService = new AofLogService(aofLogServicePath,cacheAofLogSize);
        //刷盘服务
        AsyncFlushService asyncFlushService = new AsyncFlushService(cacheFileGroup,checkPoint);

        //自身节点
        server.setMyNode(myNode);
        server.setNodes(nodes);
        //一致性hash
        server.setConsistentHash(consistentHash);
        server.setCacheFileGroup(cacheFileGroup);
        server.setCheckPoint(checkPoint);
        server.setRecycleService(recycleService);
        server.setAofLogService(aofLogService);
        server.setRaftLogMessageService(raftLogMessageService);
        server.setRaftStateMachineService(raftStateMachineService);
        server.setAsyncFlushService(asyncFlushService);

        //启动server
        server.start();
    }
}
