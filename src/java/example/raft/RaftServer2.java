package raft;

import common.Node;
import store.RaftLogMessageService;
import store.RaftStateMachineService;
import store.checkpoint.CheckPoint;

import java.util.LinkedList;
import java.util.List;

public class RaftServer2 {
    public static void main(String[] args) {
        RaftServer raftServer = new RaftServer();
        List<Node> nodes = new LinkedList<Node>();
        for (int i = 0;i<3;i++) {
            Node node = new Node.Builder()
                    .setNodeId((short)(i + 1))
                    .setIp("127.0.0.1")
                    .setListenClientPort(9000 + i)
                    .setListenHeartbeatPort(8000 + i)
                    .build();
            nodes.add(node);
        }
        Node myNode = nodes.get(1);
        ConsistentHash consistentHash = new ConsistentHash(1);

        //checkPoint
        String checkPointFilePath = "/home/jcq/store/1/0";
        CheckPoint checkPoint = new CheckPoint(checkPointFilePath);

        //raftLog
        String raftLogFilePath = "/home/jcq/store/1/00";
        int raftLogFileSize = 1024 * 1024; //1m
        RaftLogMessageService raftLogMessageService = new RaftLogMessageService(raftLogFilePath,raftLogFileSize);

        //state machine
        String raftStateFilePath = "/home/jcq/store/1/000";
        int raftStateFileSize = 1024 * 1024;//1m
        RaftStateMachineService raftStateMachineService = new RaftStateMachineService(raftStateFileSize,raftStateFilePath,consistentHash);
        raftLogMessageService.setRaftStateMachineService(raftStateMachineService);

        raftServer.setNodes(nodes);
        raftServer.setMyNode(myNode);
        raftServer.setCheckPoint(checkPoint);
        raftServer.setConsistentHash(consistentHash);
        raftServer.setRaftLogMessageService(raftLogMessageService);
        raftServer.setRaftStateMachineService(raftStateMachineService);


        raftServer.start();
    }
}
