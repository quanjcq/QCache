import raft.ConsistentHash;
import common.Node;
import org.junit.Test;
import store.AppendMessageResult;
import store.RaftLogMessageService;
import store.RaftStateMachineService;
import store.message.RaftLogMessage;

import java.nio.ByteBuffer;

public class RaftLogMessageServiceTest {
    /**
     * 追加消息测试
     */
    @Test
    public void appendMessageTest(){
        int fileSize = 1024 * 1024;//1m文件
        String filePath = "/home/jcq/store/0000000";
        RaftLogMessageService raftLogMessageService = new RaftLogMessageService(filePath,fileSize);
        ConsistentHash consistentHash = new ConsistentHash(1);
        raftLogMessageService.loadMessageInStateMachine(consistentHash);
        System.out.println(consistentHash.getSize());
        System.out.println(consistentHash.getNodesStr());
        int size = 1024 * 1024 * 10;
        String path = "/home/jcq/store/000000010";
        RaftStateMachineService raftStateMachineService = new RaftStateMachineService(size,path,consistentHash);
        raftLogMessageService.setRaftStateMachineService(raftStateMachineService);

        // add
        for (int i = 0;i<400;i++) {
            Node node = new Node.Builder()
                    .setIp("127.0.0.1")
                    .setNodeId((short)i)
                    .setListenHeartbeatPort(3000 + i)
                    .setListenClientPort(4000 + i)
                    .build();
            //add
            RaftLogMessage raftLogMessage = RaftLogMessage.getInstance(node,i ,(byte)0);
            AppendMessageResult result = raftLogMessageService.appendMessage(raftLogMessage,false);
            System.out.println(result);
        }
        /*//remove
        for (int i = 0;i<100;i++) {
            Node node = new Node.Builder()
                    .setIp("127.0.0.1")
                    .setNodeId((short)i)
                    .setListenHeartbeatPort(3000 + i)
                    .setListenClientPort(4000 + i)
                    .build();

            RaftLogMessage raftLogMessage = RaftLogMessage.getInstance(node,i ,(byte)1);
            AppendMessageResult result = raftLogMessageService.appendMessage(raftLogMessage,false);
            System.out.println(result);
        }*/
        raftLogMessageService.flush();

    }

    @Test
    public void queryTest(){
        int fileSize = 1024 * 1024;//1m文件
        String filePath = "/home/jcq/store/2/1";
        RaftLogMessageService raftLogMessageService = new RaftLogMessageService(filePath,fileSize);
        ByteBuffer byteBuffer = raftLogMessageService.queryRaftLogMessage(-1L);
        while (byteBuffer.limit() - byteBuffer.position() >= RaftLogMessage.RAFT_LOG_MESSAGE_SIZE) {
            RaftLogMessage message = RaftLogMessage.getInstance(byteBuffer);
            System.out.println(message);
        }
        System.out.println(raftLogMessageService.getRaftLogNum());

    }

    @Test
    public void loadMessageInStateMachineTest(){
        int fileSize = 1024 * 20;//10m文件
        String filePath = "/home/jcq/store/1/1";
        RaftLogMessageService raftLogMessageService = new RaftLogMessageService(filePath,fileSize);
        ConsistentHash consistentHash  = new ConsistentHash(1);
        raftLogMessageService.loadMessageInStateMachine(consistentHash);
        System.out.println(consistentHash.getNodesStr());
        assert consistentHash.getSize() == 0;

    }
}
