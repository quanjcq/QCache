import org.junit.Test;
import raft.ConsistentHash;
import store.RaftLogMessageService;
import store.RaftStateMachineService;

public class RaftStateMachineServiceTest {
    @Test
    public void raftStateMachineStore(){
        int fileSize = 1024 * 1024;
        String filePath = "/home/jcq/store/0/1";
        ConsistentHash consistentHash = new ConsistentHash(1);

        int size = 1024 * 1024;
        String path = "/home/jcq/store/0000000";
        RaftStateMachineService raftStateMachineService = new RaftStateMachineService(fileSize,filePath,consistentHash);

        RaftLogMessageService raftLogMessageService = new RaftLogMessageService(path,size);
        raftLogMessageService.loadMessageInStateMachine(consistentHash);

        System.out.println(consistentHash.getNodesStr());


        //存储
        boolean flag = raftStateMachineService.raftStateMachineStore();
        System.out.println(flag);
    }
    @Test
    public void loadRaftStateFile(){
        int fileSize = 1024 * 1024;
        String filePath = "/home/jcq/store/0/1";
        ConsistentHash consistentHash = new ConsistentHash(1);
        RaftStateMachineService raftStateMachineService = new RaftStateMachineService(fileSize,filePath,consistentHash);
        raftStateMachineService.loadRaftStateFile();
        System.out.println(consistentHash.getNodesStr());
        System.out.println(consistentHash.getSize());
    }

}
