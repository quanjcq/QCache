package store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.ConsistentHash;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * 将内存中的StateMachine 持久化磁盘中
 */
public class RaftStateMachineService {
    private static Logger logger = LoggerFactory.getLogger(RaftStateMachineService.class);
    private int fileSize;

    private MappedFile mappedFile;
    private ConsistentHash consistentHash;

    public RaftStateMachineService(int fileSize, String filePath,ConsistentHash consistentHash){
        this.fileSize = fileSize;
        this.consistentHash = consistentHash;
        try {
            this.mappedFile = new MappedFile(filePath,this.fileSize);
            //mappedFile.setWritePosition(this.fileSize);
        } catch (IOException ex) {
            logger.error("create RaftStateMachine MappedFile error: {}",ex);
            System.exit(1);
        }
    }

    /**
     * 持久化.
     * @return bool.
     */
    public boolean raftStateMachineStore (){
        ByteBuffer buffer = consistentHash.serializedConsistentHash();
        if (buffer == null) {
            logger.warn("serialized consistentHash error!");
            return false;
        }
        if (buffer.capacity() > fileSize) {
            logger.info("raftStateMachine's file have no enough space");
            return false;
        }
        AppendMessageResult appendMessageResult = mappedFile.appendMessage(buffer.array(),true);

        //System.out.println(appendMessageResult);

        return AppendMessageResult.AppendMessageState.APPEND_MESSAGE_OK == appendMessageResult.getState();
    }

    /**
     * 将持久化的应用于StateMachine
     *
     */
    public void loadRaftStateFile(){
        ByteBuffer buffer = mappedFile.getByteBuffer();
        buffer.limit(buffer.capacity());
        buffer.position(0);
        consistentHash.add(buffer);
    }
}
