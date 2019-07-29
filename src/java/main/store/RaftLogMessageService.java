package store;

import raft.ConsistentHash;
import common.UtilAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import store.message.RaftLogMessage;

import java.io.IOException;
import java.nio.ByteBuffer;

public class RaftLogMessageService {
    private static Logger logger = LoggerFactory.getLogger(RaftLogMessageService.class);
    private MappedFile mappedFile;
    private String filePath;
    private int fileSize;
    private RaftStateMachineService raftStateMachineService;

    public RaftLogMessageService(String filePath,
                                 int fileSize,
                                 RaftStateMachineService raftStateMachineService){
        this.filePath = filePath;
        this.fileSize = fileSize;
        this.raftStateMachineService = raftStateMachineService;
        try {
            mappedFile = new MappedFile(this.filePath,this.fileSize);
        } catch (IOException e) {
            logger.warn("create mappedFile error {}",e);
            System.exit(1);
        }
        loadRaftLog();

    }

    public RaftLogMessageService(String filePath,
                                 int fileSize){
        this.filePath = filePath;
        this.fileSize = fileSize;
        try {
            mappedFile = new MappedFile(this.filePath,this.fileSize);
        } catch (IOException e) {
            logger.warn("create mappedFile error {}",e);
            System.exit(1);
        }
        loadRaftLog();

    }
    public void setRaftStateMachineService(RaftStateMachineService raftStateMachineService) {
        this.raftStateMachineService = raftStateMachineService;
    }

    /**
     * 找到日志写到了那个位置
     */
    private void loadRaftLog(){
        ByteBuffer byteBuffer = mappedFile.getMappedByteBuffer().slice();
        byteBuffer.limit(fileSize);
        byteBuffer.position(0);
        byte[] body = new byte[19];
        while (byteBuffer.limit() - byteBuffer.position() >= RaftLogMessage.RAFT_LOG_MESSAGE_SIZE){
            //read body
            byteBuffer.get(body);
            int crc32CodeCaculate = UtilAll.crc32(body);

            int crc32Code = byteBuffer.getInt();

            if (crc32Code != crc32CodeCaculate) {
                //不相等,这条信息包括后面都不算完整信息
                mappedFile.setWritePosition(byteBuffer.position() - RaftLogMessage.RAFT_LOG_MESSAGE_SIZE);
                //System.out.println(getRaftLogNum());
                return;
            }else {
                mappedFile.setWritePosition(byteBuffer.position());
                logger.info("RaftLogMessage write position: {}",byteBuffer.position());
            }

        }



    }

    /**
     * 获取总的日志条目.
     * @return int
     */
    public int getRaftLogNum(){
        int size = mappedFile.getWritePosition();
        return size / RaftLogMessage.RAFT_LOG_MESSAGE_SIZE;

    }

    /**
     * 查询指定index后的日志
     * @param lastAppliedIndex index
     * @return RaftLog ByteBuffer 的形式
     */
    public ByteBuffer queryRaftLogMessage(long lastAppliedIndex){
        ByteBuffer byteBuffer = mappedFile.getByteBuffer();
        while (byteBuffer.limit() - byteBuffer.position() >= RaftLogMessage.RAFT_LOG_MESSAGE_SIZE) {
            RaftLogMessage message = RaftLogMessage.getInstance(byteBuffer);
            if (message == null) {
                return null;
            } else if (message.getLastAppliedIndex() == lastAppliedIndex){
                return byteBuffer;
            } else if (message.getLastAppliedIndex() > lastAppliedIndex) {
                byteBuffer.position(byteBuffer.position() - RaftLogMessage.RAFT_LOG_MESSAGE_SIZE);
                return byteBuffer;
            }
        }
        return null;
    }


    public AppendMessageResult appendMessage(RaftLogMessage raftLogMessage,boolean flush) {
        if (raftStateMachineService == null) {
            throw new RuntimeException("raftStateMachineService can not be null!");
        }
        AppendMessageResult appendMessageResult =  this.mappedFile.appendMessage(raftLogMessage,flush);
        if (appendMessageResult.getState() == AppendMessageResult.AppendMessageState.APPEND_MESSAGE_NO_SPACE) {
            //日志文件没有足够空间了,清空该文件,同时保存stateMachine
            logger.info("raft log file have no enough space");
            raftStateMachineService.raftStateMachineStore();
            this.mappedFile.clean();
            return appendMessage(raftLogMessage,flush);
        }

        return appendMessageResult;
    }

    /**
     * 刷盘.
     */
    public void flush(){
        this.mappedFile.flush();
    }

    /**
     * clear.
     */
    public void clear(){
        this.mappedFile.clean();
    }

    /**
     * 将本地的log,应用于StateMachine
     */
    public void loadMessageInStateMachine(ConsistentHash consistentHash){
        ByteBuffer buffer = queryRaftLogMessage(-1);
        if (buffer == null) {
            logger.info("RaftLog was empty");
            return;
        }
        while (buffer.limit() - buffer.position() >= RaftLogMessage.RAFT_LOG_MESSAGE_SIZE) {
            RaftLogMessage message = RaftLogMessage.getInstance(buffer);
            if (message == null) {
                return ;
            } else if (message.getRaftMessageType() == (byte)0) {
                //add
                consistentHash.add(message.getNode());
                logger.info("add node {}",message.getNode());
            } else if (message.getRaftMessageType() == (byte)1) {
                //remove
                consistentHash.remove(message.getNode());
                logger.info("remove node {}",message.getNode());
            }

        }
    }
}
