package store.message;

import common.Node;
import common.UtilAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * RaftLog
 * 这个消息全是固定长读.所以不用加消息长度,而是通过校验码判断是否为一个完整的消息
 * index(8) + type(1) + node.size(10) + 4(crc校验码) = 23B
 */
public class RaftLogMessage implements Message {
    public final static byte RAFT_LOG_MESSAGE_SIZE = 8 + 1 + Node.NODE_SERIALIZED_SIZE + 4;
    private static Logger logger = LoggerFactory.getLogger(RaftLogMessage.class);
    private static volatile RaftLogMessage raftLogMessage;
    private byte raftMessageType;
    private long lastAppliedIndex;
    private Node node;

    /**
     * 由于这对象只想当一个容器,所以不想用户反复创建
     */
    private RaftLogMessage() {

    }

    public static RaftLogMessage getInstance(ByteBuffer byteBuffer) {
        RaftLogMessage raftLogMessage = getInstance(null, 0L, (byte) 0);
        return raftLogMessage.decode(byteBuffer);
    }

    public static RaftLogMessage getInstance(Node node,
                                             long index,
                                             byte raftMessageType) {
        if (raftLogMessage == null) {
            synchronized (RaftLogMessage.class) {
                if (raftLogMessage == null) {
                    raftLogMessage = new RaftLogMessage();
                }
            }
        }
        raftLogMessage.setNode(node);
        raftLogMessage.setLastAppliedIndex(index);
        raftLogMessage.setRaftMessageType(raftMessageType);
        return raftLogMessage;
    }

    @Override
    public byte[] encode() {
        if (node == null) {
            logger.warn("RaftLogMessage node is null");
            return null;
        }
        ByteBuffer byteBuffer = ByteBuffer.allocate(getSerializedSize());
        byte[] nodeByte = node.serialized();
        if (nodeByte == null) {
            return null;
        }
        //index
        byteBuffer.putLong(lastAppliedIndex);
        //type
        byteBuffer.put(raftMessageType);
        //node
        byteBuffer.put(nodeByte);

        int crc32 = UtilAll.crc32(byteBuffer.array(), 0, 19);

        //crc32
        byteBuffer.putInt(crc32);

        return byteBuffer.array();
    }

    @Override
    public RaftLogMessage decode(ByteBuffer buffer) {
        if (buffer.limit() - buffer.position() < RaftLogMessage.RAFT_LOG_MESSAGE_SIZE) {
            logger.info("ByteBuffer contain no RaftLogMessage");
            return null;
        }
        byte[] messageByte = new byte[RaftLogMessage.RAFT_LOG_MESSAGE_SIZE - 4];
        //读取message
        buffer.get(messageByte);

        //crc32
        int crc32Code = buffer.getInt();
        int crc32CodeCaculate = UtilAll.crc32(messageByte);

        if (crc32Code != crc32CodeCaculate) {
            logger.info("RaftLogMessage was destroyed!");
            return null;
        }
        //body 8+1+10;
        ByteBuffer body = ByteBuffer.wrap(messageByte);
        long lastAppliedIndex = body.getLong();
        byte raftMessageType = body.get();

        Node node = Node.deSerialized(body);

        return RaftLogMessage.getInstance(node, lastAppliedIndex, raftMessageType);
    }

    @Override
    public short getSerializedSize() {
        return RaftLogMessage.RAFT_LOG_MESSAGE_SIZE;
    }

    public Node getNode() {
        return node;
    }

    public void setNode(Node node) {
        this.node = node;
    }

    public byte getRaftMessageType() {
        return raftMessageType;
    }

    public void setRaftMessageType(byte raftMessageType) {
        this.raftMessageType = raftMessageType;
    }

    public long getLastAppliedIndex() {
        return lastAppliedIndex;
    }

    public void setLastAppliedIndex(long lastAppliedIndex) {
        this.lastAppliedIndex = lastAppliedIndex;
    }

    @Override
    public String toString() {
        return "RaftLogMessage{" +
                "raftMessageType=" + raftMessageType +
                ", lastAppliedIndex=" + lastAppliedIndex +
                ", node=" + node +
                '}';
    }
}
