package common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * Created by quan on 2019/4/25
 * 服务器节点.
 */
public final class Node implements Comparable, Serializable {
    private static Logger logger = LoggerFactory.getLogger(Node.class);
    /**
     * 所有节点都有个唯一id.
     */
    private short nodeId;
    /**
     * 节点ip 地址
     */
    private String ip;
    /**
     * 节点负责监听 client的端口
     */
    private int listenClientPort;

    /**
     * node 序列化后的长度
     */
    public final static int NODE_SERIALIZED_SIZE = 10;
    /**
     * client port 到heartbeat port
     */
    public final static int CLIENT_TO_HEART = 1000;
    /**
     * 节点监听Heartbeart端口
     * [这个端口是根据client端口生成的,简化配置,不用配置过多端口信息]
     * listenHeartbeatPort = listenClientPort - 1000
     */
    private int listenHeartbeatPort;


    private Node(Builder builder) {
        this.nodeId = builder.nodeId;
        this.ip = builder.ip;
        this.listenHeartbeatPort = builder.listenHeartbeatPort;
        this.listenClientPort = builder.listenClientPort;
    }

    /**
     * 返回序列化的字节数组.
     * @return byte[],or null if error happened.
     */
    public byte[] serialized(){
        ByteBuffer buffer = ByteBuffer.allocate(getSerializedSize());
        buffer.putShort(nodeId);
        byte[] ipByte = UtilAll.ipToByte(ip);
        if (ipByte == null) {
            return null;
        }
        buffer.put(ipByte);
        buffer.putInt(listenClientPort);
        return buffer.array();
    }

    /**
     * 将字节数组转Node实例;
     * @param buffer buffer
     * @return Node ,错误将返回null
     */
    public static Node deSerialized(ByteBuffer buffer){
        if (buffer.limit() - buffer.position() != Node.NODE_SERIALIZED_SIZE) {
            logger.warn("buffer's limit {} not equal node size  {}",buffer.limit(),Node.NODE_SERIALIZED_SIZE);
            return null;
        }
        //id
        short id = buffer.getShort();
        byte[] ip = new byte[4];
        //ip
        buffer.get(ip);
        String ipString = UtilAll.ipToString(ip);
        //port
        int port = buffer.getInt();
        return new Node.Builder()
                .setNodeId(id)
                .setIp(ipString)
                .setListenClientPort(port)
                .setListenHeartbeatPort(port-Node.CLIENT_TO_HEART)
                .build();
    }

    /**
     * 获取序列化后字节数组长度.
     * @return short.
     */
    private short getSerializedSize(){
        return Node.NODE_SERIALIZED_SIZE;
    }
    public short getNodeId() {
        return nodeId;
    }

    public String getIp() {
        return ip;
    }

    public int getListenHeartbeatPort() {
        return listenHeartbeatPort;
    }

    public int getListenClientPort() {
        return listenClientPort;
    }

    @Override
    public String toString() {
        return String.format("%d:%s:%d:%d", nodeId, ip, listenHeartbeatPort, listenClientPort);
    }

    //id 相等就好了
    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (obj instanceof Node) {
            return nodeId == ((Node) obj).getNodeId();
        }
        return false;
    }

    @Override
    public int hashCode() {
        return nodeId;
    }

    @Override
    public int compareTo(Object o) {
        return this.nodeId - ((Node) o).getNodeId();
    }

    public static class Builder {
        private short nodeId;
        private String ip;
        private int listenHeartbeatPort;
        private int listenClientPort;

        public Builder setNodeId(Short nodeId) {
            this.nodeId = nodeId;
            return this;
        }

        public Builder setIp(String ip) {
            this.ip = ip;
            return this;
        }

        public Builder setListenHeartbeatPort(int listenHeartbeatPort) {
            this.listenHeartbeatPort = listenHeartbeatPort;
            return this;
        }

        public Builder setListenClientPort(int listenClientPort) {
            this.listenClientPort = listenClientPort;
            return this;
        }

        public Node build() {
            return new Node(this);
        }

    }
}
