package common;

import java.io.Serializable;

/**
 * Created by quan on 2019/4/25
 * 服务器节点
 */
public final class Node implements Comparable, Serializable {
    /**
     * 所有节点都有个唯一id.
     */
    private short nodeId;
    /**
     * 节点ip 地址
     */
    private String ip;
    /**
     * 节点监听Heartbeart端口
     */
    private int listenHeartbeatPort;

    /**
     * 节点负责监听 client的端口
     */
    private int listenClientPort;

    private Node(Builder builder) {
        this.nodeId = builder.nodeId;
        this.ip = builder.ip;
        this.listenHeartbeatPort = builder.listenHeartbeatPort;
        this.listenClientPort = builder.listenClientPort;
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
