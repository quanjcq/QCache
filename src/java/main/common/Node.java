package common;

import java.io.Serializable;

/**
 * Created by quan on 2019/4/25
 * Node
 */
public class Node implements Serializable, Comparable {
    /**
     * 所有节点都有个唯一id
     */
    private int NodeId;
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
    private int ListenClientPort;

    public Node(int nodeId, String ip, int listenHeartbeatPort, int listenClientPort) {
        NodeId = nodeId;
        this.ip = ip;
        this.listenHeartbeatPort = listenHeartbeatPort;
        ListenClientPort = listenClientPort;
    }

    public int getListenHeartbeatPort() {
        return listenHeartbeatPort;
    }

    public void setListenHeartbeatPort(int listenHeartbeatPort) {
        this.listenHeartbeatPort = listenHeartbeatPort;
    }

    public int getNodeId() {
        return NodeId;
    }

    public void setNodeId(int nodeId) {
        NodeId = nodeId;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }


    public int getListenClientPort() {
        return ListenClientPort;
    }

    public void setListenClientPort(int listenClientPort) {
        ListenClientPort = listenClientPort;
    }

    @Override
    public String toString() {
        return "Node{" +
                "NodeId=" + NodeId +
                ", ip='" + ip + '\'' +
                ", listenHeartbeatPort=" + listenHeartbeatPort +
                ", ListenClientPort=" + ListenClientPort +
                '}';
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
            return NodeId == ((Node) obj).getNodeId();
        }
        return false;
    }

    @Override
    public int hashCode() {
        return NodeId;
    }

    public int compareTo(Object o) {
        return this.NodeId - ((Node) o).getNodeId();
    }
}
