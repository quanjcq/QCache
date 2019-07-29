package raft;

import common.Node;

import java.util.Set;
import java.util.TreeSet;

/**
 * 这个用于存储leader上未提交的日志,等待follower确认
 */
public class RaftUnCommitLog{
    private long index;
    //0 add,1 remove;
    private byte type;
    private Node node;
    /**
     * 这个用于存储收到的确认节点的id
     */
    private Set<Short> ack = new TreeSet<Short>();

    public Set<Short> getAck() {
        return ack;
    }


    public long getIndex() {
        return index;
    }

    public void setIndex(long index) {
        this.index = index;
    }

    public byte getType() {
        return type;
    }

    public void setType(byte type) {
        this.type = type;
    }

    public Node getNode() {
        return node;
    }

    public void setNode(Node node) {
        this.node = node;
    }

    @Override
    public int hashCode() {
        return (int)index;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) return false;
        if (this == o) return true;
        if (!(o instanceof RaftUnCommitLog)) return false;

        return type == ((RaftUnCommitLog)o).getType()
                && this.getNode().equals(((RaftUnCommitLog)o).getNode());


    }

    @Override
    public String toString() {
        return "RaftUnCommitLog{" +
                "index=" + index +
                ", type=" + type +
                ", node=" + node +
                ", ack=" + ack +
                '}';
    }
}
