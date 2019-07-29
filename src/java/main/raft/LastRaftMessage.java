package raft;

/**
 * 维护着，每个节点最新提交的日志信息
 * leader根据这个信息，决定发送给服务器的信息类型
 */
public class LastRaftMessage {
    private short id; //node id
    // 已知的最大的已经被提交的日志条目的索引值
    private long lastAppliedIndex;

    public LastRaftMessage(short id, long lastAppliedIndex) {
        this.id = id;
        this.lastAppliedIndex = lastAppliedIndex;
    }

    public short getId() {
        return id;
    }

    public void setId(short id) {
        this.id = id;
    }

    public long getLastAppliedIndex() {
        return lastAppliedIndex;
    }

    public void setLastAppliedIndex(long lastAppliedIndex) {
        this.lastAppliedIndex = lastAppliedIndex;
    }
}
