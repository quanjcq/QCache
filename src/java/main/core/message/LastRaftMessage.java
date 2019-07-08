package core.message;

/**
 * 维护着，每个节点最新提交的日志信息
 * leader根据这个信息，决定发送给服务器的信息类型
 */
public class LastRaftMessage {
    private short id; //node id
    // 已知的最大的已经被提交的日志条目的索引值
    private long commitIndex;

    public LastRaftMessage(short id, long commitIndex) {
        this.id = id;
        this.commitIndex = commitIndex;
    }

    public short getId() {
        return id;
    }

    public void setId(short id) {
        this.id = id;
    }

    public long getCommitIndex() {
        return commitIndex;
    }

    public void setCommitIndex(long commitIndex) {
        this.commitIndex = commitIndex;
    }
}
