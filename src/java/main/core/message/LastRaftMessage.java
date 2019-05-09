package core.message;

/**
 * 维护着，每个节点最新提交的日志信息
 * leader根据这个信息，决定发送给服务器的信息类型
 */
public class LastRaftMessage {
    private int id; //node id
    // 已知的最大的已经被提交的日志条目的索引值
    private long commitIndex;
    // 已知的已经被提交的日志条目的term
    private long commitTerm;

    public LastRaftMessage(int id, long commitIndex, long commitTerm) {
        this.id = id;
        this.commitIndex = commitIndex;
        this.commitTerm = commitTerm;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public long getCommitIndex() {
        return commitIndex;
    }

    public void setCommitIndex(long commitIndex) {
        this.commitIndex = commitIndex;
    }

    public long getCommitTerm() {
        return commitTerm;
    }

    public void setCommitTerm(long commitTerm) {
        this.commitTerm = commitTerm;
    }
}
