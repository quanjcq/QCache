package core.message;

/**
 * 未提交日志已经拷贝到follower消息确认
 */
public class RaftAckMessage extends RaftMessage {
    private long ackIndex;
    private long ackTerm;

    public long getAckIndex() {
        return ackIndex;
    }

    public void setAckIndex(long ackIndex) {
        this.ackIndex = ackIndex;
    }

    public long getAckTerm() {
        return ackTerm;
    }

    public void setAckTerm(long ackTerm) {
        this.ackTerm = ackTerm;
    }

    @Override
    public String toString() {
        return super.toString() +
                "ackIndex=" + ackIndex +
                ", ackTerm=" + ackTerm +
                '}';
    }
}
