package log;

import java.util.Set;
import java.util.TreeSet;

/**
 * 这个用于存储leader上未提交的日志,等待follower的
 */
public class RaftLogUnCommit extends RaftLog {
    /**
     * 这个用于存储收到的确认节点的id
     */
    private Set<Short> ack = new TreeSet<Short>();

    public Set<Short> getAck() {
        return ack;
    }

    public void setAck(Set<Short> ack) {
        this.ack = ack;
    }

    @Override
    public String toString() {
        return super.toString() +
                "ack=" + ack +
                '}';
    }
}
