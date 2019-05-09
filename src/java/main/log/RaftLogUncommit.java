package log;

import java.util.Set;
import java.util.TreeSet;

/**
 * 这个用于存储leader上未提交的日志,等待follower的
 */
public class RaftLogUncommit extends RaftLog {
    /**
     * 这个用于存储收到的确认节点的id
     */
    private Set<Integer> ack = new TreeSet<Integer>();

    public boolean add(int id){
        return ack.add(id);
    }
    public int getSize(){
        return ack.size();
    }

    public Set<Integer> getAck() {
        return ack;
    }

    public void setAck(Set<Integer> ack) {
        this.ack = ack;
    }

    @Override
    public String toString() {
        return super.toString() +
                "ack=" + ack +
                '}';
    }
}
