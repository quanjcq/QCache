package core.message;

import java.io.Serializable;
import java.lang.management.ThreadInfo;

public abstract class RaftMessage implements Serializable {
    enum RaftMessageType{
        VOTE,       //这个消息负责选举用的，                     candidate->node,candidate <-node
        HEART,      //普通的心跳消息，没有数据(有服务器已经提交日志) leader->follower,follower->leader
        ACK,        //确认收到的leader 发送给follower的信息      follower->leader
        SANPHOT,    //传送snaphot 消息                        leader->follower
        COMMIT,     //同步已经提交的日志信息                     leader->follower
        UNCOMMIT,   //未提交的日志                             leader->follower
        PRE_VOTE    //pre_candidate-> node
    }
    private int id;
    private long currentTerm;
    private long lastAppendedIndex;
    private long lastAppendedTerm;
    private boolean isLeader = false;

    public boolean isLeader() {
        return isLeader;
    }

    public void setLeader(boolean leader) {
        isLeader = leader;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public long getCurrentTerm() {
        return currentTerm;
    }

    public void setCurrentTerm(long currentTerm) {
        this.currentTerm = currentTerm;
    }

    public long getLastAppendedIndex() {
        return lastAppendedIndex;
    }

    public void setLastAppendedIndex(long lastAppendedIndex) {
        this.lastAppendedIndex = lastAppendedIndex;
    }

    public long getLastAppendedTerm() {
        return lastAppendedTerm;
    }

    public void setLastAppendedTerm(long lastAppendedTerm) {
        this.lastAppendedTerm = lastAppendedTerm;
    }

    @Override
    public String toString() {
        return  this.getClass().getName() + "{" +
                "id=" + id +
                ", currentTerm=" + currentTerm +
                ", lastAppendedIndex=" + lastAppendedIndex +
                ", lastAppendedTerm=" + lastAppendedTerm +
                ", isLeader=" + isLeader +
                '}';
    }
}
