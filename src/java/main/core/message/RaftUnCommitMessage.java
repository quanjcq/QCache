package core.message;

import log.RaftLog;

//传送未提交日志

public class RaftUnCommitMessage extends RaftMessage {
    private RaftLog raftLog;

    public RaftLog getRaftLog() {
        return raftLog;
    }

    public void setRaftLog(RaftLog raftLog) {
        this.raftLog = raftLog;
    }

    @Override
    public String toString() {
        return super.toString() +
                "raftLog=" + raftLog +
                '}';
    }
}
