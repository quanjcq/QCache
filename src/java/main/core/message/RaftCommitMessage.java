package core.message;

import log.RaftLog;

import java.util.List;

//同步已经提交的日志信息
public class RaftCommitMessage extends RaftMessage {

    private List<RaftLog> raftLogs;
    public List<RaftLog> getRaftLogs() {
        return raftLogs;
    }

    public void setRaftLogs(List<RaftLog> raftLogs) {
        this.raftLogs = raftLogs;
    }

    @Override
    public String toString() {
        return super.toString() +
                "raftLogs=" + raftLogs +
                '}';
    }
}
