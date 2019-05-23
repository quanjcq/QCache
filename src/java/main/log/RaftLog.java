package log;

import java.io.Serializable;

public class RaftLog implements Comparable, Serializable {
    private long Timestamp;
    private long index;
    private long term;
    private String command;

    public long getTimestamp() {
        return Timestamp;
    }

    public void setTimestamp(long timestamp) {
        Timestamp = timestamp;
    }

    public long getIndex() {
        return index;
    }

    public void setIndex(long index) {
        this.index = index;
    }

    public long getTerm() {
        return term;
    }

    public void setTerm(long term) {
        this.term = term;
    }

    public String getCommand() {
        return command;
    }

    public void setCommand(String command) {
        this.command = command;
    }

    @Override
    public String toString() {
        return "RaftLog{" +
                "Timestamp=" + Timestamp +
                ", index=" + index +
                ", term=" + term +
                ", command='" + command + '\'' +
                '}';
    }

    public int compareTo(Object o) {
        RaftLog raftLog = (RaftLog) o;
        if (raftLog.getIndex() == this.index && raftLog.getTerm() == this.term) {
            return 0;
        } else if (this.term > raftLog.getTerm()
                || (this.term == raftLog.term && this.index > raftLog.index)) {
            return 1;
        } else {
            return -1;
        }
    }
}
