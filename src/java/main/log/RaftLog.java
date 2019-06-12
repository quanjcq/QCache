package log;


public class RaftLog {
    private long timestamp;
    private long index;
    private String command;
    public RaftLog(){

    }
    public RaftLog(long timestamp, long index, String command) {
        this.timestamp = timestamp;
        this.index = index;
        this.command = command;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getIndex() {
        return index;
    }

    public void setIndex(long index) {
        this.index = index;
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
                "timestamp=" + timestamp +
                ", index=" + index +
                ", command='" + command + '\'' +
                '}';
    }
}
