package store;

public class AppendMessageResult {
    private AppendMessageState state;
    private long appendOffset;
    public AppendMessageResult(){

    }
    public AppendMessageResult(AppendMessageState state) {
        this.state = state;
    }

    public AppendMessageResult(long appendOffset,AppendMessageState state) {
        this.appendOffset = appendOffset;
        this.state = state;
    }

    public AppendMessageState getState() {
        return state;
    }

    public void setState(AppendMessageState state) {
        this.state = state;
        appendOffset = -1;
    }

    public long getAppendOffset() {
        return appendOffset;
    }

    public void setAppendOffset(long appendOffset) {
        this.appendOffset = appendOffset;
    }

    public enum AppendMessageState{
        APPEND_MESSAGE_OK,
        APPEND_MESSAGE_ERROR,
        APPEND_MESSAGE_NO_SPACE,
        //插入消息为null
        APPEND_MESSAGE_MESSAGE_EMPTY,
        //插入消息越界
        APPEND_MESSAGE_OUT_OF_RANGE
    }

    @Override
    public String toString() {
        return "AppendMessageResult{" +
                "state=" + state +
                ", appendOffset=" + appendOffset +
                '}';
    }
}
