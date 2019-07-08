package nio;

import com.google.protobuf.MessageLite;

public class TaskEntity {
    private NioChannel channel;
    private MessageLite msg;

    public TaskEntity(NioChannel channel, MessageLite msg) {
        this.channel = channel;
        this.msg = msg;
    }

    public NioChannel getChannel() {
        return channel;
    }

    public MessageLite getMsg() {
        return msg;
    }

    public void setMsg(MessageLite msg) {
        this.msg = msg;
    }
}
