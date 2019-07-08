package nio;

import core.message.UserMessageProto;

import java.util.concurrent.locks.LockSupport;

public class ServerTest {
    public static void main(String[] args) {
        NioReadGroup nioReadGroup = new NioReadGroup(3);
        NioWriteGroup nioWriteGroup = new NioWriteGroup(2);
        Server server = new Server();
        server.bind(9097)
                .readGroup(nioReadGroup)
                .writeGroup(nioWriteGroup)
                .messageLite(UserMessageProto.UserMessage.getDefaultInstance())
                .start();

        LockSupport.park();
    }
}
