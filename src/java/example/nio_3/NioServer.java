package nio_3;

import core.message.UserMessageProto;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class NioServer {
    public static void main(String[] args) {
        NioEventLoopGroup nioEventLoopGroup = new NioEventLoopGroup(2);
        nioEventLoopGroup.setMessageLite(UserMessageProto.UserMessage.getDefaultInstance());

        AcceptorClientHandler acceptorClientHandler = new AcceptorClientHandler(nioEventLoopGroup,9097);
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(8,18,60, TimeUnit.MILLISECONDS,new LinkedBlockingQueue<Runnable>());

        acceptorClientHandler.run(threadPoolExecutor);
        nioEventLoopGroup.run(threadPoolExecutor);

    }
}
