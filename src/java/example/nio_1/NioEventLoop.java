package nio_1;

import com.google.protobuf.MessageLite;
import io.netty.util.internal.SystemPropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.spi.SelectorProvider;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;

public class NioEventLoop implements Runnable{
    private static Logger logger = LoggerFactory.getLogger(NioEventLoop.class);

    private AtomicBoolean wakeUp = new AtomicBoolean(true);
    // Workaround for JDK NIO bug.
    //
    // See:
    // - http://bugs.sun.com/view_bug.do?bug_id=6427854
    static {
        final String key = "sun.nio.ch.bugLevel";
        final String buglevel = SystemPropertyUtil.get(key);
        if (buglevel == null) {
            try {
                AccessController.doPrivileged(new PrivilegedAction<Void>() {
                    @Override
                    public Void run() {
                        System.setProperty(key, "");
                        return null;
                    }
                });
            } catch (final SecurityException e) {
                logger.debug("Unable to get/set System Property: {}" + key, e);
            }
        }

    }
    private NioEventLoopGroup nioEventLoopGroup;
    private final SelectorProvider provider = SelectorProvider.provider();
    private NioChannelGroup nioChannelGroup = new NioChannelGroup();
    private MessageLite messageLite;
    private volatile boolean isShutdown = false;

    private Selector selector;

    public NioEventLoop(NioEventLoopGroup nioEventLoopGroup,
                        MessageLite messageLite) {
        this.nioEventLoopGroup = nioEventLoopGroup;
        this.messageLite = messageLite;
        selector = openSelector();
    }

    @Override
    public void run() {
        while (!isShutdown()) {
            try {
                int num = 0;
                if (wakeUp.get()){
                    num = selector.select();
                }
                //System.out.println("read" + num);
                if (num <= 0) {
                    continue;
                }
                Set<SelectionKey> keys = selector.selectedKeys();
                Iterator<SelectionKey> iterator = keys.iterator();
                while (iterator.hasNext()) {
                    SelectionKey key = iterator.next();
                    iterator.remove();
                    processReadKey(key);
                }

            } catch (IOException e) {
                logger.error("select error {}", e);
            }
        }
    }

    /**
     * 处理read 事件.
     * @param selectionKey key.
     */
    private void processReadKey(SelectionKey selectionKey){
        NioChannel nioChannel = nioChannelGroup.findChannel(selectionKey);
        if (nioChannel == null) {
            logger.info("Channel already closed!");
        } else {
            Object obj = nioChannel.read();
            if (obj == null) {
                return;
            }
            synchronized (nioEventLoopGroup.getSync()) {
                if (obj instanceof UserMessageProto.UserMessage) {
                    UserMessageProto.UserMessage msg = (UserMessageProto.UserMessage) obj;
                    //System.out.println("message");
                    if (msg.getMessageType() == UserMessageProto.MessageType.GET) {
                        UserMessageProto.UserMessage message = UserMessageProto.UserMessage.newBuilder()
                                .setMessageType(UserMessageProto.MessageType.RESPONSE)
                                .setResponseMessage(UserMessageProto.ResponseMessage
                                        .newBuilder()
                                        .setVal("test")
                                        .setResponseType(UserMessageProto.ResponseType.SUCCESS)
                                        .build()
                                ).build();
                        //System.out.println("get");
                        nioChannel.write(message);
                    } else if (msg.getMessageType() == UserMessageProto.MessageType.SET) {
                        System.out.println("set");
                    } else if (msg.getMessageType() == UserMessageProto.MessageType.DEL) {
                        System.out.println("del");
                    } else if (msg.getMessageType() == UserMessageProto.MessageType.STATUS) {
                        System.out.println("status");
                    }

                }
            }
        }
    }

    private Selector openSelector() {
        try {
            return provider.openSelector();
        } catch (IOException e) {
            logger.error("fail to create a new selector {}", e, toString());
        }
        return null;
    }

    /**
     * 将channel 这测到该selector.
     *
     * @param ch          channel
     * @param interestOps interestOps
     */
    public void register(final NioChannel ch, final int interestOps) {
        if (ch == null) {
            throw new NullPointerException("ch");
        }
        if (interestOps == 0) {
            throw new IllegalArgumentException("interestOps must be non-zero.");
        }

        if (isShutdown()) {
            throw new RuntimeException("event loop shut down");
        }

        try {
            ch.channel().register(selector, interestOps);
        } catch (ClosedChannelException e) {
            logger.error("register error! {}", e);
        }


    }


    /**
     * 将channel 注册到该selector.
     *
     * @param channel     channel;
     * @param interestOps interestOps
     */
    public void register(final SelectableChannel channel, final int interestOps) {
        if (channel == null) {
            throw new NullPointerException("channel can not be empty");
        }
        if (interestOps == 0) {
            throw new IllegalArgumentException("interestOps must be non-zero.");
        }

        if (isShutdown()) {
            throw new RuntimeException("event loop shut down");
        }

        try {
            wakeUp.set(false);
            selector.wakeup();
            SelectionKey selectionKey = channel.register(selector, interestOps);
            wakeUp.set(true);
            NioChannel nioChannel = new NioChannel(nioChannelGroup,selectionKey,messageLite);
            nioChannelGroup.put(nioChannel);
        } catch (ClosedChannelException e) {
            logger.error("register error! {}", e);
        }

    }

    /**
     * 是否关闭.
     *
     * @return bool
     */
    public boolean isShutdown() {
        return isShutdown;
    }


    /**
     * 关闭.
     */
    public void shutdown() {
        isShutdown = true;
        //nioChannelGroup.closeAll();
    }

    /**
     * run.
     * @param poolExecutor pool
     */
    public void run(ThreadPoolExecutor poolExecutor){
        poolExecutor.execute(this);
    }
}
