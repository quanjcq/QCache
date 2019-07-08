package nio;

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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 处理读事件
 */
public class NioEventLoop implements Runnable {
    private static Logger logger = LoggerFactory.getLogger(NioEventLoop.class);

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

    private final SelectorProvider provider = SelectorProvider.provider();
    private AtomicBoolean wakeUp = new AtomicBoolean(true);
    private NioChannelGroup nioChannelGroup = new NioChannelGroup();
    private MessageLite messageLite;
    private NioWriteGroup nioWriteGroup;
    private volatile boolean isShutdown = false;
    private NioReadGroup nioReadGroup;
    /**
     * 读到的数据放在这里
     */
    private LinkedBlockingQueue<Object> readCache;
    /**
     * The NIO {@link Selector}.
     */
    private Selector selector;

    public NioEventLoop(LinkedBlockingQueue<Object> readCache,
                        NioReadGroup nioReadGroup,
                        NioWriteGroup nioWriteGroup,
                        MessageLite messageLite) {
        this.readCache = readCache;
        this.nioReadGroup = nioReadGroup;
        this.messageLite = messageLite;
        this.nioWriteGroup = nioWriteGroup;
        selector = openSelector();
    }

    @Override
    public void run() {
        while (!isShutdown()) {
            try {
                int num = 0;
                if (wakeUp.get()) {
                    num = selector.select();
                }
                //System.out.println("read" + num);
                if (num <= 0) {
                    continue;
                }
                Set<SelectionKey> keys = selector.selectedKeys();
                Iterator<SelectionKey> iterator = keys.iterator();
                CountDownLatch countDownLatch = new CountDownLatch(keys.size());
                nioWriteGroup.setCountDownLatch(countDownLatch);
                while (iterator.hasNext()) {
                    SelectionKey key = iterator.next();
                    iterator.remove();
                    //System.out.println("interest" + key.interestOps());
                    if (key.isReadable()) {
                        NioChannel nioChannel = nioChannelGroup.findChannel(key);
                        if (nioChannel != null) {
                            nioReadGroup.addTask(nioChannel);
                        } else {
                            countDownLatch.countDown();
                        }
                    } else {
                        countDownLatch.countDown();
                    }
                }
                try {
                    countDownLatch.await();
                } catch (InterruptedException e) {
                    logger.error(e.toString());
                }

            } catch (IOException e) {
                logger.error("select error {}", e);
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
            NioChannel nioChannel = new NioChannel(nioChannelGroup, selectionKey, readCache, messageLite);
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
     * 关闭时间轮循.
     */
    public void shutdown() {
        isShutdown = true;
        nioChannelGroup.closeAll();
    }

    /**
     * run.
     *
     * @param poolExecutor pool
     */
    public void run(ThreadPoolExecutor poolExecutor) {
        poolExecutor.execute(this);
    }
}
