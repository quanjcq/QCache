package nio;

import com.google.protobuf.MessageLite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * 处理客户端连接的
 */
public class AcceptorClientHandler implements Runnable {
    private static Logger logger = LoggerFactory.getLogger(AcceptorClientHandler.class);
    private final SelectorProvider provider = SelectorProvider.provider();
    private NioChannelGroup nioChannelGroup = new NioChannelGroup();
    private MessageLite messageLite;
    private NioWriteGroup nioWriteGroup;
    private volatile boolean isShutdown = false;
    private NioReadGroup nioReadGroup;
    private LinkedBlockingQueue<Object> readCache;
    private Selector selector;
    private int port;

    public AcceptorClientHandler(NioReadGroup nioReadGroup,
                                 NioWriteGroup nioWriteGroup,
                                 MessageLite messageLite,
                                 LinkedBlockingQueue<Object> readCache,
                                 int port) {
        this.nioReadGroup = nioReadGroup;
        this.nioWriteGroup = nioWriteGroup;
        this.messageLite = messageLite;
        this.readCache = readCache;
        selector = openSelector();
        this.port = port;

    }

    private Selector openSelector() {
        try {
            return provider.openSelector();
        } catch (IOException e) {
            logger.error("fail to create a new selector {}", e, toString());
        }
        return null;
    }

    @Override
    public void run() {
        try {
            ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
            ServerSocket serverSocket = serverSocketChannel.socket();
            serverSocket.bind(new InetSocketAddress(port));
            serverSocketChannel.configureBlocking(false);
            //接收连接请求
            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
            while (!isShutdown) {
                try {
                    int num = selector.select();
                    if (num <= 0) {
                        continue;
                    }
                    Set<SelectionKey> keySet = selector.selectedKeys();
                    CountDownLatch countDownLatch = new CountDownLatch(keySet.size());
                    nioWriteGroup.setCountDownLatch(countDownLatch);
                    Iterator<SelectionKey> iterator = keySet.iterator();
                    while (iterator.hasNext()) {
                        SelectionKey key = iterator.next();
                        if (key.isAcceptable()) {
                            SocketChannel socketChannel = serverSocketChannel.accept();

                            if (socketChannel != null) {
                                socketChannel.configureBlocking(false);
                                SelectionKey selectionKey = socketChannel.register(selector, SelectionKey.OP_READ);
                                NioChannel nioChannel = new NioChannel(nioChannelGroup, selectionKey, readCache, messageLite);
                                nioChannelGroup.put(nioChannel);
                            }
                            countDownLatch.countDown();
                        } else if (key.isReadable()) {
                            NioChannel nioChannel = nioChannelGroup.findChannel(key);
                            if (nioChannel == null) {
                                countDownLatch.countDown();
                                logger.error("NioChannel already closed!");
                            } else {
                                nioReadGroup.addTask(nioChannel);
                            }
                        }
                        iterator.remove();
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
        } catch (IOException e) {
            logger.error(e.toString());
        }
    }

    /**
     * close.
     */
    public void shutdown() {
        isShutdown = true;
        nioChannelGroup.closeAll();
        try {
            selector.close();
        } catch (IOException e) {
            logger.error("can not cancel selector {}", e);
        }
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
