package core.nio;

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

public abstract class NioServer {
    private static Logger logger = LoggerFactory.getLogger(NioServer.class);
    private final SelectorProvider provider = SelectorProvider.provider();
    private ServerSocketChannel serverChannel;
    private ServerSocket serverSocket;
    private Selector selector;
    /**
     * 监听端口
     */
    private int port;

    protected volatile boolean isShutdown = false;
    protected NioChannelGroup nioChannelGroup = new NioChannelGroup();
    private MessageLite messageLite;

    public NioServer(MessageLite messageLite, int port) {
        this.port = port;
        this.messageLite = messageLite;
    }

    public void init() {
        try {
            serverChannel = ServerSocketChannel.open();
            serverSocket = serverChannel.socket();
            serverSocket.bind(new InetSocketAddress(port));
            serverChannel.configureBlocking(false);
            selector = openSelector();
            serverChannel.register(selector, SelectionKey.OP_ACCEPT);

        } catch (IOException ex) {
            logger.error(ex.toString());
        }

        while (!isShutdown()) {
            try {
                selector.select();
                Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
                while (iterator.hasNext()) {
                    SelectionKey key = iterator.next();
                    iterator.remove();
                    processKey(key);
                }
            } catch (IOException ex) {
                logger.error(ex.toString());
            }
        }

    }

    /**
     * 处理key.
     *
     * @param selectionKey selectionKey
     */
    private void processKey(SelectionKey selectionKey) {
        if (selectionKey.isAcceptable()) {
            processAcceptKey(selectionKey);
        } else if (selectionKey.isReadable()) {
            processReadKey(selectionKey);
        }
    }

    /**
     * 处理read 事件.
     *
     * @param selectionKey key.
     */
    protected abstract void processReadKey(SelectionKey selectionKey);

    /**
     * 处理客户端连接.
     *
     * @param selectionKey key
     */
    private void processAcceptKey(SelectionKey selectionKey) {
        if (!selectionKey.isAcceptable()) {
            return;
        }
        try {
            SocketChannel clientChannel = serverChannel.accept();
            if (clientChannel != null) {
                clientChannel.configureBlocking(false);
                SelectionKey key = clientChannel.register(selector, SelectionKey.OP_READ);
                NioChannel nioChannel = new NioChannel(nioChannelGroup, key, messageLite);
                nioChannelGroup.put(nioChannel);
            }
        } catch (IOException ex) {
            logger.error("accept client error {}", ex);
        }

    }

    /**
     * open selector.
     * @return Selector.
     */
    private Selector openSelector() {
        try {
            return selector = provider.openSelector();
        } catch (IOException e) {
            logger.error("fail to create a new selector {}", e, toString());
        }
        return null;
    }

    /**
     * close.
     */
    public void shudown() {
        isShutdown = true;
        nioChannelGroup.closeAll();
    }

    /**
     * 是否关闭.
     * @return bool
     */
    public boolean isShutdown() {
        return isShutdown;
    }
}
