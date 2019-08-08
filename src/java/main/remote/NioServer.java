package remote;

import common.UtilAll;
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

public abstract class NioServer implements Runnable{
    private static Logger logger = LoggerFactory.getLogger(NioServer.class);
    private final SelectorProvider provider = SelectorProvider.provider();
    private ServerSocketChannel serverChannel;
    private ServerSocket serverSocket;
    private Selector selector;
    /**
     * 监听端口
     */
    protected int port;

    protected volatile boolean isShutdown = false;
    protected NioChannelGroup nioChannelGroup = new NioChannelGroup();
    public NioServer(int port) {
        this.port = port;
    }

    public NioServer() {

    }

    public void start(){
        if (!isShutdown()) {
            UtilAll.getThreadPool().execute(this);
            logger.info("Nio server runing");
            //管理连接
            nioChannelGroup.start();
        }
    }

    private void init() {
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

    @Override
    public void run() {
        init();
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
                clientChannel.socket().setTcpNoDelay(true);
                NioChannel nioChannel = new NioChannel(nioChannelGroup, key);
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
            logger.error("fail to create a new selector {}",e);
        }
        return null;
    }

    /**
     * close.
     */
    public void shutdown() {
        isShutdown = true;
        //关闭所有连接
        nioChannelGroup.shutdown();
    }

    /**
     * 是否关闭.
     * @return bool
     */
    public boolean isShutdown() {
        return isShutdown;
    }
}
