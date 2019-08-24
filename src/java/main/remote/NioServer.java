package remote;

import common.UtilAll;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.ReflectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.security.AccessController;
import java.security.PrivilegedAction;

public abstract class NioServer implements Runnable {
    private static Logger logger = LoggerFactory.getLogger(NioServer.class);
    private final SelectorProvider provider = SelectorProvider.provider();
    /**
     * 监听端口
     */
    protected int port;
    protected volatile boolean isShutdown = false;
    protected NioChannelGroup nioChannelGroup = new NioChannelGroup();
    private ServerSocketChannel serverChannel;
    private ServerSocket serverSocket;
    private Selector selector;
    private SelectedSelectionKeySet selectionKeys;

    public NioServer(int port) {
        this.port = port;
    }

    public NioServer() {

    }

    public void start() {
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
                for (int i = 0; i < selectionKeys.size(); i++) {
                    SelectionKey key = selectionKeys.keys[i];
                    processKey(key);
                    selectionKeys.keys[i] = null;
                }
                selectionKeys.reset();

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
                clientChannel.socket().setTcpNoDelay(true);

                NioChannel nioChannel = new NioChannel(nioChannelGroup, clientChannel);

                nioChannel.register(selector, SelectionKey.OP_READ);

                nioChannelGroup.put(nioChannel);
            }
        } catch (IOException ex) {
            logger.error("accept client error {}", ex);
        }

    }

    /**
     * open selector.
     * 来自Netty源码,主要是为了改善Selector性能问题
     *
     * @return Selector.
     */
    private Selector openSelector() {
        try {
            selector = provider.openSelector();
        } catch (IOException e) {
            logger.error("fail to create a new selector {}", e);
        }

        final SelectedSelectionKeySet selectedKeySet = new SelectedSelectionKeySet();

        Object maybeSelectorImplClass = AccessController.doPrivileged(new PrivilegedAction<Object>() {
            @Override
            public Object run() {
                try {
                    return Class.forName(
                            "sun.nio.ch.SelectorImpl",
                            false,
                            PlatformDependent.getSystemClassLoader());
                } catch (Throwable cause) {
                    return cause;
                }
            }
        });


        final Class<?> selectorImplClass = (Class<?>) maybeSelectorImplClass;

        Object maybeException = AccessController.doPrivileged(new PrivilegedAction<Object>() {
            @Override
            public Object run() {
                try {
                    Field selectedKeysField = selectorImplClass.getDeclaredField("selectedKeys");
                    Field publicSelectedKeysField = selectorImplClass.getDeclaredField("publicSelectedKeys");

                    Throwable cause = ReflectionUtil.trySetAccessible(selectedKeysField);
                    if (cause != null) {
                        return cause;
                    }
                    cause = ReflectionUtil.trySetAccessible(publicSelectedKeysField);
                    if (cause != null) {
                        return cause;
                    }

                    selectedKeysField.set(selector, selectedKeySet);
                    publicSelectedKeysField.set(selector, selectedKeySet);
                    return null;
                } catch (NoSuchFieldException e) {
                    return e;
                } catch (IllegalAccessException e) {
                    return e;
                }
            }
        });

        if (maybeException instanceof Exception) {
            Exception e = (Exception) maybeException;
            logger.trace("failed to instrument a special java.util.Set into: {}", selector, e);
            return selector;
        }

        logger.debug("instrumented a special java.util.Set into: {}", selector);
        this.selectionKeys = selectedKeySet;
        return selector;
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
     *
     * @return bool
     */
    public boolean isShutdown() {
        return isShutdown;
    }
}
