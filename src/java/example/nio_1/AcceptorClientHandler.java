package nio_1;

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
import java.util.concurrent.ThreadPoolExecutor;

/**
 * 处理客户端连接问题,并且把连接注册到NioEventLoopGroup
 * NioEventLoopGroup 分发给具体NioEventLoop
 * 多个NioEventLoop 通过NioEventLoopGroup sync 做同步
 */
public class AcceptorClientHandler implements Runnable{
    private static Logger logger = LoggerFactory.getLogger(AcceptorClientHandler.class);


    private NioEventLoopGroup nioEventLoopGroup;

    private volatile boolean isShutdown = false;
    private Selector selector;
    private int port;
    private final SelectorProvider provider = SelectorProvider.provider();
    public AcceptorClientHandler(NioEventLoopGroup nioEventLoopGroup,
                                 int port) {

        this.nioEventLoopGroup = nioEventLoopGroup;
        selector = openSelector();
        this.port = port;

    }

    /**
     * open Selector.
     * @return
     */
    private Selector openSelector(){
        try {
            return provider.openSelector();
        } catch (IOException e) {
            logger.error("fail to create a new selector {}",e,toString());
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

                    Iterator<SelectionKey> iterator = keySet.iterator();
                    while (iterator.hasNext()) {
                        SelectionKey key = iterator.next();
                        if(key.isAcceptable()) {
                            SocketChannel socketChannel = serverSocketChannel.accept();
                            if (socketChannel != null) {
                                socketChannel.configureBlocking(false);
                                nioEventLoopGroup.register(socketChannel,SelectionKey.OP_READ);
                            }
                        }else{
                           logger.info("not register event");
                        }
                        iterator.remove();
                    }

                } catch (IOException e) {
                    logger.error("select error {}",e);
                }
            }
        } catch (IOException e) {
            logger.error(e.toString());
        }
    }

    /**
     * close.
     */
    public void shutdown(){
        isShutdown = true;
        try {
            selector.close();
        } catch (IOException e) {
            logger.error("can not cancel selector {}",e);
        }
    }

    /**
     * run.
     * @param poolExecutor pool
     */
    public void run(ThreadPoolExecutor poolExecutor){
        poolExecutor.execute(this);
    }
}
