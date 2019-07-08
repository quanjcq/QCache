package nio_2;

import com.google.protobuf.MessageLite;
import core.message.UserMessageProto;
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

public class NioServer {
    private static Logger logger = LoggerFactory.getLogger(NioServer.class);
    private final SelectorProvider provider = SelectorProvider.provider();
    private ServerSocketChannel serverChannel;
    private ServerSocket serverSocket;
    private Selector selector;
    private int port;
    private volatile boolean isShutdown = false;
    private NioChannelGroup nioChannelGroup = new NioChannelGroup();
    private MessageLite messageLite;
    public NioServer(MessageLite messageLite, int port) {
        this.port = port;
        this.messageLite = messageLite;
    }

    public static void main(String[] args) {
        NioServer nioServer = new NioServer(UserMessageProto.UserMessage.getDefaultInstance(), 9097);
        nioServer.init();
    }

    public void init() {
        try {
            serverChannel = ServerSocketChannel.open();
            serverSocket = serverChannel.socket();
            serverSocket.bind(new InetSocketAddress(port));
            serverChannel.configureBlocking(false);
            selector = Selector.open();
            serverChannel.register(selector, SelectionKey.OP_ACCEPT);

        } catch (IOException ex) {
            logger.error(ex.toString());
        }

        while (!isShutdown()) {
            try {
                selector.select();
                Iterator<SelectionKey> keyIter = selector.selectedKeys().iterator();
                while (keyIter.hasNext()) {
                    SelectionKey key = keyIter.next();
                    keyIter.remove();
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
    private void processReadKey(SelectionKey selectionKey) {
        NioChannel nioChannel = nioChannelGroup.findChannel(selectionKey);
        if (nioChannel == null) {
            logger.info("Channel already closed!");
        } else {
            Object obj = nioChannel.read();
            if (obj == null) {
                return;
            }
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

    private Selector openSelector() {
        try {
            Selector selector = provider.openSelector();
            return selector;
        } catch (IOException e) {
            logger.error("fail to create a new selector {}", e, toString());
        }
        return null;
    }

    public void shudown() {
        isShutdown = true;
        nioChannelGroup.closeAll();
    }

    public boolean isShutdown() {
        return isShutdown;
    }
}
