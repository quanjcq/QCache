import com.google.protobuf.MessageLite;
import core.message.UserMessageProto;
import core.nio.NioInProtoBufHandler;
import core.nio.NioOutProtoBufHandler;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

class NioServer {
    public static void main(String[] args) {
        new NioServer().init();
    }
    private ServerSocketChannel serverChannel;
    private ServerSocket serverSocket;
    private Selector selector;

    public void init() {
        try {
            serverChannel = ServerSocketChannel.open();
            serverSocket = serverChannel.socket();
            serverSocket.bind(new InetSocketAddress(9097));
            serverChannel.configureBlocking(false);
            selector = Selector.open();
            serverChannel.register(selector, SelectionKey.OP_ACCEPT);

        }catch (IOException ex) {
            ex.printStackTrace();
        }
        core.nio.Channel channel = new core.nio.Channel.newBuilder()
                .setNioInHandler(new NioInMessageHandler(UserMessageProto.UserMessage.getDefaultInstance()))
                .setNioOutHandler(new NioOutMessageHandler())
                .build();
        while (true) {
            try {
                selector.select();
                Iterator<SelectionKey> keyIter = selector.selectedKeys().iterator();
                while (keyIter.hasNext()) {
                    final SelectionKey key = keyIter.next();
                    if (key.isAcceptable()) {
                        SocketChannel clientChannel = serverChannel.accept();
                        if (clientChannel != null) {
                            clientChannel.configureBlocking(false);
                            clientChannel.register(selector, SelectionKey.OP_READ);
                        }
                    } else if (key.isReadable()) {
                        //System.out.println("get message form " + key.channel());
                        channel.setSelectionKey(key);
                        Object obj = channel.read();
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
                                channel.write(message);
                            } else if (msg.getMessageType() == UserMessageProto.MessageType.SET) {
                                System.out.println("set");
                            } else if (msg.getMessageType() == UserMessageProto.MessageType.DEL) {
                                System.out.println("del");
                            } else if (msg.getMessageType() == UserMessageProto.MessageType.STATUS) {
                                System.out.println("status");
                            }

                        }
                        keyIter.remove();
                    }

                }
            }catch (IOException ex) {
                channel.close();
                ex.printStackTrace();
            }
        }

    }

    private class NioInMessageHandler extends NioInProtoBufHandler<UserMessageProto.UserMessage> {
        public NioInMessageHandler(MessageLite messageLite) {
            super(messageLite);
        }

        @Override
        public Object read(java.nio.channels.SocketChannel socketChannel) {
            return super.read(socketChannel);
        }
    }

    private class NioOutMessageHandler extends NioOutProtoBufHandler<UserMessageProto.UserMessage> {
        @Override
        public void write(java.nio.channels.SocketChannel socketChannel, UserMessageProto.UserMessage msg) {
            super.write(socketChannel, msg);
        }
    }
}