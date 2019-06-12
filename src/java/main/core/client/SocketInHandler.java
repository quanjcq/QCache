package core.client;

import com.google.protobuf.MessageLite;

import java.net.Socket;
import java.nio.channels.SocketChannel;

public interface SocketInHandler<T extends MessageLite> {
    /**
     * 从SocketChannel 里面读取消息
     * @return T 读取到的消息
     */
    Object read(Socket socket);
}
