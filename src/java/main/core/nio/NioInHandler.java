package core.nio;

import com.google.protobuf.MessageLite;

import java.nio.channels.SocketChannel;

public interface NioInHandler<T extends MessageLite> {
    /**
     * 从SocketChannel 里面读取消息
     * @return T 读取到的消息
     */
    Object read(SocketChannel socketChannel);
}
