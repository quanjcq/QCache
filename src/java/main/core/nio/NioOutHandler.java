package core.nio;

import com.google.protobuf.MessageLite;

import java.nio.channels.SocketChannel;

public interface NioOutHandler<T extends MessageLite> {
    /**
     * 将msg 对象写入 SocketChannel,前两个字节放置消息体的长度
     * @param socketChannel channel.
     * @param msg 需要写入的channel.
     */
    void write(SocketChannel socketChannel, T msg);
}
