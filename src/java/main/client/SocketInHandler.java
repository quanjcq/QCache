package client;

import remote.message.Message;

import java.net.Socket;

public interface SocketInHandler<T extends Message> {
    /**
     * 从SocketChannel 里面读取消息
     * @return T 读取到的消息
     */
    T read(Socket socket);
}
