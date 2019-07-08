package core.client;

import com.google.protobuf.MessageLite;

import java.io.IOException;
import java.net.Socket;

public class ClientSocket {
    private Socket socket;
    private SocketInHandler socketInHandler;
    private SocketOutHandler socketOutHandler;

    private ClientSocket(newBuilder builder) {
        this.socketInHandler = builder.socketInHandler;
        this.socketOutHandler = builder.socketOutHandler;
        this.socket = builder.socket;
    }

    public Object write(MessageLite msg) {
        socketOutHandler.write(socket, msg);
        return read();
    }

    public Object read() {
        return socketInHandler.read(socket);
    }

    /**
     * 连接是否关闭
     *
     * @return bool
     */
    public boolean isActive() {
        return socket.isConnected() && !socket.isClosed();
    }

    public void close() {
        try {
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        socket = null;
    }

    public static class newBuilder {
        private SocketInHandler socketInHandler;
        private SocketOutHandler socketOutHandler;
        private Socket socket;

        public newBuilder setSocketInHandler(SocketInHandler socketInHandler) {
            this.socketInHandler = socketInHandler;
            return this;
        }

        public newBuilder setSocketOutHandler(SocketOutHandler socketOutHandler) {
            this.socketOutHandler = socketOutHandler;
            return this;
        }

        public newBuilder setSocket(Socket socket) {
            this.socket = socket;
            return this;
        }

        public ClientSocket build() {
            if (socket == null || socketInHandler == null || socketOutHandler == null) {
                throw new IllegalArgumentException("arguments not set");
            }
            return new ClientSocket(this);
        }
    }
}
