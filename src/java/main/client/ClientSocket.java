package client;

import common.UtilAll;
import remote.message.Message;
import remote.message.RemoteMessage;

import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.ThreadPoolExecutor;

public class ClientSocket {
    private Socket socket;
    private SocketInHandler socketInHandler;
    private SocketOutHandler socketOutHandler;
    private ThreadPoolExecutor threadPool;

    private ClientSocket(NewBuilder builder) {
        this.socketInHandler = builder.socketInHandler;
        this.socketOutHandler = builder.socketOutHandler;
        this.socket = builder.socket;
        this.threadPool = UtilAll.getThreadPool();
    }

    public Message write(final RemoteMessage msg) {
        /*final CountDownLatch countDownLatch = new CountDownLatch(1);
        final SyncReadWrite syncReadWrite = new SyncReadWrite();
        syncReadWrite.result = null;
        threadPool.execute(new Runnable() {
            @Override
            public void run() {

                syncReadWrite.result = read();
                countDownLatch.countDown();
            }
        });
        if (syncReadWrite.result == null) {
            try {
                countDownLatch.await(200, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }*/
        socketOutHandler.write(socket, msg);
        Message res = read();
        return res;
        //return syncReadWrite.result;

    }

    public Message read() {
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

    public static class NewBuilder {
        private SocketInHandler socketInHandler;
        private SocketOutHandler socketOutHandler;
        private Socket socket;

        public NewBuilder setSocketInHandler(SocketInHandler socketInHandler) {
            this.socketInHandler = socketInHandler;
            return this;
        }

        public NewBuilder setSocketOutHandler(SocketOutHandler socketOutHandler) {
            this.socketOutHandler = socketOutHandler;
            return this;
        }

        public NewBuilder setSocket(Socket socket) {
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
