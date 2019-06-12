package core.nio;

import com.google.protobuf.MessageLite;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

public class Channel{
    private SelectionKey selectionKey;
    private SocketChannel socketChannel;
    private NioInHandler nioInHandler;
    private NioOutHandler nioOutHandler;
    private Channel(NioInHandler inHandler,NioOutHandler outHandler){
        this.nioInHandler = inHandler;
        this.nioOutHandler = outHandler;
    }


    public void write(MessageLite msg){
        nioOutHandler.write(socketChannel,msg);
    }

    public Object read() {
        return nioInHandler.read(socketChannel);
    }

    public void close(){
        try {
            socketChannel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        selectionKey.cancel();
    }
    public void setSelectionKey(SelectionKey selectionKey) {
        this.selectionKey = selectionKey;
        this.socketChannel = (SocketChannel) selectionKey.channel();
    }

    public static class newBuilder{
        private NioInHandler nioInHandler;
        private NioOutHandler nioOutHandler;
        public newBuilder setNioInHandler(NioInHandler inHandler) {
            this.nioInHandler = inHandler;
            return this;
        }

        public newBuilder setNioOutHandler(NioOutHandler outHandler) {
            this.nioOutHandler = outHandler;
            return this;
        }

        public Channel build() {
            if (this.nioOutHandler == null) {
                throw new IllegalArgumentException("NioOutHandler cant not empty");
            }

            if (this.nioInHandler == null) {
                throw new IllegalArgumentException("NioInHandler cant not empty");
            }
            return new Channel(this.nioInHandler,this.nioOutHandler);

        }
    }
}
