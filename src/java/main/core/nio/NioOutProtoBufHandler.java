package core.nio;

import com.google.protobuf.MessageLite;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public abstract class NioOutProtoBufHandler<T extends MessageLite> implements NioOutHandler<T> {
    @Override
    public void write(SocketChannel socketChannel, T msg) {


        int bodySize = msg.getSerializedSize();
        if (bodySize > Short.MAX_VALUE) {
            throw new IllegalArgumentException("msg length not bigger than  " + Short.MAX_VALUE);
        }

        ByteBuffer buffer = ByteBuffer.allocateDirect(bodySize + 2);
        buffer.putShort((short) bodySize);

        buffer.put(msg.toByteArray());
        buffer.flip();
        try {
            socketChannel.write(buffer);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
