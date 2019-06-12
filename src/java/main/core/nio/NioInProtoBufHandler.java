package core.nio;

import com.google.protobuf.MessageLite;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public abstract class NioInProtoBufHandler<T extends MessageLite> implements NioInHandler<T> {
    private ByteBuffer headBuf = ByteBuffer.allocateDirect(2);

    private MessageLite messageLite;

    public NioInProtoBufHandler(MessageLite messageLite) {
        this.messageLite = messageLite;
    }

    /**
     * 从channel 里面读取消息.
     * 前两个字节表示数据流的长度,后面才是真是数据
     *
     * @param socketChannel channel
     * @return T
     */
    @Override
    public Object read(SocketChannel socketChannel) {
        short headSize = 0;
        Object result = null;
        try {
            headBuf.clear();
            int temp;
            if ((temp = socketChannel.read(headBuf)) > 0) {
                headBuf.flip();
                headSize = headBuf.getShort();
            }
            if (temp == -1) {
                socketChannel.close();
                return null;
            }
            if (headSize == 0) {
                return null;
            }
            //还需要去读的数据长度
            short needRead = headSize;
            ByteBuffer buffer = ByteBuffer.allocate(needRead);
            buffer.clear();
            temp = socketChannel.read(buffer);
            if (temp == -1) {
                socketChannel.close();
            }
            if (temp < needRead) {
                return null;
            }
            buffer.flip();
            result = messageLite.getDefaultInstanceForType()
                    .newBuilderForType()
                    .mergeFrom(buffer.array(), 0, headSize)
                    .build();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return result;
    }

}
