package core.client;
import com.google.protobuf.MessageLite;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
public abstract class SocketInProtoBufHandler<T extends MessageLite> implements SocketInHandler<T> {
    private MessageLite messageLite;

    protected SocketInProtoBufHandler(MessageLite messageLite) {
        this.messageLite = messageLite;
    }
    /**
     * 从socket 里面读取消息.
     * 前两个字节表示数据流的长度,后面才是真是数据
     * @param socket socket
     * @return Obj
     */
    @Override
    public Object read(Socket socket){
        Object result = null;
        byte[] head = new byte[2];
        try {
            InputStream inputStream = socket.getInputStream();
            int readSize = inputStream.read(head,0,2);
            if (readSize != 2) {
                return null;
            }
            short len = byteToShort(head);
            if (len == 0) {
                return null;
            }

            byte[] body = new byte[len];
            int needRead = len;
            int temp  = 0;
            int start = 0;
            while (needRead > 0 && (temp = inputStream.read(body,start,needRead)) > 0) {
                //数据读完
                if (temp == needRead ) {
                    result = messageLite.getDefaultInstanceForType()
                            .newBuilderForType()
                            .mergeFrom(body,0,len)
                            .build();
                    break;
                } else if (temp < needRead) {
                    needRead -= temp;
                    start += temp;
                }
            }
            if (temp == -1) {
                socket.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }



    /**
     * 字节数组转 short (消息体的长度).
     * @param bytes 字节数组
     * @return short
     */
    private short byteToShort(byte[] bytes) {
        if (bytes.length != 2) {
            return 0;
        }
        return (short)( (((short)bytes[0] & 0xFF) << 8) | (short)bytes[1] & 0xFF);
    }
}
