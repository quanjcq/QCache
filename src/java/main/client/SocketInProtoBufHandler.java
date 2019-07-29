package client;
import remote.message.Message;
import remote.message.RemoteMessage;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.ByteBuffer;

public abstract class SocketInProtoBufHandler<T extends Message> implements SocketInHandler<T> {
    private int cacheSize = 1024;
    private byte[] buffer = new byte[cacheSize];
    protected SocketInProtoBufHandler() {

    }
    /**
     * 从socket 里面读取消息.
     * 前两个字节表示数据流的长度,后面才是真是数据
     * @param socket socket
     * @return T
     */
    @Override
    public T read(Socket socket){
        T result = null;
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
            if (len > cacheSize) {
                cacheSize = len;
                buffer = new byte[cacheSize];
            }
            int needRead = len - 2;
            int temp  = 0;
            int start = 2;
            while (needRead > 0 && (temp = inputStream.read(buffer,start,needRead)) > 0) {
                //数据读完
                if (temp == needRead ) {
                    break;
                } else if (temp < needRead) {
                    needRead -= temp;
                    start += temp;
                }
            }
            if (temp == -1) {
                socket.close();
            }
            System.arraycopy(head,0,buffer,0,2);
            ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);
            byteBuffer.limit(len);
            result = (T)RemoteMessage.decode(byteBuffer,true);

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
