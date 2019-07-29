package remote.message;

import java.nio.ByteBuffer;

public interface Message {
    /**
     * 对象序列化成 字节数组
     * @return bool
     */
    byte[] encode();

    /**
     * 将对象序列化,放入buffer[堆外内存直接写回到客户端]
     * @param buffer DirectBuffer
     */
    void encode(ByteBuffer buffer);

    /**
     * 获取对象序列化长度
     *
     * @return int 长度,出错返回-1
     */
    int getSerializedSize();

}
