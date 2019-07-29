package store.message;

import java.nio.ByteBuffer;

/**
 * 需要存储的消息.
 */
public interface Message {
    /**
     * 将message 序列化,用于存储
     * @return byte[].
     */
    byte[] encode();

    /**
     *
     * @param buffer buffer
     * @return Message 实例,如果有错误将返回null.
     */
    Message decode(ByteBuffer buffer);

    /**
     * 获取序列化长度
     * @return 长度,出错返回 -1
     */
    short getSerializedSize();
}
