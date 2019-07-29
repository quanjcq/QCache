package remote.message;

/**
 * 接收客户端的消息的
 */

import common.UtilAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * 服务端和客户端交互的消息
 * MessageSize: 2B
 * MessageType: 1B
 * put{
 * timeOut: 4B
 * keyLen:  1B
 * key:     keyLen B
 * val:     valLenB [MessageSize - 2 - 1 - 4- 1 - keyLen = size -8 -keyLen]
 * }
 * get{
 * key:     keyLen
 * }
 * del {
 * key:     keyLen
 * }
 * status{
 * <p>
 * }
 * response {
 * [OK|KEY_NOT_EXIST|SWITCH_NODE|UN_KNOWN|STATUS_S]
 * }
 */
public class RemoteMessage implements Message{
    private static Logger logger = LoggerFactory.getLogger(RemoteMessage.class);
    private static RemoteMessage remoteMessage;
    private byte messageType;
    private int timeOut;
    private String key;
    private String val;
    private byte[] response;

    private RemoteMessage() {

    }

    /**
     * 返回该类的一个实例
     * @param createNew 是创建新对象还是返回单利.
     * @return RemoteMessage.
     */
    public static RemoteMessage getInstance(boolean createNew) {
        if (createNew) {
            return new RemoteMessage();
        }
        if (remoteMessage == null) {
            synchronized (RemoteMessage.class) {
                if (remoteMessage == null) {
                    remoteMessage = new RemoteMessage();
                }
            }
        }
        return remoteMessage;
    }

    /**
     * decode
     *
     * @param buffer    directBuffer[直接内存].
     * @param createNew createNew[是创建对象还是复用单例对象].
     * @return RemoteMessage, 错误返回null.
     */
    public static RemoteMessage decode(ByteBuffer buffer, boolean createNew) {
        RemoteMessage result = RemoteMessage.getInstance(createNew);
        short size = buffer.getShort();
        if (buffer.remaining() < size - 2) {
            return null;
        }
        //type 1B
        byte type = buffer.get();
        result.setMessageType(type);

        switch (getRemoteMessageType(buffer.get(2))) {
            case PUT:
                //timeOut 2B
                int timeOut = buffer.getInt();
                //keyLen  1B
                int keyLen = UtilAll.byteToInt(buffer.get());
                if (keyLen <= 0) {
                    return null;
                }
                //keyStr
                byte[] keyByte = new byte[keyLen];
                buffer.get(keyByte);
                String keyStr = UtilAll.byte2String(keyByte,"UTF-8");
                //val
                byte[] valByte = new byte[size - 8 - keyLen];
                buffer.get(valByte);
                String valStr = UtilAll.byte2String(valByte,"UTF-8");
                result.setKey(keyStr);
                result.setTimeOut(timeOut);
                result.setVal(valStr);
                break;
            case GET:
            case DEL:
                //key
                keyByte = new byte[size - 3];
                buffer.get(keyByte);
                keyStr = UtilAll.byte2String(keyByte,"UTF-8");
                result.setKey(keyStr);
                break;
            case STATUS:
            case OK:
            case UN_KNOWN:
            case KEY_NOT_EXIST:
            case NOT_READ:
            case NOT_WRITE:
            case ERROR:
                break;
            case STATUS_R:
            case SWITCH_NODE:
            case GET_R:
                byte[] responseByte = new byte[size - 3];
                buffer.get(responseByte);
                result.setResponse(responseByte);
                break;
        }
        return result;
    }

    public static byte getTypeByte(RemoteMessageType type) {
        return (byte) type.ordinal();
    }

    public static RemoteMessageType getRemoteMessageType(byte type) {
        if (type < 0 || type >= RemoteMessageType.values().length) {
            return RemoteMessageType.UN_KNOWN;
        }
        return RemoteMessageType.values()[type];
    }

    /**
     * 对象序列化成字节数组
     *
     * @return byte[], 出错将返回null
     */
    public byte[] encode() {
        int size = getSerializedSize();
        if (size < 3 || size > Short.MAX_VALUE) {
            return null;
        }
        ByteBuffer buffer = ByteBuffer.allocate(size);
        //长度
        buffer.putShort((short) size);
        //类型
        buffer.put(messageType);
        switch (getRemoteMessageType(messageType)) {
            case PUT:
                //timeOut
                buffer.putInt(timeOut);
                //keyLen
                buffer.put(UtilAll.intToByte(UtilAll.getStrLen(key,"UTF-8")));
                //key
                byte[] keyByte = UtilAll.string2Byte(key,"UTF-8");
                if (keyByte != null) {
                    buffer.put(keyByte);
                }
                //val
                byte[] valByte = UtilAll.string2Byte(val,"UTF-8");
                if (valByte != null) {
                    buffer.put(valByte);
                }
                break;
            case GET:
            case DEL:
                //key
                keyByte = UtilAll.string2Byte(key,"UTF-8");
                if (keyByte != null) {
                    buffer.put(keyByte);
                }
                break;
            case STATUS:
            case OK:
            case UN_KNOWN:
            case KEY_NOT_EXIST:
            case NOT_READ:
            case NOT_WRITE:
            case ERROR:
                break;
            case STATUS_R:
            case SWITCH_NODE:
            case GET_R:
                buffer.put(response);
                break;
        }
        return buffer.array();
    }

    /**
     * 将对象序列化,放入buffer[堆外内存直接写回到客户端]
     * @param buffer DirectBuffer
     */
    public void encode(ByteBuffer buffer) {

        int size = getSerializedSize();
        if (buffer.remaining() < size) {
            logger.info("Not have enough space,buffer remain {},space need {}",buffer.remaining(),size);
        }
        if (size < 3 || size > Short.MAX_VALUE) {
            return;
        }
        //长度
        buffer.putShort((short) size);
        //类型
        buffer.put(messageType);
        switch (getRemoteMessageType(messageType)) {
            case PUT:
                //timeOut
                buffer.putInt(timeOut);
                //keyLen
                buffer.put(UtilAll.intToByte(UtilAll.getStrLen(key,"UTF-8")));
                //key
                byte[] keyByte = UtilAll.string2Byte(key,"UTF-8");
                if (keyByte != null) {
                    buffer.put(keyByte);
                }
                //val
                byte[] valByte = UtilAll.string2Byte(val,"UTF-8");
                if (valByte != null) {
                    buffer.put(valByte);
                }
                break;
            case GET:
            case DEL:
                //key
                keyByte = UtilAll.string2Byte(key,"UTF-8");
                if (keyByte != null) {
                    buffer.put(keyByte);
                }
                break;
            case STATUS:
            case OK:
            case UN_KNOWN:
            case KEY_NOT_EXIST:
            case NOT_READ:
            case NOT_WRITE:
            case ERROR:
                break;
            case STATUS_R:
            case GET_R:
            case SWITCH_NODE:
                buffer.put(response);
                break;
        }
    }

    /**
     * 获取对象序列化长度
     *
     * @return int 长度,出错返回-1
     */
    public int getSerializedSize() {
        switch (getRemoteMessageType(messageType)) {
            case PUT:
                if (key == null || val == null) {
                    return -1;
                }
                return 8 + UtilAll.getStrLen(key,"UTF-8") + UtilAll.getStrLen(val,"UTF-8");
            case GET:
            case DEL:
                if (key == null) {
                    return -1;
                }
                return 3 + UtilAll.getStrLen(key,"UTF-8");
            case STATUS:
            case KEY_NOT_EXIST:
            case UN_KNOWN:
            case OK:
            case NOT_READ:
            case NOT_WRITE:
            case ERROR:
                return 3;
            case SWITCH_NODE:
            case GET_R:
            case STATUS_R:
                if (response == null) {
                    return -1;
                }
                return 3 + response.length;
        }
        return -1;
    }

    /**
     * 返回buffer中存放的消息类型
     * @param buffer DirectBuffer
     * @return RemoteMessageType
     */
    public static RemoteMessageType getRemoteMessageType(ByteBuffer buffer){
        byte type = buffer.get(2);
        return getRemoteMessageType(type);
    }
    public byte getMessageType() {
        return messageType;
    }

    public void setMessageType(byte messageType) {
        this.messageType = messageType;
    }

    public int getTimeOut() {
        return timeOut;
    }

    public void setTimeOut(int timeOut) {
        this.timeOut = timeOut;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getVal() {
        return val;
    }

    public void setVal(String val) {
        this.val = val;
    }

    public byte[] getResponse() {
        return response;
    }

    public void setResponse(byte[] response) {
        this.response = response;
    }

    @Override
    public String toString() {
        return "RemoteMessage{" +
                "messageType=" + RemoteMessage.getRemoteMessageType(messageType) +
                ", timeOut=" + timeOut +
                ", key='" + key + '\'' +
                ", val='" + val + '\'' +
                '}';
    }
}
