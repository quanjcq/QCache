package store.message;

import common.UtilAll;

import java.nio.ByteBuffer;

/**
 * put key val timeOut storeTime
 * del key
 * <p>
 * size:2B
 * status:1B0[未提交] 1[已经提交]
 * type:1B [0-put,1-del]
 * <p>
 * put{
 * timeOut: 4B
 * storeTime: 8B
 * keyLen:1B
 * key: kenLen B
 * val: valLen B
 * <p>
 * }
 * <p>
 * del{
 * key: keyLen B
 * }
 * <p>
 * crcCode32: 4B
 */
public class AofLogMessage implements Message {
    private static volatile AofLogMessage aofLogMessage;
    private byte status;
    private byte type;
    //put
    private String key = null;
    private String val = null;
    private int timeOut;
    private long storeTime;



    private AofLogMessage() {

    }

    public static AofLogMessage getPutAofInstance(String key, String val, int timeOut) {
        if (aofLogMessage == null) {
            synchronized (AofLogMessage.class) {
                if (aofLogMessage == null) {
                    aofLogMessage = new AofLogMessage();
                }

            }
        }
        aofLogMessage.key = key;
        aofLogMessage.val = val;
        aofLogMessage.timeOut = timeOut;
        aofLogMessage.storeTime = System.currentTimeMillis();
        aofLogMessage.status = 0;
        aofLogMessage.type = 0;
        return aofLogMessage;
    }

    public static AofLogMessage getDelAofInstance(String key) {
        if (aofLogMessage == null) {
            synchronized (AofLogMessage.class) {
                if (aofLogMessage == null) {
                    aofLogMessage = new AofLogMessage();
                }

            }
        }
        aofLogMessage.key = key;

        aofLogMessage.status = 0;
        aofLogMessage.type = 1;
        return aofLogMessage;
    }
    public static AofLogMessage getInstance() {
        if (aofLogMessage == null) {
            synchronized (AofLogMessage.class) {
                if (aofLogMessage == null) {
                    aofLogMessage = new AofLogMessage();
                }

            }
        }
        return aofLogMessage;
    }
    @Override
    public byte[] encode() {
        short size = getSerializedSize();
        byte[] data = new byte[size];
        ByteBuffer buffer = ByteBuffer.wrap(data);

        buffer.clear();
        //messageSize; 2B
        buffer.putShort(size);
        //status 1B
        buffer.put(status);
        //type 1B
        buffer.put(type);
        //put
        if (type == (byte) 0) {
            //timeOut 4B
            buffer.putInt(timeOut);
            //storeTime 8B
            buffer.putLong(storeTime);
            //keyLen 1B
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
        } else {
            //del
            //key
            byte[] keyByte = UtilAll.string2Byte(key,"UTF-8");
            if (keyByte != null) {
                buffer.put(keyByte);
            }
        }
        int crc32COde = UtilAll.crc32(buffer.array(), 0, size - 4);
        buffer.putInt(crc32COde);
        return buffer.array();
    }

    @Override
    public AofLogMessage decode(ByteBuffer buffer) {

        if (buffer == null || buffer.remaining() <= 7) {
            return null;
        }
        short size = buffer.getShort();
        if (size <= 7 || size > buffer.remaining()) {
            return null;
        }
        ByteBuffer byteBuffer = ByteBuffer.allocate(size);
        byteBuffer.putShort(size);
        buffer.get(byteBuffer.array(),2,size -6);
        byteBuffer.position(size - 4);
        int crc32Code = buffer.getInt();

        int crc32CodeCaculate = UtilAll.crc32(byteBuffer.array(),0,size - 4);
        if (crc32Code != crc32CodeCaculate) {
            return null;
        }
        byteBuffer.putInt(crc32Code);

        byteBuffer.flip();
        //size
        byteBuffer.getShort();
        //status
        byte status = byteBuffer.get();
        //type
        byte type = byteBuffer.get();
        if (type == (byte) 0) {
            int timeOut = byteBuffer.getInt();
            long storeTime = byteBuffer.getLong();
            int keyLen = UtilAll.byteToInt(byteBuffer.get());
            byte[] keyByte = new byte[keyLen];
            byteBuffer.get(keyByte);
            String keyStr = UtilAll.byte2String(keyByte,"UTF-8");
            byte[] valByte = new byte[size - 21 - keyLen];
            byteBuffer.get(valByte);
            String valStr = UtilAll.byte2String(valByte,"UTF-8");
            AofLogMessage result = AofLogMessage.getPutAofInstance(keyStr,valStr,timeOut);
            result.setStoreTime(storeTime);
            result.setStatus(status);
            return result;
        } else {
            byte[] keyByte = new byte[size - 8];
            byteBuffer.get(keyByte);
            String keyStr = UtilAll.byte2String(keyByte,"UTF-8");
            AofLogMessage result =  AofLogMessage.getDelAofInstance(keyStr);
            result.setStatus(status);
            return result;
        }

    }

    @Override
    public short getSerializedSize() {
        int result = 0;

        if (type == (byte) 0) {
            //put
            result += 2    //messageSize
                    + 1    //status
                    + 1    //type
                    + 1    //keyLen
                    + 4    //timeOut
                    + 8    //storeTime
                    + UtilAll.getStrLen(key,"UTF-8") //key
                    + UtilAll.getStrLen(val,"UTF-8") //val
                    + 4;   //crc32Code

        } else {
            //del
            result += 2    //messageSize
                    + 1    //status
                    + 1    //type
                    + UtilAll.getStrLen(key,"UTF-8") //key
                    + 4;   //crc32Code
        }
        if (result > Short.MAX_VALUE) {
            return -1;
        }
        return (short) result;
    }

    public byte getStatus() {
        return status;
    }

    public void setStatus(byte status) {
        this.status = status;
    }

    public byte getType() {
        return type;
    }

    public void setType(byte type) {
        this.type = type;
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

    public int getTimeOut() {
        return timeOut;
    }

    public void setTimeOut(int timeOut) {
        this.timeOut = timeOut;
    }

    public long getStoreTime() {
        return storeTime;
    }

    public void setStoreTime(long storeTime) {
        this.storeTime = storeTime;
    }

    @Override
    public String toString() {
        return "AofLogMessage{" +
                "status=" + status +
                ", type=" + type +
                ", key='" + key + '\'' +
                ", val='" + val + '\'' +
                ", timeOut=" + timeOut +
                ", storeTime=" + storeTime +
                '}';
    }
}
