package store.message;

import common.UtilAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.ByteBuffer;

/**
 * MessageSize: 2B
 * status: 1B(是否被删除)
 * visit: 2B(访问次数,可以根据访问次数,决定将数据放在那些文件,让访问频率高的数据待在一块)
 * StoreTime:4B(StoreTime = currentTime - minTime)
 * timeOut:4B(过时时间,-1不过时)
 * keyLen:1B(key长度[0~255])
 * key:keyLen B
 * val: 根据MessageSize 计算
 * crc32.检验码[只校验key,val]
 */
//该对象只是当做容器使用,所以是单例,注意多线程的情况下可能会出现的问题.
public class CacheStoreMessage implements Message {
    private static Logger logger = LoggerFactory.getLogger(CacheStoreMessage.class);
    private static CacheStoreMessage cacheStoreMessage = null;
    private String key;
    private String val;
    private byte status;
    private short visit;
    private int storeTime;
    private int timeOut;
    private boolean createNew;
    private short bufferSize = 0;
    private byte[] buffer;

    private CacheStoreMessage() {

    }

    private CacheStoreMessage(String key, String val, int storeTime, int timeOut, boolean createNew) {
        this.key = key;
        this.val = val;
        this.storeTime = storeTime;
        this.timeOut = timeOut;
        this.createNew = createNew;

    }
    public static CacheStoreMessage getInstance(boolean createNew) {
        return getInstance(null,null,-1,0,createNew);
    }
    /**
     * 获取实例
     * @param key       key
     * @param val       val
     * @param storeTime time
     * @param timeOut   timeOut
     * @param createNew 是否创建新对象还是复用单例对象
     * @return 对象实例;
     */
    public static CacheStoreMessage getInstance(String key, String val, int storeTime, int timeOut, boolean createNew) {
        if (createNew) {
            //创建新对象
            return new CacheStoreMessage(key, val, storeTime, timeOut, true);
        }
        //复用之前的单例对象当容器使用
        if (cacheStoreMessage == null) {
            synchronized (CacheStoreMessage.class) {
                if (cacheStoreMessage == null) {
                    cacheStoreMessage = new CacheStoreMessage();
                }
            }
        }
        cacheStoreMessage.key = key;
        cacheStoreMessage.val = val;
        cacheStoreMessage.storeTime = storeTime;
        cacheStoreMessage.timeOut = timeOut;
        cacheStoreMessage.status = 0;
        cacheStoreMessage.visit = 0;
        cacheStoreMessage.createNew = false;
        return cacheStoreMessage;
    }

    @Override
    public byte[] encode() {
        /**
         * MessageSize: 2B
         * status: 1B(是否被删除)
         * visit: 2B(访问次数,可以根据访问次数,决定将数据放在那些文件,让访问频率高的数据待在一块)
         * StoreTime:4B(StoreTime = currentTime - minTime)
         * timeOut:4B(过时时间,-1不过时)
         * keyLen:1B(key长度[0~255])
         * key:keyLen B
         * val 根据MessageSize 计算
         * crc32.检验码[只校验key,val]
         */
        short size = getSerializedSize();
        //创建新对象
        if (createNew) {
            bufferSize = size;
            buffer = new byte[bufferSize];
        } else {
            //不创建对象复用之前的对象
            if (size > bufferSize) {
                bufferSize = size;
                buffer = new byte[bufferSize];
            }
        }

        ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);
        byteBuffer.clear();

        //size 2B   0
        byteBuffer.putShort(size);
        //status 1B 2
        byteBuffer.put(status);
        //visit 2B  3
        byteBuffer.putShort(visit);
        //storeTime 4B 5
        byteBuffer.putInt(storeTime);
        //timeOut 4B   9
        byteBuffer.putInt(timeOut);
        //keyLen  1B  13
        byteBuffer.put(UtilAll.intToByte(UtilAll.getStrLen(key,"UTF-8")));

        byte[] keyByte = UtilAll.string2Byte(key,"UTF-8");
        if (keyByte != null){
            byteBuffer.put(keyByte);
        } else {
            System.out.println("key error");
        }
        byte[] valByte =  UtilAll.string2Byte(val,"UTF-8");
        if (valByte != null) {
            byteBuffer.put(valByte);
        } else {
            System.out.println("val error");
        }
        int crc32Code = UtilAll.crc32(buffer, 0, size - 4);
        //crc32Code 4B
        byteBuffer.putInt(crc32Code);
        return byteBuffer.array();
    }

    @Override
    public CacheStoreMessage decode(ByteBuffer buffer) {
        /**
         * MessageSize: 2B
         * status: 1B(0,正常: 1删除)
         * visit: 2B(访问次数,可以根据访问次数,决定将数据放在那些文件,让访问频率高的数据待在一块)
         * StoreTime:4B(StoreTime = currentTime - minTime)
         * timeOut:4B(过时时间,-1不过时)
         * keyLen:1B(key长度[0~255])
         * key:keyLen B
         * val 根据MessageSize 计算
         * crc32: 4B检验码[只在启动的时候计算这个值]
         */
        short size = buffer.getShort();
        if (buffer.limit() -buffer.position() < size - 2) {
            return null;
        }
        byte status = buffer.get();
        short visit = buffer.getShort();
        int storeTime = buffer.getInt();
        int timeOut = buffer.getInt();
        byte keyLen = buffer.get();
        byte[] key = new byte[keyLen];
        buffer.get(key);
        String keyStr = UtilAll.byte2String(key,"UTF-8");
        byte[] val = new byte[size - 18 - keyLen];
        buffer.get(val);
        String valStr = UtilAll.byte2String(val,"UTF-8");
        buffer.getInt();
        CacheStoreMessage cacheStoreMessage = CacheStoreMessage.getInstance(keyStr, valStr, storeTime, timeOut, createNew);

        cacheStoreMessage.setStatus(status);
        cacheStoreMessage.setVisit(visit);

        return cacheStoreMessage;
    }

    @Override
    public short getSerializedSize() {
        int result = 2   //size
                + 1      //status
                + 2      //visit
                + 4      //storeTime
                + 4      //timeOut
                + 1      //keyLen
                + UtilAll.getStrLen(key,"UTF-8")   //key 长度
                + UtilAll.getStrLen(val,"UTF-8")   //val val长度
                + 4;     //校验码
        if (result > Short.MAX_VALUE) {
            return -1;
        }
        return (short) result;
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

    public byte getStatus() {
        return status;
    }

    public void setStatus(byte status) {
        this.status = status;
    }

    public short getVisit() {
        return visit;
    }

    public void setVisit(short visit) {
        this.visit = visit;
    }

    public int getStoreTime() {
        return storeTime;
    }

    public void setStoreTime(int storeTime) {
        this.storeTime = storeTime;
    }

    public int getTimeOut() {
        return timeOut;
    }

    public void setTimeOut(int timeOut) {
        this.timeOut = timeOut;
    }

    @Override
    public String toString() {
        return "CacheStoreMessage{" +
                "key='" + key + '\'' +
                ", val='" + val + '\'' +
                ", status=" + status +
                ", visit=" + visit +
                ", storeTime=" + storeTime +
                ", timeOut=" + timeOut +
                ", createNew=" + createNew +
                ", bufferSize=" + bufferSize +
                '}';
    }
}
