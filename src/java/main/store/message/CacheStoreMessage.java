package store.message;

import common.UtilAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.ByteBuffer;
import java.util.TreeMap;

/**
 * MessageSize: 2B
 * status: 1B(是否被删除)
 * visit: 4B(访问时间,LRU算法回收,visit = currentTime - minTime)
 * StoreTime:4B(StoreTime = currentTime - minTime)
 * timeOut:4B(过时时间,-1不过时)
 * keyLen:1B(key长度[0~255])
 * key:keyLen B
 * ext: 1B 扩展字段长度(0~255)
 * val: 根据MessageSize 计算
 *
 * crc32.检验码[只校验key,val,为了效率程序运行时候会动态变化的字段不校验]
 */
//该对象只是当做容器使用,所以是单例,注意多线程的情况下可能会出现的问题.
public class CacheStoreMessage implements Message {
    private static Logger logger = LoggerFactory.getLogger(CacheStoreMessage.class);
    private static volatile CacheStoreMessage cacheStoreMessage = null;
    private String key;
    private String val;
    private byte status;
    private int visit;
    private int storeTime;
    private int timeOut;

    //存放扩展字段
    private TreeMap<String,String> ext = new TreeMap<String, String>();
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
        cacheStoreMessage.visit = storeTime;
        cacheStoreMessage.createNew = false;
        return cacheStoreMessage;
    }

    @Override
    public byte[] encode() {
        /**
         * MessageSize: 2B
         * status: 1B(是否被删除)
         * visit: 4B(访问时间,LRU算法回收,visit = currentTime - minTime)
         * StoreTime:4B(StoreTime = currentTime - minTime)
         * timeOut:4B(过时时间,-1不过时)
         * keyLen:1B(key长度[0~255])
         * key:keyLen B
         * ext: 1B 扩展字段长度(0~255)
         * val: 根据MessageSize 计算
         *
         * crc32.检验码[为了效率程序运行时候会动态变化的字段不校验,比如visitTime]
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
        //visit 4B  3
        byteBuffer.putInt(storeTime);
        //storeTime 4B 7
        byteBuffer.putInt(storeTime);
        //timeOut 4B   11
        byteBuffer.putInt(timeOut);
        //keyLen  1B  15
        byteBuffer.put(UtilAll.intToByte(UtilAll.getStrLen(key,"UTF-8")));

        byte[] keyByte = UtilAll.string2Byte(key,"UTF-8");
        if (keyByte != null){
            byteBuffer.put(keyByte);
        } else {
            logger.error("key error");
            return null;
        }

        //extSize
        byteBuffer.put(UtilAll.intToByte(getExtSize()));
        for (String key:ext.keySet()) {
            int extkeyLen = UtilAll.getStrLen(key,"UTF-8");
            String extVal = ext.get(key);
            int extValLen = UtilAll.getStrLen(extVal,"UTF-8");
            //put key
            byteBuffer.put((byte)extkeyLen);
            byte[] extKeyByte = UtilAll.string2Byte(key,"UTF-8");
            if (extKeyByte != null) {
                byteBuffer.put(extKeyByte);
            } else {
                logger.error("ext fields error");
                return null;
            }
            //put val
            byteBuffer.put((byte)extValLen);
            byte[] extValByte = UtilAll.string2Byte(extVal,"UTF-8");
            if (extValByte != null) {
                byteBuffer.put(extValByte);
            } else {
                logger.error("ext fields error");
                return null;
            }


        }
        //val
        byte[] valByte =  UtilAll.string2Byte(val,"UTF-8");
        if (valByte != null) {
            byteBuffer.put(valByte);
        } else {
            logger.error("val error");
            return null;
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
         * status: 1B(是否被删除)
         * visit: 4B(访问时间,LRU算法回收,visit = currentTime - minTime)
         * StoreTime:4B(StoreTime = currentTime - minTime)
         * timeOut:4B(过时时间,-1不过时)
         * keyLen:1B(key长度[0~255])
         * key:keyLen B
         * ext: 1B 扩展字段长度(0~255)
         * val: 根据MessageSize 计算
         *
         * crc32.检验码[只校验key,val,为了效率程序运行时候会动态变化的字段不校验]
         */
        short size = buffer.getShort();
        if (buffer.limit() -buffer.position() < size - 2) {
            return null;
        }
        byte status = buffer.get();

        int visit = buffer.getInt();
        int storeTime = buffer.getInt();
        int timeOut = buffer.getInt();

        int keyLen = UtilAll.byteToInt(buffer.get());
        byte[] key = new byte[keyLen];
        buffer.get(key);
        String keyStr = UtilAll.byte2String(key,"UTF-8");
        //ext
        int extSize = UtilAll.byteToInt(buffer.get());
        int tempExtSize = extSize;
        ext.clear();
        while (tempExtSize > 0 && buffer.remaining() > 0) {
            //key
            int extKeyLen = UtilAll.byteToInt(buffer.get());
            byte[] extKeyByte = new byte[extKeyLen];
            buffer.get(extKeyByte);
            String extKeyStr = UtilAll.byte2String(extKeyByte,"UTF-8");

            //val
            int extValLen = UtilAll.byteToInt(buffer.get());
            byte[] extValByte = new byte[extValLen];
            buffer.get(extValByte);
            String extValStr = UtilAll.byte2String(extValByte,"UTF-8");


            ext.put(extKeyStr,extValStr);
            tempExtSize -= 2 + extKeyLen + extValLen;

        }




        byte[] val = new byte[size - 21 - keyLen - extSize];
        buffer.get(val);
        String valStr = UtilAll.byte2String(val,"UTF-8");
        buffer.getInt();
        CacheStoreMessage cacheStoreMessage = CacheStoreMessage.getInstance(keyStr, valStr, storeTime, timeOut, createNew);
        cacheStoreMessage.setStatus(status);
        cacheStoreMessage.setVisit(visit);
        cacheStoreMessage.addExtFields(ext);

        return cacheStoreMessage;
    }

    @Override
    public short getSerializedSize() {
        int extSize = getExtSize();
        if (extSize == -1) {
            return -1;
        }
        int keyLen = UtilAll.getStrLen(key,"UTF-8");
        if (keyLen > 255) {
            logger.error("key Len {} can not than 255",keyLen);
            return -1;
        }
        int result = 2   //size
                + 1      //status
                + 4      //visit
                + 4      //storeTime
                + 4      //timeOut
                + 1      //keyLen
                + keyLen //key 长度
                + 1      //extLen
                + extSize//extVal
                + UtilAll.getStrLen(val,"UTF-8")   //val val长度
                + 4;     //校验码
        if (result > Short.MAX_VALUE) {
            return -1;
        }
        return (short) result;
    }

    /**
     * 获取扩展列的长度
     * extSize: 1B [0~255]
     * 对于每个key val 都是keyLen(1B): key,valLen(1B): val
     * @return int
     */
    private int getExtSize(){
        int result = 0;
        for (String key:ext.keySet()) {
            result += UtilAll.getStrLen(key,"UTF-8") + UtilAll.getStrLen(ext.get(key),"UTF-8") + 2;
        }
        if (result > 255) {
            logger.error("ext fields total size: {} can not bigger than 255",result);
            return -1;
        }
        return result;
    }

    /**
     * 添加ext 属性
     * @param key key
     * @param val val
     */
    public void addExtField(String key,String val) {
        ext.put(key,val);
    }

    /**
     * 获取一个ext 属性值
     * @param key key
     * @return String val
     */
    public String getExtField(String key) {
        return ext.get(key);
    }

    public TreeMap<String,String> getExt(){
        return ext;
    }

    public void addExtFields(TreeMap<String,String> data) {
        ext.clear();
        ext.putAll(data);
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

    public int getVisit() {
        return visit;
    }

    public void setVisit(int visit) {
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
                ", ext=" + ext +
                ", createNew=" + createNew +
                ", bufferSize=" + bufferSize +
                '}';
    }
}
