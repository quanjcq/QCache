package store;

import common.UtilAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import store.message.CacheStoreMessage;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 存放缓存数据是多个文件,该文件实现统一读写,文件名对应该文件第一个数据的偏移量
 * 文件header:在所有文件中第一个文件中预留一页作为header
 * 目前header:
 * timestamp = 8B
 * crc32 = 4B
 */

public class CacheFileGroup {
    private static Logger logger = LoggerFactory.getLogger(CacheFileGroup.class);
    //存放文件的目录
    private String storePath;

    //storePath 下面的一个子目录,具体文件是放在这个下面的
    private String storePathNow = null;
    //每个文件的大小
    private int fileSize;
    //目前文件写到的位置
    private AtomicLong writePosition = new AtomicLong(0);


    //具体文件列表
    private volatile List<MappedFile> mappedFileList = new ArrayList<MappedFile>();

    //第一条记录的时间
    private long firstTime = 0;

    //充当索引
    private volatile Map<String, Long> cacheIndex = new HashMap<String, Long>();

    //总记录数量
    private AtomicLong totalMessage = new AtomicLong(0);

    //被删除的总量
    private AtomicLong totalDelete = new AtomicLong(0);

    private String storePathNext;
    private AtomicLong writePositionNext = new AtomicLong(0);
    private List<MappedFile> mappedFileListNext;
    private Map<String, Long> cacheIndexNext;

    public CacheFileGroup(String storePath, int fileSize) {
        this.storePath = storePath;
        this.fileSize = fileSize;
        load();
    }

    private boolean load() {
        File dir = new File(this.storePath);
        //防止目录不存在
        MappedFile.ensureDirOK(this.storePath);

        //找storePath下面的第一个目录文件
        File[] rootFiles = dir.listFiles();
        if (rootFiles == null || rootFiles.length == 0) {
            storePathNow = getDirName(0) + "/";
            MappedFile.ensureDirOK(storePathNow);
        } else {
            for (int i = 0; i < rootFiles.length; i++) {
                if (rootFiles[i].isDirectory() && storePathNow == null) {
                    storePathNow = rootFiles[i].getAbsolutePath() + "/";
                } else {
                    boolean flag = rootFiles[i].delete();
                    logger.info(rootFiles[i].getAbsolutePath() + " delete " + (flag ? "OK" : "Failed"));
                }
            }
        }
        File dirNow = new File(storePathNow);
        File[] files = dirNow.listFiles();
        //没有文件
        if (files != null && files.length == 0) {
            try {
                MappedFile mappedFile = new MappedFile(getFileName(storePathNow, 0L), fileSize);
                mappedFileList.add(mappedFile);
                firstTime = System.currentTimeMillis();
                setFirstTime(mappedFileList, firstTime);
                writePosition.set(StoreOptions.OS_PAGE_SIZE);
            } catch (IOException e) {
                logger.error("load cache MappedFile file error: {}", e.toString());
            }
        } else if (files != null) {
            // ascending order
            Arrays.sort(files);
            for (File file : files) {
                try {
                    MappedFile mappedFile = new MappedFile(file.getPath(), fileSize);
                    mappedFileList.add(mappedFile);
                } catch (IOException e) {
                    logger.info(file.getPath() + " MappedFile destroy!");
                }
            }
            firstTime = getFirstTime();
            if (firstTime == -1) {
                delete();
                return load();
            }
            long maxPosition = (long) mappedFileList.size() * (long) fileSize;
            long read = StoreOptions.OS_PAGE_SIZE;
            writePosition.set(read);
            /**
             * 0  MessageSize: 2B
             * 2  status: 1B(0,正常: 1,非正常)
             * 3  visit: 2B(访问次数,可以根据访问次数,决定将数据放在那些文件,让访问频率高的数据待在一块)
             * 5  StoreTime:4B(StoreTime = currentTime - minTime)
             * 9  timeOut:4B(过时时间,-1不过时)
             * 13 keyLen:1B(key长度[0~255])
             * 14 key:keyLen B
             *    val 根据MessageSize 计算
             *    crc32: 4B检验码[只在启动的时候计算这个值]
             * 2 + 1 + 2 + 4 + 4 + 1 + 4 + key.len + val.len = 18 + key.len + val.len
             */
            while (read < maxPosition) {
                //计算那个文件
                int fileIndex = (int) (read / (long) fileSize);

                //获取文件的位置
                int fileOffset = (int) (read % fileSize);

                if (fileOffset + 18 <= fileSize) {
                    //该文件后面可能还有消息,尝试读
                    MappedFile mappedFile = mappedFileList.get(fileIndex);
                    //size 2B
                    short size = mappedFile.getShort(fileOffset);
                    if (size <= 18 || size + fileOffset > fileSize) {

                        //System.out.println("到达该文件末尾,进入下个文件");
                        read = (fileIndex + 1) * (long) fileSize;
                        continue;
                    }


                    //status 1B
                    byte status = mappedFile.getByte(fileOffset + 2);


                    //visit 2B
                    short visit = mappedFile.getShort(fileOffset + 3);

                    //storeTime 4B
                    int storeTime = mappedFile.getInt(fileOffset + 5);

                    //timeOut 4B
                    int timeOut = mappedFile.getInt(fileOffset + 9);

                    if (timeOut > 0 && System.currentTimeMillis() - storeTime - firstTime > timeOut) {
                        //System.out.println("time out!");
                        read += size;
                        continue;
                    }

                    //keyLen
                    int keyLen = UtilAll.byteToInt(mappedFile.getByte(fileOffset + 13));
                    if (keyLen <= 0 || 18 + keyLen >= size) {
                        logger.info("keyLen error!,this file destroy,go next file! size: " + keyLen);
                        read = (fileIndex + 1) * (long) fileSize;
                        continue;
                    }
                    //key kenLen
                    byte[] key = mappedFile.getBytes(fileOffset + 14, keyLen);
                    String keyStr = UtilAll.byte2String(key,"UTF-8");

                    //val size - 18 - keyLen
                    byte[] val = mappedFile.getBytes(fileOffset + 14 + keyLen, size - 18 - keyLen);
                    String valStr = UtilAll.byte2String(val,"UTF-8");

                    //crc32Code
                    int crc32Code = mappedFile.getInt(fileOffset + size - 4);

                    ByteBuffer byteBuffer = ByteBuffer.allocate(size - 4);
                    //size 2B   0
                    byteBuffer.putShort(size);
                    //status 1B 2
                    byteBuffer.put((byte) 0);
                    //visit 2B  3
                    byteBuffer.putShort(visit);
                    //storeTime 4B 5
                    byteBuffer.putInt(storeTime);
                    //timeOut 4B   9
                    byteBuffer.putInt(timeOut);
                    //keyLen  1B  13
                    byteBuffer.put(UtilAll.intToByte(keyLen));
                    //key
                    byteBuffer.put(key);
                    byteBuffer.put(val);

                    int crc32CodeCaculate = UtilAll.crc32(byteBuffer.array());

                    //检验文件是否正常
                    if (crc32Code != crc32CodeCaculate) {
                        logger.info("{} file destroy!", mappedFile.getFilePath());
                        read = (fileIndex + 1) * (long) fileSize;
                        continue;
                    }
                    //该记录被删除
                    if (status == (byte) 1) {
                        //System.out.println("该记录被删除,读下个记录");
                        read += size;
                        continue;
                    }
                    totalMessageAdd();
                    if (read == writePosition.get()) {
                        cacheIndex.put(keyStr, read);
                        read += size;
                        writePosition.addAndGet(size);
                        //不需要移动元素
                        //System.out.println("不需要移动");
                        continue;
                    }
                    //

                    CacheStoreMessage cacheStoreMessage = CacheStoreMessage.getInstance(keyStr, valStr, storeTime, timeOut, false);
                    cacheStoreMessage.setVisit(visit);

                    AppendMessageResult appendMessageResult = appendMessage(mappedFileList, writePosition, storePathNow, cacheStoreMessage);
                    if (appendMessageResult.getState() == AppendMessageResult.AppendMessageState.APPEND_MESSAGE_OK) {
                        cacheIndex.put(keyStr, appendMessageResult.getAppendOffset());
                        writePosition.set(appendMessageResult.getAppendOffset());
                        writePosition.addAndGet(size);
                        read += size;


                    } else {
                        logger.info("appendMessage error! {}", appendMessageResult);
                        //读下一个文件
                        read = (fileIndex + 1) * (long) fileSize;
                    }
                } else {
                    read = (fileIndex + 1) * (long) fileSize;
                }


            }
        }
        writeFileEOF();
        return true;
    }

    private void writeFileEOF() {
        //计算那个文件
        int fileIndex = (int) (writePosition.get() / fileSize);

        //获取文件的位置
        int fileOffset = (int) (writePosition.get() % fileSize);

        if (fileOffset + 18 <= fileSize) {
            MappedFile mappedFile = mappedFileList.get(fileIndex);
            mappedFile.putShort(fileOffset, StoreOptions.FILE_EOF);
        }

        //后面若还存在文件删除他
        for (int i = fileIndex + 1; i < mappedFileList.size(); i++) {
            mappedFileList.get(i).destroy();
        }
        int num = mappedFileList.size() - fileIndex - 1;
        while (num-- > 0) {
            mappedFileList.remove(mappedFileList.size() - 1);
        }

    }

    /**
     * 重新构建CacheFile.将被删除的,过期数据清除.rebulid过程需要关闭所有写操作.
     *
     * @return bool
     */
    public boolean rebulid() {
        totalMessage.set(0);
        totalDelete.set(0);
        //1.创建一个新的目录
        storePathNext = getDirName(Long.parseLong(new File(storePathNow).getName()) + 1) + "/";
        mappedFileListNext = new ArrayList<MappedFile>();
        cacheIndexNext = new HashMap<String, Long>();
        try {
            MappedFile mappedFile = new MappedFile(getFileName(storePathNext, 0L), fileSize);
            mappedFileListNext.add(mappedFile);
            //firstTime = System.currentTimeMillis();
            setFirstTime(mappedFileListNext, firstTime);
            writePositionNext.set(StoreOptions.OS_PAGE_SIZE);
        } catch (IOException e) {
            logger.error("load cache MappedFile file error: {}", e.toString());
        }

        //复制数据到新的文件中
        long read = StoreOptions.OS_PAGE_SIZE;

        writePositionNext.set(read);

        long maxPosition = writePosition.get();

        while (read < maxPosition) {
            //计算那个文件
            int fileIndex = (int) (read / (long) fileSize);

            //获取文件的位置
            int fileOffset = (int) (read % fileSize);

            if (fileOffset + 18 <= fileSize) {
                //该文件后面可能还有消息,尝试读
                MappedFile mappedFile = mappedFileList.get(fileIndex);
                //size 2B
                short size = mappedFile.getShort(fileOffset);
                if (size <= 18 || size + fileOffset > fileSize) {

                    //System.out.println("到达该文件末尾,进入下个文件");
                    read = (fileIndex + 1) * (long) fileSize;
                    continue;
                }


                //status 1B
                byte status = mappedFile.getByte(fileOffset + 2);

                //该记录被删除
                if (status == (byte) 1) {
                    //System.out.println("该记录被删除,读下个记录");
                    read += size;
                    continue;
                }

                //visit 2B
                short visit = mappedFile.getShort(fileOffset + 3);

                //storeTime 4B
                int storeTime = mappedFile.getInt(fileOffset + 5);

                //timeOut 4B
                int timeOut = mappedFile.getInt(fileOffset + 9);

                if (timeOut > 0 && System.currentTimeMillis() - storeTime - firstTime > timeOut) {
                    //logger.info("time out!");
                    read += size;
                    continue;
                }

                //keyLen
                int keyLen = UtilAll.byteToInt(mappedFile.getByte(fileOffset + 13));
                if (keyLen <= 0 || 18 + keyLen >= size) {
                    logger.info("keyLen error!,this file destroy,go next file! size: " + keyLen);
                    read = (fileIndex + 1) * (long) fileSize;
                    continue;
                }
                //key keyLen
                byte[] key = mappedFile.getBytes(fileOffset + 14, keyLen);
                String keyStr = UtilAll.byte2String(key,"UTF-8");

                //val size - 18 - keyLen
                byte[] val = mappedFile.getBytes(fileOffset + 14 + keyLen, size - 18 - keyLen);
                String valStr = UtilAll.byte2String(val,"UTF-8");

                //crc32Code
                int crc32Code = mappedFile.getInt(fileOffset + size - 4);

                ByteBuffer byteBuffer = ByteBuffer.allocate(size - 4);
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
                byteBuffer.put(UtilAll.intToByte(keyLen));
                //key
                byteBuffer.put(key);
                byteBuffer.put(val);

                int crc32CodeCaculate = UtilAll.crc32(byteBuffer.array());

                //检验文件是否正常
                if (crc32Code != crc32CodeCaculate) {
                    //进入下个文件
                    CacheStoreMessage cacheStoreMessage = CacheStoreMessage.getInstance(keyStr, valStr, storeTime, timeOut, false);
                    System.out.println(cacheStoreMessage);
                    read = (fileIndex + 1) * (long) fileSize;
                    continue;
                }
                //将数据插入到新文件当中
                CacheStoreMessage cacheStoreMessage = CacheStoreMessage.getInstance(keyStr, valStr, storeTime, timeOut, false);
                cacheStoreMessage.setVisit(visit);
                AppendMessageResult appendMessageResult = appendMessage(mappedFileListNext, writePositionNext, storePathNext, cacheStoreMessage);
                if (appendMessageResult.getState() == AppendMessageResult.AppendMessageState.APPEND_MESSAGE_OK) {
                    cacheIndexNext.put(keyStr, appendMessageResult.getAppendOffset());
                    writePositionNext.set(appendMessageResult.getAppendOffset());
                    writePositionNext.addAndGet(size);
                    read += size;
                } else {
                    logger.info("appendMessage error! {}", appendMessageResult);
                    //读下一个文件
                    read = (fileIndex + 1) * (long) fileSize;
                }
                totalMessageAdd();
            } else {
                read = (fileIndex + 1) * (long) fileSize;
            }
        }
        //被删除数至于0
        totalDelete.set(0);
        return true;
    }

    /**
     * 重建Cache file在这之是没有生效的[这个方法执行前是要关闭读写]
     */
    public void setRebulidEffective() {
        //1.删除之前mappedFileList 所有mappedFile
        delete();
        //2.把mappedFileList 插入到 mappedFileListNext
        mappedFileList.addAll(mappedFileListNext);
        mappedFileListNext.clear();
        //3.修改writePosition
        writePosition.set(writePositionNext.get());
        writePositionNext.set(0);
        //4.修改index
        cacheIndex.clear();
        cacheIndex = cacheIndexNext;

        //5.删除之前的目录文件文件
        File file = new File(storePathNow);
        boolean result = file.delete();
        logger.info(storePathNow + " delete " + (result ? "OK" : "Failed"));
        storePathNow = storePathNext;
    }

    public long getFirstTime() {
        if (firstTime != 0) {
            return firstTime;
        }
        ByteBuffer byteBuffer = ByteBuffer.allocate(8);
        ByteBuffer mappedFileBuffer = mappedFileList.get(0).getMappedByteBuffer();
        mappedFileBuffer.position(0);
        mappedFileBuffer.get(byteBuffer.array());
        int crc32Code = mappedFileBuffer.getInt();
        int crc32Caculate = UtilAll.crc32(byteBuffer.array());
        if (crc32Caculate != crc32Code) {
            logger.info(mappedFileList.get(0).getFilePath() + " MappedFile destroy!");
            return -1;
        } else {
            return byteBuffer.getLong();
        }
    }

    private void setFirstTime(List<MappedFile> mappedFileList, long firstTime) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(8);
        byteBuffer.putLong(firstTime);
        int crc32Code = UtilAll.crc32(byteBuffer.array());
        ByteBuffer mappedFileBuffer = mappedFileList.get(0).getMappedByteBuffer();
        mappedFileBuffer.putLong(0, firstTime);
        mappedFileBuffer.putInt(8, crc32Code);
    }

    /**
     * 删除所有文件
     */
    public void delete() {
        for (MappedFile mappedFile : mappedFileList) {
            mappedFile.destroy();
        }
        mappedFileList.clear();
        logger.info("delete all Cache MappedFile!");
    }

    private String getFileName(String basePath, long firstPosition) {
        StringBuilder stringBuilder = new StringBuilder();
        int len = String.valueOf(firstPosition).length();
        stringBuilder.append(basePath);
        for (int i = 0; i < 32 - len; i++) {
            stringBuilder.append('0');
        }
        stringBuilder.append(firstPosition);
        return stringBuilder.toString();
    }

    private String getDirName(long num) {
        StringBuilder stringBuilder = new StringBuilder();
        int len = String.valueOf(num).length();
        stringBuilder.append(storePath);
        for (int i = 0; i < 32 - len; i++) {
            stringBuilder.append('0');
        }
        stringBuilder.append(num);
        return stringBuilder.toString();
    }

    /**
     * 插入数据.
     *
     * @param message msg
     * @return result.
     */
    private AppendMessageResult appendMessage(List<MappedFile> mappedFileList, AtomicLong writePosition, String basePath, CacheStoreMessage message) {
        //计算那个文件
        int fileIndex = (int) (writePosition.get() / fileSize);

        //插入文件的位置
        int fileOffset = (int) (writePosition.get() % fileSize);

        int size = message.getSerializedSize();
        if (fileOffset + size >= fileSize) {
            if (fileOffset + 2 <= fileSize) {
                mappedFileList.get(fileIndex).putShort(fileOffset, StoreOptions.FILE_EOF);
            }
            fileIndex++;
            fileOffset = 0;
            //create new file
            if (fileIndex >= mappedFileList.size()) {
                //System.out.println("需要创建新文件");
                String fileName = getFileName(basePath, (long) fileIndex * (long) fileSize);
                create(mappedFileList, fileName);
            }
        }

        return mappedFileList.get(fileIndex).insertMessage(message, fileOffset, false);
    }

    /**
     * 创建新的MappedFile
     *
     * @param fileName filename
     */
    private void create(List<MappedFile> mappedFileList, String fileName) {
        try {
            MappedFile mappedFile = new MappedFile(fileName, fileSize);
            mappedFileList.add(mappedFile);
        } catch (IOException e) {
            logger.error("create new MappedFile error: {}", e);
        }
    }

    /**
     * 读取指定位置处的数据.
     *
     * @param offset offset
     * @return message.
     */
    public CacheStoreMessage getMessage(long offset) {
        if (offset >= writePosition.get()) {
            logger.warn("offset: {} out of range:  {}", offset, writePosition.get());
            return null;
        }
        //计算那个文件
        int fileIndex = (int) (offset / fileSize);

        //获取文件的位置
        int fileOffset = (int) (offset % fileSize);

        MappedFile mappedFile = mappedFileList.get(fileIndex);
        ByteBuffer buffer = mappedFile.getByteBuffer();
        buffer.position(fileOffset);

        return CacheStoreMessage.getInstance(false).decode(buffer);
    }

    /**
     * 删除指定位置的数据[只是标记删除]
     *
     * @param offset
     * @return
     */
    private boolean deleteMessage(long offset) {
        if (offset > writePosition.get()) {
            return false;
        }
        //计算那个文件
        int fileIndex = (int) (offset / fileSize);

        //获取文件的位置
        int fileOffset = (int) (offset % fileSize);
        if (fileIndex + 2 >= fileSize) {
            return false;
        }
        MappedFile mappedFile = mappedFileList.get(fileIndex);

        //标记删除
        mappedFile.put(fileOffset + 2, (byte) 1);
        totalDelete.getAndIncrement();
        return true;
    }

    /**
     * put k v
     *
     * @param key     key
     * @param val     val
     * @param timeOut 超时时间
     * @return bool
     */
    public boolean put(String key, String val, int timeOut) {
        if (UtilAll.getStrLen(key,"UTF-8") + UtilAll.getStrLen(val,"UTF-8") + 18 > Short.MAX_VALUE) {
            return false;
        }
        int storeTime = (int) (System.currentTimeMillis() - firstTime);
        CacheStoreMessage cacheStoreMessage = CacheStoreMessage.getInstance(key, val, storeTime, timeOut, false);
        AppendMessageResult appendMessageResult = appendMessage(mappedFileList, writePosition, storePathNow, cacheStoreMessage);
        if (appendMessageResult.getState() == AppendMessageResult.AppendMessageState.APPEND_MESSAGE_OK) {
            long res = appendMessageResult.getAppendOffset();
            Long preVal = cacheIndex.get(key);
            if (preVal != null) {
                //System.out.println("删除已经存在的");
                deleteMessage(preVal);
            }
            cacheIndex.put(key, res);
            writePosition.set(appendMessageResult.getAppendOffset());
            writePosition.addAndGet(cacheStoreMessage.getSerializedSize());
            totalMessageAdd();
            return true;
        } else {
            logger.info("AppendMessage error! {}", appendMessageResult);
        }
        return false;
    }

    /**
     * get k
     *
     * @param key key
     * @return String, 不存在将返回null
     */
    public String get(String key) {
        String result = null;
        Long valOffset = cacheIndex.get(key);
        if (valOffset == null) {
            //System.out.println("valOffset = null");
            return result;
        }
        if (valOffset >= writePosition.get()) {
            //System.out.println("valOffset >= max");
            return result;
        }
        //计算那个文件
        int fileIndex = (int) (valOffset / fileSize);

        //获取文件的位置
        int fileOffset = (int) (valOffset % fileSize);
        //System.out.println("fileOffset:" + fileOffset);
        MappedFile mappedFile = mappedFileList.get(fileIndex);
        byte status = mappedFile.getByte(fileOffset + 2);
        if (status == (byte) 1) {
            //该数据被删
            //System.out.println("数据已经被删除");
            return result;
        }
        //storeTime
        int storeTime = mappedFile.getInt(fileOffset + 5);
        //System.out.println("storeTime:" +storeTime);
        //timeOut
        int timeOut = mappedFile.getInt(fileOffset + 9);

        //System.out.println("timeOut: " + timeOut);
        if (timeOut >= 0 && System.currentTimeMillis() - storeTime - firstTime > timeOut) {
            System.out.println("time out!");
            return result;
        }
        short size = mappedFile.getShort(fileOffset);

        int keyLen = UtilAll.getStrLen(key,"UTF-8");

        byte[] valByte = mappedFile.getBytes(fileOffset + 14 + keyLen, size - 18 - keyLen);

        result = UtilAll.byte2String(valByte,"UTF-8");

        return result;
    }

    /**
     * del.
     *
     * @param key key.
     * @return bool, 不存在将返回false
     */
    public boolean del(String key) {
        Long valOffset = cacheIndex.get(key);
        if (valOffset == null) {
            return false;
        }
        //计算那个文件
        int fileIndex = (int) (valOffset / fileSize);

        //获取文件的位置
        int fileOffset = (int) (valOffset % fileSize);

        MappedFile mappedFile = mappedFileList.get(fileIndex);
        //标记删除
        mappedFile.put(fileOffset + 2, (byte) 1);
        deleteMessageAdd();
        return true;
    }


    /**
     * 获取总记录数.
     *
     * @return long
     */
    public long getTotalMessage() {
        return totalMessage.get();
    }

    /**
     * 获取被删除的总记录数量
     *
     * @return long
     */
    public long getTotalDelete() {
        return totalDelete.get();
    }

    public void totalMessageAdd() {
        totalMessage.getAndIncrement();
    }

    public void deleteMessageAdd() {
        totalDelete.getAndIncrement();
    }

    public void deleteMessageAddCount(int count) {
        totalDelete.addAndGet(count);
    }

    /**
     * 获取磁盘写入的大小
     *
     * @return long
     */
    public long getWriteSize() {
        return writePosition.get();
    }

    /**
     * 刷盘.
     */
    public void flush() {
        for (MappedFile mappedFile : mappedFileList) {
            mappedFile.flush();
        }
    }

    public List<MappedFile> getMappedFileList() {
        return mappedFileList;
    }

    public int getFileSize () {
        return fileSize;
    }
}
