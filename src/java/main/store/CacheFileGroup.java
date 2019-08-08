package store;

import common.UtilAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import store.message.CacheStoreMessage;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

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
    private volatile String storePathNow = null;
    //每个文件的大小
    private int fileSize;
    //目前文件写到的位置
    private AtomicLong writePosition = new AtomicLong(0);


    //具体文件列表
    private  List<MappedFile> mappedFileList = new ArrayList<MappedFile>();

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
    /**
     * 是否有线程在重建Cache文件.
     * 在重建索引的过程,所有写请求需要写两份,读请求读之前的
     */
    private volatile boolean isRebulid = false;

    /**
     * 磁盘是否可以执行刷盘.
     */
    private AtomicBoolean canFlush = new AtomicBoolean(false);
    //对重建的索引的写操作需要加锁
    private ReentrantLock lock = new ReentrantLock();

    private List<MappedFile> mappedFileListNext = new ArrayList<MappedFile>();
    private volatile Map<String, Long> cacheIndexNext = new HashMap<String, Long>();

    public CacheFileGroup(String storePath, int fileSize) {
        this.storePath = storePath;
        this.fileSize = fileSize;
        load();
        canFlush.set(true);
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

            while (read < maxPosition) {
                //计算那个文件
                int fileIndex = (int) (read / (long) fileSize);

                //获取文件的位置
                int fileOffset = (int) (read % fileSize);

                if (fileOffset + 21 <= fileSize) {
                    //该文件后面可能还有消息,尝试读
                    MappedFile mappedFile = mappedFileList.get(fileIndex);
                    //size 2B
                    short size = mappedFile.getShort(fileOffset);
                    if (size <= 21 || size + fileOffset > fileSize) {

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

                    //visit 4B
                    int visit = mappedFile.getInt(fileOffset + 3);

                    //storeTime 4B
                    int storeTime = mappedFile.getInt(fileOffset + 7);

                    //timeOut 4B
                    int timeOut = mappedFile.getInt(fileOffset + 11);

                    if (timeOut > 0 && System.currentTimeMillis() - storeTime - firstTime > timeOut) {
                        //System.out.println("time out!");
                        read += size;
                        continue;
                    }

                    //keyLen
                    int keyLen = UtilAll.byteToInt(mappedFile.getByte(fileOffset + 15));
                    if (keyLen <= 0 || 18 + keyLen >= size) {
                        logger.debug("keyLen error!,this file destroy,go next file! size: " + keyLen);
                        read = (fileIndex + 1) * (long) fileSize;
                        continue;
                    }
                    //key kenLen
                    byte[] key = mappedFile.getBytes(fileOffset + 16, keyLen);
                    String keyStr = UtilAll.byte2String(key,"UTF-8");

                    //extSize
                    int extSize = UtilAll.byteToInt(mappedFile.getByte(fileOffset + 16 + keyLen));

                    //略过ext的内容
                    //byte[] extBody = mappedFile.getBytes(fileOffset + 17 + keyLen,extSize);

                    //val size - 21 - kenLen - extSize
                    byte[] val = mappedFile.getBytes(fileOffset + 17 + keyLen + extSize, size - 21 - keyLen - extSize);
                    String valStr = UtilAll.byte2String(val,"UTF-8");

                    //crc32Code
                    int crc32Code = mappedFile.getInt(fileOffset + size - 4);

                    ByteBuffer byteBuffer = ByteBuffer.allocate(size - 4);
                    //size 2B   0
                    byteBuffer.putShort(size);
                    //status 1B 2
                    byteBuffer.put((byte) 0);
                    //visit 4B  3
                    byteBuffer.putInt(storeTime);
                    //storeTime 4B 7
                    byteBuffer.putInt(storeTime);
                    //timeOut 4B   11
                    byteBuffer.putInt(timeOut);
                    //keyLen  1B  15
                    byteBuffer.put(UtilAll.intToByte(keyLen));
                    //key
                    byteBuffer.put(key);

                    //ext
                    byteBuffer.put(UtilAll.intToByte(extSize));
                    TreeMap<String,String> ext = new TreeMap<String, String>();
                    if (extSize != 0) {
                        byte[] extBody = mappedFile.getBytes(fileOffset + 17 + keyLen,extSize);
                        byteBuffer.put(extBody);
                        int start = fileOffset + 17 + keyLen;
                        int index = 0;
                        while (index < extSize) {
                            int extKeyLen = UtilAll.byteToInt(mappedFile.getByte(start + index));
                            int extValLen = UtilAll.byteToInt(mappedFile.getByte(start + index + 1 + extKeyLen));
                            try {
                                String extKey = new String(extBody,start + index + 1,extKeyLen,"UTF-8");
                                String extVal = new String(extBody,start + index + extKeyLen + 2,extValLen,"UTF-8");
                                ext.put(extKey,extVal);
                                index += extKeyLen + extValLen + 2;
                            } catch (UnsupportedEncodingException e) {
                                logger.error(e.toString());
                                break;
                            }

                        }

                    }
                    byteBuffer.put(val);

                    int crc32CodeCaculate = UtilAll.crc32(byteBuffer.array());

                    //检验文件是否正常
                    if (crc32Code != crc32CodeCaculate) {
                        logger.debug("{} file destroy!", mappedFile.getFilePath());
                        read = (fileIndex + 1) * (long) fileSize;
                        continue;
                    }

                    totalMessageAdd();
                    if (read == writePosition.get()) {
                        cacheIndex.put(keyStr, read);
                        read += size;
                        writePosition.addAndGet(size);
                        //logger.debug("不需要移动元素");
                        continue;
                    }
                    //

                    CacheStoreMessage cacheStoreMessage = CacheStoreMessage.getInstance(keyStr, valStr, storeTime, timeOut, false);
                    cacheStoreMessage.setVisit(visit);
                    cacheStoreMessage.addExtFields(ext);

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
     * 重新构建CacheFile.将被删除的,过期数据清除.
     *
     * @return bool
     */
    public boolean rebulid() {
        totalMessage.set(0);
        totalDelete.set(0);
        isRebulid = true;
        //1.创建一个新的目录
        storePathNext = getDirName(Long.parseLong(new File(storePathNow).getName()) + 1) + "/";

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

            if (fileOffset + 21 <= fileSize) {
                //该文件后面可能还有消息,尝试读
                MappedFile mappedFile = mappedFileList.get(fileIndex);
                //size 2B
                short size = mappedFile.getShort(fileOffset);
                if (size <= 21 || size + fileOffset > fileSize) {

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

                //visit 4B
                int visit = mappedFile.getInt(fileOffset + 3);

                //storeTime 4B
                int storeTime = mappedFile.getInt(fileOffset + 7);

                //timeOut 4B
                int timeOut = mappedFile.getInt(fileOffset + 11);

                if (timeOut > 0 && System.currentTimeMillis() - storeTime - firstTime > timeOut) {
                    //System.out.println("time out!");
                    read += size;
                    continue;
                }

                //keyLen
                int keyLen = UtilAll.byteToInt(mappedFile.getByte(fileOffset + 15));
                if (keyLen <= 0 || 18 + keyLen >= size) {
                    logger.debug("keyLen error!,this file destroy,go next file! size: " + keyLen);
                    read = (fileIndex + 1) * (long) fileSize;
                    continue;
                }
                //key kenLen
                byte[] key = mappedFile.getBytes(fileOffset + 16, keyLen);
                String keyStr = UtilAll.byte2String(key,"UTF-8");

                //extSize
                int extSize = UtilAll.byteToInt(mappedFile.getByte(fileOffset + 16 + keyLen));

                //略过ext的内容
                //byte[] extBody = mappedFile.getBytes(fileOffset + 17 + keyLen,extSize);

                //val size - 21 - kenLen - extSize
                byte[] val = mappedFile.getBytes(fileOffset + 17 + keyLen + extSize, size - 21 - keyLen - extSize);
                String valStr = UtilAll.byte2String(val,"UTF-8");

                //crc32Code
                int crc32Code = mappedFile.getInt(fileOffset + size - 4);

                ByteBuffer byteBuffer = ByteBuffer.allocate(size - 4);
                //size 2B   0
                byteBuffer.putShort(size);
                /**
                 * status ,and visit 在程序运行时,会变化,所以不检验这个值,把他最初值放进去检验
                 */
                //status 1B 2
                byteBuffer.put((byte) 0);
                //visit 4B  3
                byteBuffer.putInt(storeTime);
                //storeTime 4B 7
                byteBuffer.putInt(storeTime);
                //timeOut 4B   11
                byteBuffer.putInt(timeOut);
                //keyLen  1B  15
                byteBuffer.put(UtilAll.intToByte(keyLen));
                //key
                byteBuffer.put(key);

                //ext
                byteBuffer.put(UtilAll.intToByte(extSize));
                TreeMap<String,String> ext = new TreeMap<String, String>();
                if (extSize != 0) {
                    byte[] extBody = mappedFile.getBytes(fileOffset + 17 + keyLen,extSize);
                    byteBuffer.put(extBody);
                    int start = fileOffset + 17 + keyLen;
                    int index = 0;
                    while (index < extSize) {
                        int extKeyLen = UtilAll.byteToInt(mappedFile.getByte(start + index));
                        int extValLen = UtilAll.byteToInt(mappedFile.getByte(start + index + 1 + extKeyLen));
                        try {
                            String extKey = new String(extBody,start + index + 1,extKeyLen,"UTF-8");
                            String extVal = new String(extBody,start + index + extKeyLen + 2,extValLen,"UTF-8");
                            ext.put(extKey,extVal);
                            index += extKeyLen + extValLen + 2;
                        } catch (UnsupportedEncodingException e) {
                            logger.error(e.toString());
                            break;
                        }

                    }

                }


                byteBuffer.put(val);

                int crc32CodeCaculate = UtilAll.crc32(byteBuffer.array());

                //检验文件是否正常
                if (crc32Code != crc32CodeCaculate) {
                    logger.debug("{} file destroy!", mappedFile.getFilePath());
                    read = (fileIndex + 1) * (long) fileSize;
                    continue;
                }

                //将数据插入到新文件当中
                CacheStoreMessage cacheStoreMessage = CacheStoreMessage.getInstance(keyStr, valStr, storeTime, timeOut, false);
                cacheStoreMessage.setVisit(visit);
                cacheStoreMessage.addExtFields(ext);

                //主线程的写操作也会写这个数据,所以需要加锁
                lock.lock();
                try {
                    Long preOffset = cacheIndexNext.get(keyStr);
                    boolean flag = true;
                    if (preOffset != null) {
                        int preFileIndex = (int)(preOffset / fileSize);
                        int preFileOffset = (int)(preOffset % fileSize);
                        MappedFile mappedFileTemp = mappedFileListNext.get(preFileIndex);
                        byte preStatus = mappedFileTemp.getByte(preFileOffset + 2);

                        int preKeyLen = UtilAll.byteToInt(mappedFileTemp.getByte(preFileOffset + 15));

                        int preExtSize = UtilAll.byteToInt(mappedFileTemp.getByte(preFileOffset + 16 + preKeyLen));

                        byte[] preValByte = mappedFile.getBytes(fileOffset + 17 + keyLen + preExtSize, size - 21 - keyLen - preExtSize);
                        String preValStr = UtilAll.byte2String(preValByte,"UTF-8");

                        /**
                         * 相等说明之前已经插入了
                         * 这里主要防止插入双份
                         */

                        if (preValStr != null && preValStr.equals(valStr)) {
                            if (preStatus == 1) {
                                mappedFileTemp.put(preFileOffset + 2,(byte)0);
                                totalDelete.getAndDecrement();
                            }
                            flag = false;
                        } else {
                            mappedFileTemp.put(preFileOffset + 2,(byte)1);
                            totalDelete.getAndIncrement();
                        }
                    }

                    if (flag) {
                        AppendMessageResult appendMessageResult = appendMessage(mappedFileListNext, writePositionNext, storePathNext, cacheStoreMessage);
                        if (appendMessageResult.getState() == AppendMessageResult.AppendMessageState.APPEND_MESSAGE_OK) {
                            Long offset = cacheIndexNext.get(keyStr);
                            if (offset != null) {
                                deleteMessage(offset,true);
                            }
                            cacheIndexNext.put(keyStr, appendMessageResult.getAppendOffset());
                            writePositionNext.set(appendMessageResult.getAppendOffset());
                            writePositionNext.addAndGet(size);
                            read += size;
                        } else {
                            logger.debug("appendMessage error! {}", appendMessageResult);
                            //读下一个文件
                            read = (fileIndex + 1) * (long) fileSize;
                        }
                        totalMessageAdd();
                    }
                } finally {
                    lock.unlock();
                }
            } else {
                read = (fileIndex + 1) * (long) fileSize;
            }
        }

        return true;
    }

    /**
     * 重建Cache file在这之是没有生效的[这个方法执行前需要关闭主线程的读服务]
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
        cacheIndexNext = new HashMap<String, Long>();

        //5.删除之前的目录文件文件
        File file = new File(storePathNow);
        boolean result = file.delete();
        logger.info(storePathNow + " delete " + (result ? "OK" : "Failed"));
        storePathNow = storePathNext;
        isRebulid = false;
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
     * @param offset offset.
     * @param rebulid 是删除之前的文件还是重建文件
     */
    private void deleteMessage(long offset,boolean rebulid) {
        if (rebulid) {
            deleteRebulidMessage(offset);
        } else {
            deleteNowMessage(offset);
        }
    }

    private void deleteNowMessage(long offset) {
        if (offset > writePosition.get()) {
            return;
        }
        //计算那个文件
        int fileIndex = (int) (offset / fileSize);

        //获取文件的位置
        int fileOffset = (int) (offset % fileSize);
        if (fileIndex + 2 >= fileSize) {
            return;
        }
        MappedFile mappedFile = mappedFileList.get(fileIndex);

        //标记删除
        mappedFile.put(fileOffset + 2, (byte) 1);
        totalDelete.getAndIncrement();
    }

    private void deleteRebulidMessage(long offset) {
        if (offset > writePositionNext.get()) {
            return;
        }
        //计算那个文件
        int fileIndex = (int) (offset / fileSize);

        //获取文件的位置
        int fileOffset = (int) (offset % fileSize);
        if (fileIndex + 2 >= fileSize) {
            return;
        }
        MappedFile mappedFile = mappedFileListNext.get(fileIndex);

        //标记删除
        mappedFile.put(fileOffset + 2, (byte) 1);
        totalDelete.getAndIncrement();
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
                deleteMessage(preVal,false);
            }
            cacheIndex.put(key, res);
            writePosition.set(appendMessageResult.getAppendOffset());
            writePosition.addAndGet(cacheStoreMessage.getSerializedSize());
            totalMessageAdd();
            //如果有线程在后台重建索引,需要将正在写的内容写一份到重建的里面
            if (isRebulid) {
                lock.lock();
                try {
                    AppendMessageResult appendMessageResult2 = appendMessage(mappedFileListNext, writePositionNext, storePathNext, cacheStoreMessage);
                    if (appendMessageResult2.getState() == AppendMessageResult.AppendMessageState.APPEND_MESSAGE_OK) {
                        Long offset = cacheIndexNext.get(key);
                        if (offset != null) {
                            deleteMessage(offset,true);
                        }
                        cacheIndexNext.put(key, appendMessageResult2.getAppendOffset());
                        writePositionNext.set(appendMessageResult2.getAppendOffset());
                        writePositionNext.addAndGet(cacheStoreMessage.getSerializedSize());
                    } else {
                        logger.debug("appendMessage error! {}", appendMessageResult);

                    }
                    totalMessageAdd();
                    logger.debug("put message in rebulid mode");
                } finally {
                    lock.unlock();
                }

            }
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

        MappedFile mappedFile = mappedFileList.get(fileIndex);
        byte status = mappedFile.getByte(fileOffset + 2);
        if (status == (byte) 1) {
            //该数据被删
            return result;
        }
        //storeTime
        int storeTime = mappedFile.getInt(fileOffset + 7);

        //timeOut
        int timeOut = mappedFile.getInt(fileOffset + 11);

        if (timeOut >= 0 && System.currentTimeMillis() - storeTime - firstTime > timeOut) {
            System.out.println("time out!");
            return result;
        }
        //修改visitTime
        mappedFile.putInt(fileOffset + 3,(int)(System.currentTimeMillis() - firstTime));

        short size = mappedFile.getShort(fileOffset);

        int keyLen = UtilAll.getStrLen(key,"UTF-8");
        int extSize = UtilAll.byteToInt(mappedFile.getByte(fileOffset + 16 + keyLen));
        byte[] valByte = mappedFile.getBytes(fileOffset + 17 + keyLen + extSize, size - 21 - keyLen - extSize);

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
        cacheIndex.remove(key);
        if (isRebulid) {
            try {
                lock.lock();
                Long valOffsetNext = cacheIndexNext.get(key);
                if (valOffsetNext == null) {
                    return true;
                }
                //计算那个文件
                int fileIndexNext = (int) (valOffsetNext / fileSize);

                //获取文件的位置
                int fileOffsetNext = (int) (valOffsetNext % fileSize);

                MappedFile mappedFileNext = mappedFileListNext.get(fileIndexNext);
                //标记删除
                mappedFileNext.put(fileOffsetNext + 2, (byte) 1);
                cacheIndexNext.remove(key);
                logger.debug("del message in rebulid mode");
            } finally {
                lock.unlock();
            }
        }
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
        if (canFlush.get()) {
            for (MappedFile mappedFile : mappedFileList) {
                mappedFile.flush();
            }
        }
    }

    public List<MappedFile> getMappedFileList() {
        return mappedFileList;
    }

    public int getFileSize () {
        return fileSize;
    }

    public AtomicBoolean getCanFlush() {
        return canFlush;
    }
}
