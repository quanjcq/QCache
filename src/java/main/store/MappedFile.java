package store;

import com.sun.istack.internal.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import store.message.Message;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 基于内存映射文件存储.
 */
public class MappedFile {
    private static Logger logger = LoggerFactory.getLogger(MappedFile.class);

    private final AtomicInteger writePosition = new AtomicInteger(0);

    private int fileSize;

    private FileChannel fileChannel;

    private String fileName;

    //该文件是从那个偏移量开始的
    private long fileFromOffset;

    private File file;
    //内存映射文件
    private MappedByteBuffer mappedByteBuffer;

    public MappedFile(final String fileName, final int fileSize) throws IOException {
        init(fileName, fileSize);
    }

    /**
     * 目录是否存在,不存在则创建
     *
     * @param dirName dirName
     */
    public static void ensureDirOK(final String dirName) {
        if (dirName != null) {
            File f = new File(dirName);
            if (!f.exists()) {
                boolean result = f.mkdirs();
                logger.info(dirName + " mkdir " + (result ? "OK" : "Failed"));
            }
        }
    }

    /**
     * 初始化基本信息,fileChannel,MappedByteBuffer
     *
     * @param fileName filename
     * @param fileSize fileSize
     * @throws IOException ex
     */
    private void init(final String fileName, final int fileSize) throws IOException {
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.file = new File(fileName);
        this.fileFromOffset = Long.parseLong(this.file.getName());
        boolean ok = false;
        //确保该文件的上级目录存在
        ensureDirOK(this.file.getParent());
        try {
            this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
            this.mappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
            ok = true;
        } catch (FileNotFoundException e) {
            logger.error("create file channel " + this.fileName + " Failed. ", e);
            throw e;
        } catch (IOException e) {
            logger.error("map file " + this.fileName + " Failed. ", e);
            throw e;
        } finally {
            if (!ok && this.fileChannel != null) {
                this.fileChannel.close();
            }
        }
    }

    /**
     * 向磁盘中追加日志.
     *
     * @param message message
     * @param flush   是否刷盘
     * @return AppendMessageResult
     */
    public AppendMessageResult appendMessage(@NotNull Message message, boolean flush) {
        int size = message.getSerializedSize();
        ByteBuffer messageBuf = ByteBuffer.allocate(size);
        messageBuf.put(message.encode());
        return appendMessage(messageBuf.array(), flush);
    }

    public AppendMessageResult appendMessage(@NotNull byte[] message, boolean flush) {
        return appendMessage(message, 0, message.length, flush);
    }

    public AppendMessageResult appendMessage(@NotNull byte[] message, int start, int length, boolean flush) {
        if (message.length == 0 || start + length >= message.length) {
            return new AppendMessageResult(AppendMessageResult.AppendMessageState.APPEND_MESSAGE_MESSAGE_EMPTY);
        }
        ByteBuffer buffer = mappedByteBuffer.slice();
        int position = writePosition.get();
        if (position + length > fileSize) {
            //没有容量写了
            return new AppendMessageResult(AppendMessageResult.AppendMessageState.APPEND_MESSAGE_NO_SPACE);
        }
        AppendMessageResult result = new AppendMessageResult();
        buffer.position(writePosition.get());
        buffer.put(message, start, length);
        writePosition.addAndGet(length);
        result.setState(AppendMessageResult.AppendMessageState.APPEND_MESSAGE_OK);
        result.setAppendOffset(fileFromOffset + position);
        //刷盘
        if (flush) {
            this.mappedByteBuffer.force();
        }
        return result;

    }

    /**
     * 指定位置插入数据.
     *
     * @param message message
     * @param offset  偏移量
     * @param flush   是否刷盘
     * @return AppendMessageResult.
     */
    public AppendMessageResult insertMessage(@NotNull Message message, int offset, boolean flush) {
        if (offset >= fileSize) {
            return new AppendMessageResult(AppendMessageResult.AppendMessageState.APPEND_MESSAGE_OUT_OF_RANGE);
        }
        int size = message.getSerializedSize();
        byte[] data = message.encode();
        return insertMessage(data, offset,0,size,flush);
    }

    /**
     * 指定位置插入数据
     *
     * @param message msg
     * @param offset  offset
     * @param flush   flush
     * @return AppendMessageResult
     */
    public AppendMessageResult insertMessage(@NotNull byte[] message, int offset, boolean flush) {
        if (message.length == 0) {
            return new AppendMessageResult(AppendMessageResult.AppendMessageState.APPEND_MESSAGE_MESSAGE_EMPTY);
        }
        if (offset >= fileSize) {
            return new AppendMessageResult(AppendMessageResult.AppendMessageState.APPEND_MESSAGE_OUT_OF_RANGE);
        }
        return insertMessage(message, offset, 0, message.length, flush);
    }

    /**
     * 指定位置插入数据.
     *
     * @param message message
     * @param offset  offset
     * @param start   start
     * @param length  length
     * @param flush   flush
     * @return AppendMessageResult
     */
    public AppendMessageResult insertMessage(@NotNull byte[] message, int offset, int start, int length, boolean flush) {
        if (message.length == 0 || start >= message.length) {
            return new AppendMessageResult(AppendMessageResult.AppendMessageState.APPEND_MESSAGE_MESSAGE_EMPTY);
        }
        if (offset >= fileSize) {
            return new AppendMessageResult(AppendMessageResult.AppendMessageState.APPEND_MESSAGE_OUT_OF_RANGE);
        }


        if (offset + length >= fileSize) {
            //没有容量写了
            return new AppendMessageResult(AppendMessageResult.AppendMessageState.APPEND_MESSAGE_NO_SPACE);
        }
        AppendMessageResult result = new AppendMessageResult();

        putBytes(offset, message, start, length);
        /*if (offset  + 2 < fileSize ) {
            putShort(offset + length, StoreOptions.FILE_EOF);
        }*/


        result.setState(AppendMessageResult.AppendMessageState.APPEND_MESSAGE_OK);
        result.setAppendOffset(fileFromOffset + offset);
        writePosition.addAndGet(length);
        //刷盘
        if (flush) {
            this.mappedByteBuffer.force();
        }
        return result;
    }

    /**
     * 清除文件里面的内容
     */
    public void clean() {
        //position 置0
        writePosition.set(0);
        ByteBuffer byteBuffer = mappedByteBuffer.slice();
        byteBuffer.position(0);
        //同时在文件开始写上文件结束符
        byteBuffer.putShort(StoreOptions.FILE_EOF);
        mappedByteBuffer.force();
    }

    /**
     * close file
     */
    public void close() {
        ByteBuffer byteBuffer = mappedByteBuffer.slice();
        byteBuffer.position(writePosition.get());
        byteBuffer.putShort(StoreOptions.FILE_EOF);
        mappedByteBuffer.force();
        try {
            fileChannel.close();
        } catch (IOException e) {
            logger.error("close fileChannel error: {}", e);
        }
    }

    /**
     * 删除文件.
     *
     * @return bool
     */
    public boolean destroy() {
        try {
            this.fileChannel.close();
            logger.info("close file channel " + this.fileName + " OK");
            return this.file.delete();
        } catch (Exception e) {
            logger.warn("close file channel " + this.fileName + " Failed. ", e);
        }
        return true;
    }

    /**
     * 获取文件最后被修改的时间.
     *
     * @return long
     */
    public long getLastModifiedTimestamp() {
        return this.file.lastModified();
    }

    /**
     * 文件大小.
     *
     * @return int
     */
    public int getFileSize() {
        return fileSize;
    }

    /**
     * 获取fileChannel.
     *
     * @return fileChannel
     */
    public FileChannel getFileChannel() {
        return fileChannel;
    }

    public MappedByteBuffer getMappedByteBuffer() {
        return mappedByteBuffer;
    }


    public ByteBuffer getByteBuffer() {
        ByteBuffer byteBuffer = mappedByteBuffer.slice();
        byteBuffer.position(0);
        byteBuffer.limit(writePosition.get());
        return byteBuffer;
    }

    /**
     * 指定位置put一个byte.
     *
     * @param offset offset
     * @param a      byte
     */
    public void put(int offset, byte a) {
        mappedByteBuffer.put(offset, a);
    }

    /**
     * 指定位置put一个short.
     *
     * @param offset offset
     * @param num    num
     */
    public void putShort(int offset, short num) {
        mappedByteBuffer.putShort(offset, num);
    }

    /**
     * 指定位置put一个int.
     *
     * @param offset offset
     * @param num    num.
     */
    public void putInt(int offset, int num) {
        mappedByteBuffer.putInt(offset, num);
    }

    /**
     * 指定位置put一个long.
     *
     * @param offset offset
     * @param num    num.
     */
    public void putLong(int offset, long num) {
        mappedByteBuffer.putLong(offset, num);
    }

    /**
     * 指定位置put bytes 数组.
     *
     * @param offset  offset.
     * @param message message.
     * @param start   message 起始位置
     * @param length  length. 长度
     */
    public void putBytes(int offset, byte[] message, int start, int length) {
        for (int i = 0; i < length; i++) {
            put(offset + i, message[start + i]);
        }
    }

    /**
     * 指定位置读取一个byte
     *
     * @param offset offset
     * @return byte
     */
    public byte getByte(int offset) {
        return mappedByteBuffer.get(offset);
    }

    /**
     * 指定位置读取一个short.
     *
     * @param offset offset
     * @return short
     */
    public short getShort(int offset) {
        return mappedByteBuffer.getShort(offset);

    }

    /**
     * 指定位置读取一个int.
     *
     * @param offset offset
     * @return int
     */
    public int getInt(int offset) {
        return mappedByteBuffer.getInt(offset);

    }

    /**
     * 指定位置读取一个long.
     *
     * @param offset offset
     * @return long
     */
    public long getLong(int offset) {
        return mappedByteBuffer.getLong(offset);

    }

    /**
     * 在指定位置读取连续的字节.
     *
     * @param offset offset
     * @param length 需要读取的长度
     * @return byte[]
     */

    public byte[] getBytes(int offset, int length) {
        byte[] result = new byte[length];
        for (int i = 0; i < length; i++) {
            result[i] = getByte(offset + i);
        }
        return result;
    }

    /**
     * 获取写到的位置
     *
     * @return int.
     */
    public int getWritePosition() {
        return writePosition.get();
    }

    /**
     * 设置writePosition
     *
     * @param position position.
     */
    public void setWritePosition(int position) {
        writePosition.set(position);
    }

    /**
     * 刷盘.
     */
    public void flush() {
        this.mappedByteBuffer.force();
    }

    public String getFilePath() {
        return fileName;
    }
}
