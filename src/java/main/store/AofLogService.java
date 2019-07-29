package store;

import com.sun.istack.internal.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import store.message.AofLogMessage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 给所有写操作,写日志.
 */
public class AofLogService {
    private static Logger logger = LoggerFactory.getLogger(AofLogService.class);
    private MappedFile mappedFile;
    private String filePath;
    private int fileSize;
    private AtomicInteger writePositon = new AtomicInteger(0);


    public AofLogService(String filePath, int fileSize) {
        this.filePath = filePath;
        this.fileSize = fileSize;
        try {
            this.mappedFile = new MappedFile(filePath,fileSize);
        } catch (IOException e) {
            logger.error("create MappedFile error:{}",e);
        }
        init();
    }

    /**
     * 初始化,主要找到上次写到了的位置.
     */
    private void init(){
        ByteBuffer buffer = mappedFile.getByteBuffer();
        buffer.position(0);
        buffer.limit(fileSize);
        AofLogMessage aofLogMessage = AofLogMessage.getInstance();
        while (aofLogMessage.decode(buffer) != null) {
            writePositon.addAndGet(aofLogMessage.getSerializedSize());
        }
    }
    /**
     * 追加消息
     * @param message message
     * @return result
     */
    public AppendMessageResult appendMessage(@NotNull AofLogMessage message) {
        AppendMessageResult result = this.mappedFile.insertMessage(message,writePositon.get(),true);
        if (result.getState() == AppendMessageResult.AppendMessageState.APPEND_MESSAGE_OK) {
            writePositon.getAndAdd(message.getSerializedSize());
        }
        return result;
    }

    /**
     * 指定位置读取一条消息
     * @param byteBuffer byteBuffer
     * @param offset offset
     * @return message,错误返回null
     */
    public AofLogMessage getAofMessage(ByteBuffer byteBuffer,int offset){
        ByteBuffer buffer = byteBuffer.slice();
        buffer.position(offset);

        return AofLogMessage.getInstance().decode(buffer);
    }

    public MappedFile getMappedFile(){
        return mappedFile;
    }


}
