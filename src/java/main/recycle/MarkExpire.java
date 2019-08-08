package recycle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import store.CacheFileGroup;
import store.MappedFile;
import store.StoreOptions;
import java.util.List;

/**
 * 标记所有超时的数据.
 */
public class MarkExpire implements Mark{
    private static Logger logger = LoggerFactory.getLogger(MarkExpire.class);
    /**
     * Cache存储的文件
     */
    private CacheFileGroup cacheFileGroup;
    public MarkExpire(CacheFileGroup cacheFileGroup){
        this.cacheFileGroup = cacheFileGroup;
    }
    @Override
    public int mark() {
        int result = 0;
        long scanPosition = StoreOptions.OS_PAGE_SIZE;
        long maxPosition  = cacheFileGroup.getWriteSize();
        long baseTime = cacheFileGroup.getFirstTime();
        List<MappedFile> mappedFileList = cacheFileGroup.getMappedFileList();
        int fileSize = cacheFileGroup.getFileSize();
        //遍历所有缓存
        int fileIndex;
        int fileOffset;
        while (scanPosition < maxPosition) {
            //计算那个文件
            fileIndex = (int) (scanPosition / (long) fileSize);
            //计算文件的位置
            fileOffset = (int) (scanPosition % fileSize);
            if (fileOffset + 21 <= fileSize) {
                //该文件后面可能还有消息,尝试读
                MappedFile mappedFile = mappedFileList.get(fileIndex);
                //size 2B
                short size = mappedFile.getShort(fileOffset);
                if (size <= 21 || size + fileOffset > fileSize) {
                    scanPosition = (fileIndex + 1) * (long) fileSize;
                    continue;
                }
                //status 1B
                byte status = mappedFile.getByte(fileOffset + 2);

                //该记录已被删除
                if (status == (byte) 1) {
                    scanPosition += size;
                    continue;
                }
                //timeOut 4B
                int timeOut = mappedFile.getInt(fileOffset + 11);
                if (timeOut == -1) {
                    //这个消息不会过期
                    scanPosition += size;
                    continue;
                }
                //storeTime 4B
                int storeTime = mappedFile.getInt(fileOffset + 7);
                if (timeOut > 0 && System.currentTimeMillis() - storeTime - baseTime > timeOut) {
                    //logger.info("time out!");
                    //标记删除
                    mappedFile.put(fileOffset + 2,(byte) 1);
                    result++;
                }
                scanPosition += size;
            } else {
                scanPosition = (fileIndex + 1) * (long) fileSize;
            }
        }
        return result;
    }


}
