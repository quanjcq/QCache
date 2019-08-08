package recycle;

import store.CacheFileGroup;
import store.MappedFile;
import store.StoreOptions;

import java.util.List;

/**
 * 标记最早放入内存的数据.
 * 数据全是append方式放入的,所以最早放入的数据,一定都是在前面,把前面一半删除.
 */
public class MarkFistIn implements Mark {
    /**
     * Cache存储的文件
     */
    private CacheFileGroup cacheFileGroup;
    public MarkFistIn(CacheFileGroup cacheFileGroup){
        this.cacheFileGroup = cacheFileGroup;
    }
    @Override
    public int mark() {
        int result = 0;
        long scanPosition = StoreOptions.OS_PAGE_SIZE;
        //只标记删除前面的一般
        long maxPosition  = cacheFileGroup.getWriteSize() >> 1;
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
                //标记删除
                mappedFile.put(fileOffset + 2,(byte) 1);
                scanPosition += size;
                result++;
            } else {
                scanPosition = (fileIndex + 1) * (long) fileSize;
            }
        }
        return result;
    }
}
