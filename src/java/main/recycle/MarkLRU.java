package recycle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import store.CacheFileGroup;
import store.MappedFile;
import store.StoreOptions;

import java.util.*;

/**
 * 之前没想到,没有提供访问的记录,目前没能实现
 */
public class MarkLRU implements Mark {
    private static Logger logger = LoggerFactory.getLogger(MarkLRU.class);
    /**
     * Cache存储的文件
     */
    private CacheFileGroup cacheFileGroup;


    public MarkLRU(CacheFileGroup cacheFileGroup){
        this.cacheFileGroup = cacheFileGroup;
    }
    @Override
    public int mark() {
        int result = 0;
        long scanPosition = StoreOptions.OS_PAGE_SIZE;
        long maxPosition  = cacheFileGroup.getWriteSize();
        List<MappedFile> mappedFileList = cacheFileGroup.getMappedFileList();
        int fileSize = cacheFileGroup.getFileSize();

        int fileIndex;
        int fileOffset;
        Set<LRUNode> lruNodeTreeSet = new TreeSet<LRUNode>();
        while (scanPosition < maxPosition) {
            //计算那个文件
            fileIndex = (int) (scanPosition / fileSize);
            //计算文件的位置
            fileOffset = (int) (scanPosition % fileSize);
            if (fileOffset + 21 <= fileSize) {
                MappedFile mappedFile = mappedFileList.get(fileIndex);
                //size 2B
                short size = mappedFile.getShort(fileOffset);
                if (size <= 21 || size + fileOffset > fileSize) {
                    scanPosition = (fileIndex + 1) * (long) fileSize;
                    continue;
                }
                int visit = mappedFile.getInt(fileOffset + 3);

                lruNodeTreeSet.add(new LRUNode(visit,scanPosition));
                scanPosition += size;

            } else {
                scanPosition = (fileIndex + 1) * (long) fileSize;

            }

        }
        TreeSet<Long> messageOffset = new TreeSet<Long>();
        //在原有的基础上清除一半
        int markSize = lruNodeTreeSet.size() >> 1;
        logger.debug("{},{}",markSize,lruNodeTreeSet.size());
        Iterator<LRUNode> iterator = lruNodeTreeSet.iterator();
        //按照offset 排序,顺序访问性能更好
        while (markSize-- > 0 && iterator.hasNext()) {
            messageOffset.add(iterator.next().offset);
        }

        lruNodeTreeSet = null;

        //开始遍历标记删除
        for (long position : messageOffset) {
            //计算那个文件
            fileIndex = (int) (position / (long) fileSize);
            //计算文件的位置
            fileOffset = (int) (position % fileSize);

            MappedFile mappedFile = mappedFileList.get(fileIndex);

            mappedFile.put(fileOffset + 2,(byte) 1);
            result++;
        }
        return result;
    }

    private class LRUNode implements Comparable {
        public int visit;
        public long offset;
        public LRUNode(int visit,long offset) {
            this.visit = visit;
            this.offset = offset;
        }

        @Override
        public int hashCode() {
            return (int)offset;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (obj == this) {
                return true;
            }
            if (obj instanceof LRUNode) {
                return this.offset == ((LRUNode)obj).offset;
            }
            return false;
        }

        @Override
        public int compareTo(Object o) {
            if (this.visit - ((LRUNode)o).visit != 0) {
                return  this.visit - ((LRUNode)o).visit;
            } else {
                return (int)(this.offset - ((LRUNode)o).offset);
            }
        }

        @Override
        public String toString() {
            return "LRUNode{" +
                    "visit=" + visit +
                    ", offset=" + offset +
                    '}';
        }
    }
}
