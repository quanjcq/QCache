package store.checkpoint;

import common.UtilAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import store.MappedFile;
import store.StoreOptions;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Term: 8B
 * lastAppliedIndex: 8B
 * flushTime: 8B
 *
 * crc32校验码: 4B
 * 共28字节
 *
 */
public class CheckPoint {
    private static Logger log = LoggerFactory.getLogger(CheckPoint.class);
    private MappedFile mappedFile;
    private final int CHECK_POINT_SIZE = 28;
    private final ByteBuffer buffer = ByteBuffer.allocate(CHECK_POINT_SIZE - 4);
    public CheckPoint(String filePath) {
        try {
            this.mappedFile = new MappedFile(filePath, StoreOptions.OS_PAGE_SIZE);
            mappedFile.setWritePosition(CHECK_POINT_SIZE);
        } catch (IOException e) {
            log.error("create checkPoint mappedFile error: {}",e);
            System.exit(1);
        }
    }

    /**
     * get term.
     * @return term,文件被破坏返回 -1
     */
    public long getTerm(){
        return getVal(0);

    }



    /**
     * get lastAppliedIndex.
     * @return index,文件被破坏返回 -1;
     */
    public long getLastAppliedIndex(){
        return getVal(8);
    }

    /**
     * get flush time.
     * @return flush,文件被破坏返回 -1
     */
    public long getFlush(){
        return getVal(16);

    }
    /**
     * 获取指定位置一个long 值
     * @param offset 偏移量
     * @return val,文件被破坏返回-1.
     */
    private long getVal(int offset){
        ByteBuffer byteBuffer = mappedFile.getByteBuffer();
        buffer.clear();
        //body
        byteBuffer.get(buffer.array(),0,buffer.capacity());
        int crc32CodeCaculate = UtilAll.crc32(buffer.array());
        int crc32Code = mappedFile.getMappedByteBuffer().getInt(24);
        if (crc32Code != crc32CodeCaculate) {
            log.error("CheckPoint file was destroy!");
            return -1;
        }
        return mappedFile.getMappedByteBuffer().getLong(offset);
    }

    /**
     * set term
     * @param term term.
     */
    public void setTerm(long term) {
        setVal(0,term);
    }

    /**
     * set lastAppliedIndex.
     * @param lastAppliedIndex lastAppliedIndex.
     */
    public void setLastAppliedIndex(long lastAppliedIndex) {
        setVal(8,lastAppliedIndex);
    }

    /**
     * set flush.
     * @param flush flush.
     */
    public void setFlush(long flush) {
        setVal(16,flush);
    }

    /**
     * 在指定位置设置一个long值
     * @param offset offset
     * @param val val.
     */
    private void setVal(int offset,long val){
        ByteBuffer byteBuffer = mappedFile.getByteBuffer();
        mappedFile.getMappedByteBuffer().putLong(offset,val);
        buffer.clear();

        //body
        byteBuffer.get(buffer.array(),0,buffer.capacity());

        int crc32CodeCaculate = UtilAll.crc32(buffer.array());

        //修改后面的检验码
        mappedFile.getMappedByteBuffer().putInt(24,crc32CodeCaculate);
        mappedFile.flush();
    }

}
