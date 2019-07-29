package recycle;

import common.UtilAll;

public class RecycleOptions {
    //连接数小于多少才能进行垃圾回收
    public static final int MIN_CONNECTORS = 100;

    //Cache 数据达到多少才进行垃圾回收[物理内存的一般]
    public static final long MAX_PHY_SIZE = UtilAll.getTotalMemorySize() >> 1;

    //检测是否需要垃圾回收.时间间隔[2分钟]
    public static final long RECYCLE_DELAY = 1000 * 60 * 2;

    // delete / total > DEFAULT_FACTOR 才会触发垃圾回收
    public static final float DEFAULT_FACTOR = 0.25f;
}
