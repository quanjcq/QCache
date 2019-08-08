package store;

public class StoreOptions {
    //os 页大小.linux默认是4KB
    public static final int OS_PAGE_SIZE = 1024 * 4;
    //文件结束符
    public static final short FILE_EOF = Short.MIN_VALUE;
    //异步刷盘时间间隔
    public static final int FLUSH_TIME = 1024 * 60 * 3;
}
