package core.cache;

import java.util.Date;

/**
 * 存放数据的实体
 */
public abstract class CacheData {
    //最后一次访问的时间
    public long lastVisit;
    //缓存可以存放的时间
    public long last; //ms

    //是否超时
    public boolean isOutDate() {
        if (last < 0) {
            return false;
        }
        return lastVisit + last < new Date().getTime();
    }
}
