package core.cache;

import java.io.Serializable;

public class CacheDataInt implements CacheDataI, Serializable {
    //数据
    public int val;
    //最后一次访问的时间
    public long lastVisit;
    //缓存可以存放的时间
    public long last; //ms

    public CacheDataInt(int val, long lastVisit, long last) {
        this.val = val;
        this.lastVisit = lastVisit;
        this.last = last;
    }
}
