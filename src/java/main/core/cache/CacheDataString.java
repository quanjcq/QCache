package core.cache;

import java.io.Serializable;

public class CacheDataString implements CacheDataI, Serializable {
    //数据
    public String val;
    //最后一次访问的时间
    public long lastVisit;
    //缓存可以存放的时间,-1 代表不过期的
    public long last; //ms

    public CacheDataString(String val, long lastVisit, long last) {
        this.val = val;
        this.lastVisit = lastVisit;
        this.last = last;
    }
}
