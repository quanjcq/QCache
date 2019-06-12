package core.cache;

import java.io.Serializable;

public class CacheDataInt extends CacheData implements Serializable {
    //数据
    public int val;

    public CacheDataInt(int val, long lastVisit, long last) {
        this.val = val;
        this.lastVisit = lastVisit;
        this.last = last;
    }
}
