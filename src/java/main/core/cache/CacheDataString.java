package core.cache;

import java.io.Serializable;

public class CacheDataString extends CacheData implements Serializable {
    //数据
    public String val;

    public CacheDataString(String val, long lastVisit, long last) {
        this.val = val;
        this.lastVisit = lastVisit;
        this.last = last;
    }
}
