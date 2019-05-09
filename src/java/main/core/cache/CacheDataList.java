package core.cache;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

public class CacheDataList implements CacheDataI, Serializable {
    //最后一次访问的时间
    public long lastVisit;
    //缓存可以存放的时间
    public long last;
    public LinkedList<CacheDataI> val = new LinkedList<CacheDataI>();


}
